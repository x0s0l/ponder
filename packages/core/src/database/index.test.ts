import { setupCommon, setupDatabaseConfig } from "@/_test/setup.js";
import { buildSchema } from "@/build/schema.js";
import { onchainEnum, onchainTable, primaryKey } from "@/drizzle/onchain.js";
import { createShutdown } from "@/internal/shutdown.js";
import {
  type Checkpoint,
  ZERO_CHECKPOINT,
  encodeCheckpoint,
} from "@/utils/checkpoint.js";
import { and, eq, sql } from "drizzle-orm";
import { beforeEach, expect, test } from "vitest";
import { type Database, createDatabase, getPonderMeta } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupDatabaseConfig);

const account = onchainTable("account", (p) => ({
  address: p.hex().primaryKey(),
  balance: p.bigint(),
}));

function createCheckpoint(checkpoint: Partial<Checkpoint>): string {
  return encodeCheckpoint({ ...ZERO_CHECKPOINT, ...checkpoint });
}

test("migrate() succeeds with empty schema", async (context) => {
  const database = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });

  await database.migrate({ buildId: "abc" });

  const tableNames = await getUserTableNames(database, "public");
  expect(tableNames).toContain("account");
  expect(tableNames).toContain("_reorg__account");
  expect(tableNames).toContain("_ponder_meta");

  const metadata = await database.qb.drizzle.select().from(sql`_ponder_meta`);

  expect(metadata).toHaveLength(1);

  await context.common.shutdown.kill();
});

test("migrate() with empty schema creates tables and enums", async (context) => {
  const mood = onchainEnum("mood", ["sad", "happy"]);

  const kyle = onchainTable("kyle", (p) => ({
    age: p.integer().primaryKey(),
    mood: mood().notNull(),
  }));

  const user = onchainTable(
    "table",
    (p) => ({
      name: p.text(),
      age: p.integer(),
      address: p.hex(),
    }),
    (table) => ({
      primaryKeys: primaryKey({ columns: [table.name, table.address] }),
    }),
  );

  const database = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account, kyle, mood, user },
      statements: buildSchema({ schema: { account, kyle, mood, user } })
        .statements,
    },
  });

  await database.migrate({ buildId: "abc" });

  const tableNames = await getUserTableNames(database, "public");
  expect(tableNames).toContain("account");
  expect(tableNames).toContain("_reorg__account");
  expect(tableNames).toContain("kyle");
  expect(tableNames).toContain("_reorg__kyle");
  expect(tableNames).toContain("kyle");
  expect(tableNames).toContain("_reorg__kyle");
  expect(tableNames).toContain("_ponder_meta");

  await context.common.shutdown.kill();
});

test("migrate() throws with schema used", async (context) => {
  const database = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });
  await database.migrate({ buildId: "abc" });
  await context.common.shutdown.kill();

  context.common.shutdown = createShutdown();

  const databaseTwo = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });

  const error = await databaseTwo
    .migrate({ buildId: "def" })
    .catch((err) => err);

  expect(error).toBeDefined();

  await context.common.shutdown.kill();
});

// PGlite not being able to concurrently connect to the same database from two different clients
// makes this test impossible.
test("migrate() throws with schema used after waiting for lock", async (context) => {
  if (context.databaseConfig.kind !== "postgres") return;

  context.common.options.databaseHeartbeatInterval = 250;
  context.common.options.databaseHeartbeatTimeout = 1000;

  const database = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });
  await database.migrate({ buildId: "abc" });

  await database.finalize({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
    db: database.qb.drizzle,
  });

  const databaseTwo = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });

  const error = await databaseTwo
    .migrate({ buildId: "abc" })
    .catch((err) => err);

  expect(error).toBeDefined();

  await context.common.shutdown.kill();
});

test("migrate() succeeds with crash recovery", async (context) => {
  const database = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });

  await database.migrate({ buildId: "abc" });

  await database.finalize({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
    db: database.qb.drizzle,
  });

  await context.common.shutdown.kill();

  context.common.shutdown = createShutdown();

  const databaseTwo = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });

  await databaseTwo.migrate({ buildId: "abc" });

  const metadata = await databaseTwo.qb.drizzle
    .select()
    .from(getPonderMeta("public"));

  expect(metadata).toHaveLength(1);

  const tableNames = await getUserTableNames(databaseTwo, "public");
  expect(tableNames).toContain("account");
  expect(tableNames).toContain("_reorg__account");
  expect(tableNames).toContain("_ponder_meta");

  await context.common.shutdown.kill();
});

test("migrate() succeeds with crash recovery after waiting for lock", async (context) => {
  context.common.options.databaseHeartbeatInterval = 750;
  context.common.options.databaseHeartbeatTimeout = 500;

  const database = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });
  await database.migrate({ buildId: "abc" });

  await database.finalize({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
    db: database.qb.drizzle,
  });

  const databaseTwo = await createDatabase({
    common: context.common,
    namespace: "public",
    preBuild: {
      databaseConfig: context.databaseConfig,
    },
    schemaBuild: {
      schema: { account },
      statements: buildSchema({ schema: { account } }).statements,
    },
  });

  await databaseTwo.migrate({ buildId: "abc" });

  await context.common.shutdown.kill();
});

test("heartbeat updates the heartbeat_at value", async (context) => {
  context.common.options.databaseHeartbeatInterval = 250;
  context.common.options.databaseHeartbeatTimeout = 625;

  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  const row = await database.qb.drizzle
    .select()
    .from(getPonderMeta("public"))
    .then((result) => result[0]!.value);

  await wait(500);

  const rowAfterHeartbeat = await database.qb.drizzle
    .select()
    .from(getPonderMeta("public"))
    .then((result) => result[0]!.value);

  expect(BigInt(rowAfterHeartbeat!.heartbeat_at)).toBeGreaterThan(
    row!.heartbeat_at,
  );
});

async function getUserTableNames(database: Database, namespace: string) {
  const rows = await database.qb.drizzle
    .select({ name: sql<string>`table_name`.as("name") })
    .from(sql`information_schema.tables`)
    .where(
      and(eq(sql`table_schema`, namespace), eq(sql`table_type`, "BASE TABLE")),
    );

  return rows.map(({ name }) => name);
}
