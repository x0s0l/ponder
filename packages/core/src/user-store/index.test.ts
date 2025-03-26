import { setupCleanup } from "@/_test/setup.js";
import {
  setupCommon,
  setupDatabaseConfig,
  setupPonder,
} from "@/_test/setup.js";
import { buildSchema } from "@/build/schema.js";
import { type Database, getPonderMeta } from "@/database/index.js";
import { getReorgTable } from "@/drizzle/kit/index.js";
import { onchainTable, primaryKey } from "@/drizzle/onchain.js";
import { createRealtimeIndexingStore } from "@/indexing-store/realtime.js";
import { createShutdown } from "@/internal/shutdown.js";
import {
  type Checkpoint,
  MAX_CHECKPOINT_STRING,
  ZERO_CHECKPOINT,
  encodeCheckpoint,
} from "@/utils/checkpoint.js";
import { and, eq, sql } from "drizzle-orm";
import { index } from "drizzle-orm/pg-core";
import { zeroAddress } from "viem";
import { beforeEach, expect, test } from "vitest";
import { createUserStore, revert } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupDatabaseConfig);
beforeEach(setupCleanup);

const account = onchainTable("account", (p) => ({
  address: p.hex().primaryKey(),
  balance: p.bigint(),
}));

function createCheckpoint(checkpoint: Partial<Checkpoint>): string {
  return encodeCheckpoint({ ...ZERO_CHECKPOINT, ...checkpoint });
}

test("recoverCheckpoint() with crash recovery reverts rows", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  // setup tables, reorg tables, and metadata checkpoint

  await userStore.createTriggers();

  const indexingStore = createRealtimeIndexingStore(app);

  await indexingStore
    .insert(account)
    .values({ address: zeroAddress, balance: 10n });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 9n }),
  });

  await indexingStore
    .insert(account)
    .values({ address: "0x0000000000000000000000000000000000000001" });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 11n }),
  });

  await userStore.finalize({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
  });

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
  const checkpoint = await databaseTwo.recoverCheckpoint();

  expect(checkpoint).toStrictEqual(
    createCheckpoint({ chainId: 1n, blockNumber: 10n }),
  );

  const rows = await databaseTwo.qb.drizzle
    .execute(sql`SELECT * from "account"`)
    .then((result) => result.rows);

  expect(rows).toHaveLength(1);
  expect(rows[0]!.address).toBe(zeroAddress);

  const metadata = await databaseTwo.qb.drizzle
    .select()
    .from(getPonderMeta("public"));

  expect(metadata).toHaveLength(1);
});

test("recoverCheckpoint() with crash recovery drops indexes and triggers", async (context) => {
  const account = onchainTable(
    "account",
    (p) => ({
      address: p.hex().primaryKey(),
      balance: p.bigint(),
    }),
    (table) => ({
      balanceIdx: index().on(table.balance),
    }),
  );

  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  await userStore.finalize({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
  });

  await userStore.createIndexes();

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
  await databaseTwo.recoverCheckpoint();

  const indexNames = await getUserIndexNames(databaseTwo, "public", "account");

  expect(indexNames).toHaveLength(1);
});

test("finalize()", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  // setup tables, reorg tables, and metadata checkpoint

  await userStore.createTriggers();

  const indexingStore = createRealtimeIndexingStore(app);

  await indexingStore
    .insert(account)
    .values({ address: zeroAddress, balance: 10n });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 9n }),
  });

  await indexingStore
    .update(account, { address: zeroAddress })
    .set({ balance: 88n });

  await indexingStore
    .insert(account)
    .values({ address: "0x0000000000000000000000000000000000000001" });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 11n }),
  });

  await userStore.finalize({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
  });

  // reorg tables

  const rows = await app.database.qb.drizzle
    .select()
    .from(getReorgTable(account));

  expect(rows).toHaveLength(2);

  // metadata

  const metadata = await app.database.qb.drizzle
    .select()
    .from(getPonderMeta("public"))
    .then((result) => result[0]!.value);

  expect(metadata.checkpoint).toStrictEqual(
    createCheckpoint({ chainId: 1n, blockNumber: 10n }),
  );
});

test("createIndexes()", async (context) => {
  const account = onchainTable(
    "account",
    (p) => ({
      address: p.hex().primaryKey(),
      balance: p.bigint(),
    }),
    (table) => ({
      balanceIdx: index("balance_index").on(table.balance),
    }),
  );

  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  await userStore.createIndexes();

  const indexNames = await getUserIndexNames(app.database, "public", "account");
  expect(indexNames).toContain("balance_index");
});

test("createTriggers()", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  await userStore.createTriggers();

  const indexingStore = createRealtimeIndexingStore(app);

  await indexingStore
    .insert(account)
    .values({ address: zeroAddress, balance: 10n });

  const { rows } = await app.database.qb.drizzle.execute(
    sql`SELECT * FROM _reorg__account`,
  );

  expect(rows).toStrictEqual([
    {
      address: zeroAddress,
      balance: "10",
      operation: 0,
      operation_id: 1,
      checkpoint: MAX_CHECKPOINT_STRING,
    },
  ]);
});

test("createTriggers() duplicate", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  await userStore.createTriggers();
  await userStore.createTriggers();
});

test("complete()", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  await userStore.createTriggers();

  const indexingStore = createRealtimeIndexingStore(app);

  await indexingStore
    .insert(account)
    .values({ address: zeroAddress, balance: 10n });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
  });

  const { rows } = await app.database.qb.drizzle.execute(
    sql`SELECT * FROM _reorg__account`,
  );

  expect(rows).toStrictEqual([
    {
      address: zeroAddress,
      balance: "10",
      operation: 0,
      operation_id: 1,
      checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
    },
  ]);
});

test("revert()", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  // setup tables, reorg tables, and metadata checkpoint

  await userStore.createTriggers();

  const indexingStore = createRealtimeIndexingStore(app);

  await indexingStore
    .insert(account)
    .values({ address: zeroAddress, balance: 10n });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 9n }),
  });

  await indexingStore
    .update(account, { address: zeroAddress })
    .set({ balance: 88n });

  await indexingStore
    .insert(account)
    .values({ address: "0x0000000000000000000000000000000000000001" });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 10n }),
  });

  await indexingStore.delete(account, { address: zeroAddress });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 11n }),
  });

  await app.database.qb.drizzle.transaction(async (tx) => {
    await revert(app, {
      checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 9n }),
      tx,
    });
  });

  const rows = await app.database.qb.drizzle.select().from(account);

  expect(rows).toHaveLength(1);
  expect(rows[0]).toStrictEqual({ address: zeroAddress, balance: 10n });
});

test("revert() with composite primary key", async (context) => {
  const test = onchainTable(
    "Test",
    (p) => ({
      a: p.integer("A").notNull(),
      b: p.integer("B").notNull(),
      c: p.integer("C"),
    }),
    (table) => ({
      pk: primaryKey({ columns: [table.a, table.b] }),
    }),
  );

  const app = await setupPonder(context, { schema: { test } });
  const userStore = createUserStore(app);

  // setup tables, reorg tables, and metadata checkpoint

  await userStore.createTriggers();

  const indexingStore = createRealtimeIndexingStore(app);

  await indexingStore.insert(test).values({ a: 1, b: 1 });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 11n }),
  });

  await indexingStore.update(test, { a: 1, b: 1 }).set({ c: 1 });

  await userStore.complete({
    checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 12n }),
  });

  await app.database.qb.drizzle.transaction(async (tx) => {
    await revert(app, {
      checkpoint: createCheckpoint({ chainId: 1n, blockNumber: 11n }),
      tx,
    });
  });

  const rows = await app.database.qb.drizzle.select().from(test);

  expect(rows).toHaveLength(1);
  expect(rows[0]).toStrictEqual({ a: 1, b: 1, c: null });
});

test("getStatus() empty", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  const status = await userStore.getStatus();

  expect(status).toBe(null);
});

test("setStatus()", async (context) => {
  const app = await setupPonder(context, { schema: { account } });
  const userStore = createUserStore(app);

  await userStore.setStatus({
    status: {
      [1]: { block: { number: 10, timestamp: 10 }, ready: false },
    },
  });

  const status = await userStore.getStatus();

  expect(status).toStrictEqual({
    [1]: { block: { number: 10, timestamp: 10 }, ready: false },
  });
});

async function getUserIndexNames(
  database: Database,
  namespace: string,
  tableName: string,
) {
  const rows = await database.qb.drizzle
    .select({
      name: sql<string>`indexname`.as("name"),
    })
    .from(sql`pg_indexes`)
    .where(and(eq(sql`schemaname`, namespace), eq(sql`tablename`, tableName)));
  return rows.map((r) => r.name);
}
