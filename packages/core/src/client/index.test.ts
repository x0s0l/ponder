import {
  setupCleanup,
  setupCommon,
  setupDatabaseConfig,
  setupPonder,
} from "@/_test/setup.js";
import { bigint, hex, onchainTable } from "@/drizzle/onchain.js";
import type { QueryWithTypings } from "drizzle-orm";
import { pgSchema } from "drizzle-orm/pg-core";
import { Hono } from "hono";
import superjson from "superjson";
import { beforeEach, expect, test } from "vitest";
import { client } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupDatabaseConfig);
beforeEach(setupCleanup);

const queryToParams = (query: QueryWithTypings) =>
  new URLSearchParams({ sql: superjson.stringify(query) });

test("client.db", async (context) => {
  globalThis.PONDER_NAMESPACE_BUILD = "public";

  const account = onchainTable("account", (p) => ({
    address: p.hex().primaryKey(),
    balance: p.bigint(),
  }));

  const app = await setupPonder(context, { schema: { account } });

  globalThis.PONDER_DATABASE = app.database;

  const hono = new Hono().use(
    client({
      db: app.database.qb.drizzleReadonly,
      schema: { account },
    }),
  );

  let query = {
    sql: "SELECT * FROM account",
    params: [],
  };

  let response = await hono.request(`/sql/db?${queryToParams(query)}`);
  expect(response.status).toBe(200);
  const result = await response.json();
  expect(result.rows).toStrictEqual([]);

  query = {
    sql: "SELECT 1;",
    params: [],
  };

  response = await hono.request(`/sql/db?${queryToParams(query)}`);
  expect(response.status).toBe(200);
});

test("client.db error", async (context) => {
  const app = await setupPonder(context);
  globalThis.PONDER_DATABASE = app.database;

  const hono = new Hono().use(
    client({
      db: app.database.qb.drizzleReadonly,
      schema: {},
    }),
  );

  globalThis.PONDER_NAMESPACE_BUILD = "public";
  globalThis.PONDER_DATABASE = app.database;

  const query = {
    sql: "SELECT * FROM account",
    params: [],
  };

  const response = await hono.request(`/sql/db?${queryToParams(query)}`);
  expect(response.status).toBe(500);
  expect(await response.text()).toContain('relation "account" does not exist');
});

test("client.db search_path", async (context) => {
  globalThis.PONDER_NAMESPACE_BUILD = "Ponder";

  const schemaAccount = pgSchema("Ponder").table("account", {
    address: hex().primaryKey(),
    balance: bigint(),
  });

  const app = await setupPonder(context, {
    namespace: "Ponder",
    schema: { account: schemaAccount },
  });

  globalThis.PONDER_DATABASE = app.database;

  const hono = new Hono().use(
    client({
      db: app.database.qb.drizzleReadonly,
      schema: { account: schemaAccount },
    }),
  );

  const query = {
    sql: "SELECT * FROM account",
    params: [],
  };

  const response = await hono.request(`/sql/db?${queryToParams(query)}`);
  expect(response.status).toBe(200);
});

test("client.db readonly", async (context) => {
  globalThis.PONDER_NAMESPACE_BUILD = "public";

  const account = onchainTable("account", (p) => ({
    address: p.hex().primaryKey(),
    balance: p.bigint(),
  }));

  const app = await setupPonder(context, {
    schema: { account },
  });

  globalThis.PONDER_DATABASE = app.database;

  const hono = new Hono().use(
    client({ db: app.database.qb.drizzleReadonly, schema: { account } }),
  );

  const query = {
    sql: "INSERT INTO account (address, balance) VALUES ('0x123', 100)",
    params: [],
  };

  const response = await hono.request(`/sql/db?${queryToParams(query)}`);
  expect(response.status).toBe(500);
  expect(await response.text()).toContain("InsertStmt not supported");
});

test("client.db recursive", async (context) => {
  globalThis.PONDER_NAMESPACE_BUILD = "public";

  const account = onchainTable("account", (p) => ({
    address: p.hex().primaryKey(),
    balance: p.bigint(),
  }));

  const app = await setupPonder(context, {
    schema: { account },
  });

  globalThis.PONDER_DATABASE = app.database;

  const hono = new Hono().use(
    client({ db: app.database.qb.drizzleReadonly, schema: { account } }),
  );

  const query = {
    sql: `
WITH RECURSIVE infinite_cte AS (
  SELECT 1 AS num
  UNION ALL
  SELECT num + 1
  FROM infinite_cte
)
SELECT *
FROM infinite_cte;`,
    params: [],
  };

  const response = await hono.request(`/sql/db?${queryToParams(query)}`);
  expect(response.status).toBe(500);
  expect(await response.text()).toContain("Recursive CTEs not supported");
});
