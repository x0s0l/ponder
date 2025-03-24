import { getPonderMeta } from "@/database/index.js";
import { getPonderStatus } from "@/database/index.js";
import { getPrimaryKeyColumns, getTableNames } from "@/drizzle/index.js";
import { getColumnCasing, getReorgTable } from "@/drizzle/kit/index.js";
import type { PonderApp, Schema, Status } from "@/internal/types.js";
import type { Drizzle } from "@/types/db.js";
import {
  MAX_CHECKPOINT_STRING,
  ZERO_CHECKPOINT_STRING,
  decodeCheckpoint,
} from "@/utils/checkpoint.js";
import {
  type TableConfig,
  eq,
  getTableColumns,
  getTableName,
  is,
  lte,
  sql,
} from "drizzle-orm";
import { PgTable, type PgTableWithColumns } from "drizzle-orm/pg-core";

export type UserStore = {
  /** Determine the app checkpoint, possibly reverting unfinalized rows. */
  recoverCheckpoint(): Promise<string>;
  createIndexes(): Promise<void>;
  createTriggers(): Promise<void>;
  removeTriggers(): Promise<void>;
  getStatus: () => Promise<Status | null>;
  setStatus: (params: { status: Status }) => Promise<void>;
  finalize(params: { checkpoint: string }): Promise<void>;
  complete(params: { checkpoint: string }): Promise<void>;
};

export const createUserStore = (
  app: Omit<PonderApp, "buildId" | "indexingBuild" | "apiBuild">,
  { db = app.database.qb.drizzle }: { db?: Drizzle<Schema> } = {
    db: app.database.qb.drizzle,
  },
): UserStore => {
  const PONDER_META = getPonderMeta(app.namespace);
  const PONDER_STATUS = getPonderStatus(app.namespace);

  const tables = Object.values(app.schemaBuild.schema).filter(
    (table): table is PgTableWithColumns<TableConfig> => is(table, PgTable),
  );

  return {
    async recoverCheckpoint() {
      // new tables are empty
      if (createdTables) return ZERO_CHECKPOINT_STRING;

      return app.database.wrap(
        { method: "recoverCheckpoint", includeTraceLogs: true },
        () =>
          db.transaction(async (tx) => {
            const meta = await tx
              .select({ value: PONDER_META.value })
              .from(PONDER_META)
              .where(eq(PONDER_META.key, "app"))
              .then((result) => result[0]!.value);

            if (meta.checkpoint === ZERO_CHECKPOINT_STRING) {
              for (const table of tables) {
                await tx.execute(
                  sql.raw(
                    `TRUNCATE TABLE "${app.namespace}"."${getTableName(table)}", "${app.namespace}"."${getTableName(getReorgTable(table))}" CASCADE`,
                  ),
                );
              }
            } else {
              // Update metadata

              meta.is_locked = 1;
              meta.is_dev = app.common.options.command === "dev" ? 1 : 0;

              await tx
                .update(PONDER_META)
                .set({ value: meta })
                .where(eq(PONDER_META.key, "app"));

              // Remove triggers

              for (const table of tables) {
                await tx.execute(
                  sql.raw(
                    `DROP TRIGGER IF EXISTS "${getTableNames(table).trigger}" ON "${app.namespace}"."${getTableName(table)}"`,
                  ),
                );
              }

              // Remove indexes

              for (const indexStatement of app.schemaBuild.statements.indexes
                .json) {
                await tx.execute(
                  sql.raw(
                    `DROP INDEX IF EXISTS "${app.namespace}"."${indexStatement.data.name}"`,
                  ),
                );
                app.common.logger.info({
                  service: "database",
                  msg: `Dropped index '${indexStatement.data.name}' in schema '${app.namespace}'`,
                });
              }

              // Revert unfinalized data
              await revert(app, { tx, checkpoint: meta.checkpoint });
            }

            return meta.checkpoint;
          }),
      );
    },
    async createIndexes() {
      for (const statement of app.schemaBuild.statements.indexes.sql) {
        await db.execute(statement);
      }
    },
    async createTriggers() {
      await app.database.wrap(
        { method: "createTriggers", includeTraceLogs: true },
        async () => {
          for (const table of tables) {
            const columns = getTableColumns(table);

            const columnNames = Object.values(columns).map(
              (column) => `"${getColumnCasing(column, "snake_case")}"`,
            );

            await db.execute(
              sql.raw(`
CREATE OR REPLACE FUNCTION "${app.namespace}".${getTableNames(table).triggerFn}
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO "${app.namespace}"."${getTableName(getReorgTable(table))}" (${columnNames.join(",")}, operation, checkpoint)
    VALUES (${columnNames.map((name) => `NEW.${name}`).join(",")}, 0, '${MAX_CHECKPOINT_STRING}');
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO "${app.namespace}"."${getTableName(getReorgTable(table))}" (${columnNames.join(",")}, operation, checkpoint)
    VALUES (${columnNames.map((name) => `OLD.${name}`).join(",")}, 1, '${MAX_CHECKPOINT_STRING}');
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO "${app.namespace}"."${getTableName(getReorgTable(table))}" (${columnNames.join(",")}, operation, checkpoint)
    VALUES (${columnNames.map((name) => `OLD.${name}`).join(",")}, 2, '${MAX_CHECKPOINT_STRING}');
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql`),
            );

            await db.execute(
              sql.raw(`
CREATE OR REPLACE TRIGGER "${getTableNames(table).trigger}"
AFTER INSERT OR UPDATE OR DELETE ON "${app.namespace}"."${getTableName(table)}"
FOR EACH ROW EXECUTE FUNCTION "${app.namespace}".${getTableNames(table).triggerFn};
`),
            );
          }
        },
      );
    },
    async removeTriggers() {
      await app.database.wrap(
        { method: "removeTriggers", includeTraceLogs: true },
        async () => {
          for (const table of tables) {
            await db.execute(
              sql.raw(
                `DROP TRIGGER IF EXISTS "${getTableNames(table).trigger}" ON "${app.namespace}"."${getTableName(table)}"`,
              ),
            );
          }
        },
      );
    },
    getStatus() {
      return app.database.wrap({ method: "getStatus" }, async () => {
        const result = await db.select().from(PONDER_STATUS);

        if (result.length === 0) {
          return null;
        }

        const status: Status = {};

        for (const row of result) {
          status[row.network_name] = {
            block:
              row.block_number && row.block_timestamp
                ? {
                    number: row.block_number,
                    timestamp: row.block_timestamp,
                  }
                : null,
            ready: row.ready,
          };
        }

        return status;
      });
    },
    setStatus({ status }) {
      return app.database.wrap({ method: "setStatus" }, async () => {
        await db
          .insert(PONDER_STATUS)
          .values(
            Object.entries(status).map(([networkName, value]) => ({
              network_name: networkName,
              block_number: value.block?.number,
              block_timestamp: value.block?.timestamp,
              ready: value.ready,
            })),
          )
          .onConflictDoUpdate({
            target: PONDER_STATUS.network_name,
            set: {
              block_number: sql`excluded.block_number`,
              block_timestamp: sql`excluded.block_timestamp`,
              ready: sql`excluded.ready`,
            },
          });
      });
    },
    async finalize({ checkpoint }) {
      await app.database.record(
        { method: "finalize", includeTraceLogs: true },
        async () => {
          await db
            .update(PONDER_META)
            .set({
              value: sql`jsonb_set(value, '{checkpoint}', to_jsonb(${checkpoint}::varchar(75)))`,
            })
            .where(eq(PONDER_META.key, "app"));

          await Promise.all(
            tables.map((table) =>
              db
                .delete(getReorgTable(table))
                .where(lte(getReorgTable(table).checkpoint, checkpoint)),
            ),
          );
        },
      );

      const decoded = decodeCheckpoint(checkpoint);

      app.common.logger.debug({
        service: "database",
        msg: `Updated finalized checkpoint to (timestamp=${decoded.blockTimestamp} chainId=${decoded.chainId} block=${decoded.blockNumber})`,
      });
    },
    async complete({ checkpoint }) {
      await Promise.all(
        tables.map((table) =>
          app.database.wrap({ method: "complete" }, async () => {
            const reorgTable = getReorgTable(table);
            await db
              .update(reorgTable)
              .set({ checkpoint })
              .where(eq(reorgTable.checkpoint, MAX_CHECKPOINT_STRING));
          }),
        ),
      );
    },
  };
};

export const revert = async (
  app: Omit<PonderApp, "buildId" | "indexingBuild" | "apiBuild">,
  { tx, checkpoint }: { tx: Drizzle<Schema>; checkpoint: string },
) => {
  const tables = Object.values(app.schemaBuild.schema).filter(
    (table): table is PgTableWithColumns<TableConfig> => is(table, PgTable),
  );

  await app.database.record({ method: "revert", includeTraceLogs: true }, () =>
    Promise.all(
      tables.map(async (table) => {
        const primaryKeyColumns = getPrimaryKeyColumns(table);

        const result = await tx.execute(
          sql.raw(`
WITH reverted1 AS (
DELETE FROM "${app.namespace}"."${getTableName(getReorgTable(table))}"
WHERE checkpoint > '${checkpoint}' RETURNING *
), reverted2 AS (
SELECT ${primaryKeyColumns.map(({ sql }) => `"${sql}"`).join(", ")}, MIN(operation_id) AS operation_id FROM reverted1
GROUP BY ${primaryKeyColumns.map(({ sql }) => `"${sql}"`).join(", ")}
), reverted3 AS (
SELECT ${Object.values(getTableColumns(table))
            .map(
              (column) =>
                `reverted1."${getColumnCasing(column, "snake_case")}"`,
            )
            .join(", ")}, reverted1.operation FROM reverted2
INNER JOIN reverted1
ON ${primaryKeyColumns.map(({ sql }) => `reverted2."${sql}" = reverted1."${sql}"`).join("AND ")}
AND reverted2.operation_id = reverted1.operation_id
), inserted AS (
DELETE FROM "${app.namespace}"."${getTableName(table)}" as t
WHERE EXISTS (
SELECT * FROM reverted3
WHERE ${primaryKeyColumns.map(({ sql }) => `t."${sql}" = reverted3."${sql}"`).join("AND ")}
AND OPERATION = 0
)
RETURNING *
), updated_or_deleted AS (
INSERT INTO  "${app.namespace}"."${getTableName(table)}"
SELECT ${Object.values(getTableColumns(table))
            .map((column) => `"${getColumnCasing(column, "snake_case")}"`)
            .join(", ")} FROM reverted3
WHERE operation = 1 OR operation = 2
ON CONFLICT (${primaryKeyColumns.map(({ sql }) => `"${sql}"`).join(", ")})
DO UPDATE SET
${Object.values(getTableColumns(table))
  .map(
    (column) =>
      `"${getColumnCasing(column, "snake_case")}" = EXCLUDED."${getColumnCasing(column, "snake_case")}"`,
  )
  .join(", ")}
RETURNING *
) SELECT COUNT(*) FROM reverted1 as count;
`),
        );

        app.common.logger.info({
          service: "database",
          // @ts-ignore
          msg: `Reverted ${result.rows[0]!.count} unfinalized operations from '${getTableName(table)}'`,
        });
      }),
    ),
  );
};
