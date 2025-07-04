"use strict";
var __awaiter =
  (this && this.__awaiter) ||
  function (thisArg, _arguments, P, generator) {
    function adopt(value) {
      return value instanceof P
        ? value
        : new P(function (resolve) {
            resolve(value);
          });
    }
    return new (P || (P = Promise))(function (resolve, reject) {
      function fulfilled(value) {
        try {
          step(generator.next(value));
        } catch (e) {
          reject(e);
        }
      }
      function rejected(value) {
        try {
          step(generator["throw"](value));
        } catch (e) {
          reject(e);
        }
      }
      function step(result) {
        result.done
          ? resolve(result.value)
          : adopt(result.value).then(fulfilled, rejected);
      }
      step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
  };
var __generator =
  (this && this.__generator) ||
  function (thisArg, body) {
    var _ = {
        label: 0,
        sent: function () {
          if (t[0] & 1) throw t[1];
          return t[1];
        },
        trys: [],
        ops: [],
      },
      f,
      y,
      t,
      g;
    return (
      (g = { next: verb(0), throw: verb(1), return: verb(2) }),
      typeof Symbol === "function" &&
        (g[Symbol.iterator] = function () {
          return this;
        }),
      g
    );
    function verb(n) {
      return function (v) {
        return step([n, v]);
      };
    }
    function step(op) {
      if (f) throw new TypeError("Generator is already executing.");
      while ((g && ((g = 0), op[0] && (_ = 0)), _))
        try {
          if (
            ((f = 1),
            y &&
              (t =
                op[0] & 2
                  ? y["return"]
                  : op[0]
                    ? y["throw"] || ((t = y["return"]) && t.call(y), 0)
                    : y.next) &&
              !(t = t.call(y, op[1])).done)
          )
            return t;
          if (((y = 0), t)) op = [op[0] & 2, t.value];
          switch (op[0]) {
            case 0:
            case 1:
              t = op;
              break;
            case 4:
              _.label++;
              return { value: op[1], done: false };
            case 5:
              _.label++;
              y = op[1];
              op = [0];
              continue;
            case 7:
              op = _.ops.pop();
              _.trys.pop();
              continue;
            default:
              if (
                !((t = _.trys), (t = t.length > 0 && t[t.length - 1])) &&
                (op[0] === 6 || op[0] === 2)
              ) {
                _ = 0;
                continue;
              }
              if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) {
                _.label = op[1];
                break;
              }
              if (op[0] === 6 && _.label < t[1]) {
                _.label = t[1];
                t = op;
                break;
              }
              if (t && _.label < t[2]) {
                _.label = t[2];
                _.ops.push(op);
                break;
              }
              if (t[2]) _.ops.pop();
              _.trys.pop();
              continue;
          }
          op = body.call(thisArg, _);
        } catch (e) {
          op = [6, e];
          y = 0;
        } finally {
          f = t = 0;
        }
      if (op[0] & 5) throw op[1];
      return { value: op[0] ? op[1] : void 0, done: true };
    }
  };
Object.defineProperty(exports, "__esModule", { value: true });
exports.createViews = void 0;
var index_js_1 = require("@/build/index.js");
var index_js_2 = require("@/database/index.js");
var logger_js_1 = require("@/internal/logger.js");
var metrics_js_1 = require("@/internal/metrics.js");
var options_js_1 = require("@/internal/options.js");
var shutdown_js_1 = require("@/internal/shutdown.js");
var telemetry_js_1 = require("@/internal/telemetry.js");
var drizzle_orm_1 = require("drizzle-orm");
var exit_js_1 = require("../utils/exit.js");
var emptySchemaBuild = {
  schema: {},
  statements: {
    tables: { sql: [], json: [] },
    enums: { sql: [], json: [] },
    indexes: { sql: [], json: [] },
  },
};
function createViews(_a) {
  var cliOptions = _a.cliOptions;
  return __awaiter(this, void 0, void 0, function () {
    var options,
      logger,
      metrics,
      shutdown,
      telemetry,
      common,
      build,
      exit,
      configResult,
      buildResult,
      database,
      PONDER_META,
      meta,
      _i,
      _b,
      table,
      trigger,
      notification,
      channel;
    return __generator(this, function (_c) {
      switch (_c.label) {
        case 0:
          options = (0, options_js_1.buildOptions)({ cliOptions: cliOptions });
          logger = (0, logger_js_1.createLogger)({
            level: "warn",
            mode: options.logFormat,
          });
          metrics = new metrics_js_1.MetricsService();
          shutdown = (0, shutdown_js_1.createShutdown)();
          telemetry = (0, telemetry_js_1.createTelemetry)({
            options: options,
            logger: logger,
            shutdown: shutdown,
          });
          common = {
            options: options,
            logger: logger,
            metrics: metrics,
            telemetry: telemetry,
            shutdown: shutdown,
          };
          return [
            4 /*yield*/,
            (0, index_js_1.createBuild)({
              common: common,
              cliOptions: cliOptions,
            }),
          ];
        case 1:
          build = _c.sent();
          exit = (0, exit_js_1.createExit)({ common: common });
          if (!(cliOptions.schema === undefined)) return [3 /*break*/, 3];
          logger.warn({
            service: "create-views",
            msg: "Required CLI option '--schema' not provided.",
          });
          return [
            4 /*yield*/,
            exit({ reason: "Create views failed", code: 1 }),
          ];
        case 2:
          _c.sent();
          return [2 /*return*/];
        case 3:
          if (!(cliOptions.viewsSchema === undefined)) return [3 /*break*/, 5];
          logger.warn({
            service: "create-views",
            msg: "Required CLI option '--views-schema' not provided.",
          });
          return [
            4 /*yield*/,
            exit({ reason: "Create views failed", code: 1 }),
          ];
        case 4:
          _c.sent();
          return [2 /*return*/];
        case 5:
          return [4 /*yield*/, build.executeConfig()];
        case 6:
          configResult = _c.sent();
          if (!(configResult.status === "error")) return [3 /*break*/, 8];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 7:
          _c.sent();
          return [2 /*return*/];
        case 8:
          buildResult = build.preCompile(configResult.result);
          if (!(buildResult.status === "error")) return [3 /*break*/, 10];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 9:
          _c.sent();
          return [2 /*return*/];
        case 10:
          return [
            4 /*yield*/,
            (0, index_js_2.createDatabase)({
              common: common,
              // Note: `namespace` is not used in this command
              namespace: {
                schema: "public",
                viewsSchema: undefined,
              },
              preBuild: buildResult.result,
              schemaBuild: emptySchemaBuild,
            }),
          ];
        case 11:
          database = _c.sent();
          PONDER_META = (0, index_js_2.getPonderMetaTable)(cliOptions.schema);
          return [
            4 /*yield*/,
            database.qb.drizzle
              .select({ app: PONDER_META.value })
              .from(PONDER_META)
              .where((0, drizzle_orm_1.eq)(PONDER_META.key, "app")),
          ];
        case 12:
          meta = _c.sent();
          if (!(meta.length === 0)) return [3 /*break*/, 14];
          logger.warn({
            service: "create-views",
            msg: "No Ponder app found in schema ".concat(
              cliOptions.schema,
              ".",
            ),
          });
          return [
            4 /*yield*/,
            exit({ reason: "Create views failed", code: 0 }),
          ];
        case 13:
          _c.sent();
          return [2 /*return*/];
        case 14:
          return [
            4 /*yield*/,
            database.qb.drizzle.execute(
              drizzle_orm_1.sql.raw(
                'CREATE SCHEMA IF NOT EXISTS "'.concat(
                  cliOptions.viewsSchema,
                  '"',
                ),
              ),
            ),
          ];
        case 15:
          _c.sent();
          (_i = 0), (_b = meta[0].app.table_names);
          _c.label = 16;
        case 16:
          if (!(_i < _b.length)) return [3 /*break*/, 20];
          table = _b[_i];
          // Note: drop views before creating new ones to avoid enum errors.
          return [
            4 /*yield*/,
            database.qb.drizzle.execute(
              drizzle_orm_1.sql.raw(
                'DROP VIEW IF EXISTS "'
                  .concat(cliOptions.viewsSchema, '"."')
                  .concat(table, '"'),
              ),
            ),
          ];
        case 17:
          // Note: drop views before creating new ones to avoid enum errors.
          _c.sent();
          return [
            4 /*yield*/,
            database.qb.drizzle.execute(
              drizzle_orm_1.sql.raw(
                'CREATE VIEW "'
                  .concat(cliOptions.viewsSchema, '"."')
                  .concat(table, '" AS SELECT * FROM "')
                  .concat(cliOptions.schema, '"."')
                  .concat(table, '"'),
              ),
            ),
          ];
        case 18:
          _c.sent();
          _c.label = 19;
        case 19:
          _i++;
          return [3 /*break*/, 16];
        case 20:
          logger.warn({
            service: "create-views",
            msg: "Created "
              .concat(meta[0].app.table_names.length, ' views in schema "')
              .concat(cliOptions.viewsSchema, '"'),
          });
          return [
            4 /*yield*/,
            database.qb.drizzle.execute(
              drizzle_orm_1.sql.raw(
                'CREATE OR REPLACE VIEW "'
                  .concat(
                    cliOptions.viewsSchema,
                    '"."_ponder_meta" AS SELECT * FROM "',
                  )
                  .concat(cliOptions.schema, '"."_ponder_meta"'),
              ),
            ),
          ];
        case 21:
          _c.sent();
          return [
            4 /*yield*/,
            database.qb.drizzle.execute(
              drizzle_orm_1.sql.raw(
                'CREATE OR REPLACE VIEW "'
                  .concat(
                    cliOptions.viewsSchema,
                    '"."_ponder_checkpoint" AS SELECT * FROM "',
                  )
                  .concat(cliOptions.schema, '"."_ponder_checkpoint"'),
              ),
            ),
          ];
        case 22:
          _c.sent();
          trigger = "status_".concat(cliOptions.viewsSchema, "_trigger");
          notification = "status_notify()";
          channel = "".concat(cliOptions.viewsSchema, "_status_channel");
          return [
            4 /*yield*/,
            database.qb.drizzle.execute(
              drizzle_orm_1.sql.raw(
                '\nCREATE OR REPLACE FUNCTION "'
                  .concat(cliOptions.viewsSchema, '".')
                  .concat(
                    notification,
                    '\nRETURNS TRIGGER\nLANGUAGE plpgsql\nAS $$\nBEGIN\nNOTIFY "',
                  )
                  .concat(channel, '";\nRETURN NULL;\nEND;\n$$;'),
              ),
            ),
          ];
        case 23:
          _c.sent();
          return [
            4 /*yield*/,
            database.qb.drizzle.execute(
              drizzle_orm_1.sql.raw(
                '\nCREATE OR REPLACE TRIGGER "'
                  .concat(trigger, '"\nAFTER INSERT OR UPDATE OR DELETE\nON "')
                  .concat(
                    cliOptions.schema,
                    '"._ponder_checkpoint\nFOR EACH STATEMENT\nEXECUTE PROCEDURE "',
                  )
                  .concat(cliOptions.viewsSchema, '".')
                  .concat(notification, ";"),
              ),
            ),
          ];
        case 24:
          _c.sent();
          return [4 /*yield*/, exit({ reason: "Success", code: 0 })];
        case 25:
          _c.sent();
          return [2 /*return*/];
      }
    });
  });
}
exports.createViews = createViews;
