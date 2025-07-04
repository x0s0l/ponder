"use strict";
var __makeTemplateObject =
  (this && this.__makeTemplateObject) ||
  function (cooked, raw) {
    if (Object.defineProperty) {
      Object.defineProperty(cooked, "raw", { value: raw });
    } else {
      cooked.raw = raw;
    }
    return cooked;
  };
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
var __spreadArray =
  (this && this.__spreadArray) ||
  function (to, from, pack) {
    if (pack || arguments.length === 2)
      for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
          if (!ar) ar = Array.prototype.slice.call(from, 0, i);
          ar[i] = from[i];
        }
      }
    return to.concat(ar || Array.prototype.slice.call(from));
  };
Object.defineProperty(exports, "__esModule", { value: true });
exports.list = void 0;
var index_js_1 = require("@/build/index.js");
var index_js_2 = require("@/database/index.js");
var index_js_3 = require("@/database/index.js");
var logger_js_1 = require("@/internal/logger.js");
var metrics_js_1 = require("@/internal/metrics.js");
var options_js_1 = require("@/internal/options.js");
var shutdown_js_1 = require("@/internal/shutdown.js");
var telemetry_js_1 = require("@/internal/telemetry.js");
var app_js_1 = require("@/ui/app.js");
var format_js_1 = require("@/utils/format.js");
var drizzle_orm_1 = require("drizzle-orm");
var pg_core_1 = require("drizzle-orm/pg-core");
var exit_js_1 = require("../utils/exit.js");
var emptySchemaBuild = {
  schema: {},
  statements: {
    tables: { sql: [], json: [] },
    enums: { sql: [], json: [] },
    indexes: { sql: [], json: [] },
  },
};
function list(_a) {
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
      ponderSchemas,
      ponderViewSchemas,
      queries,
      result,
      columns,
      rows,
      lines,
      text;
    return __generator(this, function (_b) {
      switch (_b.label) {
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
          build = _b.sent();
          exit = (0, exit_js_1.createExit)({ common: common });
          return [4 /*yield*/, build.executeConfig()];
        case 2:
          configResult = _b.sent();
          if (!(configResult.status === "error")) return [3 /*break*/, 4];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 3:
          _b.sent();
          return [2 /*return*/];
        case 4:
          buildResult = build.preCompile(configResult.result);
          if (!(buildResult.status === "error")) return [3 /*break*/, 6];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 5:
          _b.sent();
          return [2 /*return*/];
        case 6:
          return [
            4 /*yield*/,
            (0, index_js_2.createDatabase)({
              common: common,
              // Note: `namespace` is not used in this command
              namespace: { schema: "public", viewsSchema: undefined },
              preBuild: buildResult.result,
              schemaBuild: emptySchemaBuild,
            }),
          ];
        case 7:
          database = _b.sent();
          return [
            4 /*yield*/,
            database.qb.drizzle
              .select({ schema: index_js_3.TABLES.table_schema })
              .from(index_js_3.TABLES)
              .where(
                (0, drizzle_orm_1.eq)(
                  index_js_3.TABLES.table_name,
                  "_ponder_meta",
                ),
              ),
          ];
        case 8:
          ponderSchemas = _b.sent();
          return [
            4 /*yield*/,
            database.qb.drizzle
              .select({ schema: index_js_2.VIEWS.table_schema })
              .from(index_js_2.VIEWS)
              .where(
                (0, drizzle_orm_1.eq)(
                  index_js_2.VIEWS.table_name,
                  "_ponder_meta",
                ),
              ),
          ];
        case 9:
          ponderViewSchemas = _b.sent();
          queries = ponderSchemas.map(function (row) {
            return database.qb.drizzle
              .select({
                value: (0, index_js_2.getPonderMetaTable)(row.schema).value,
                schema: (0, drizzle_orm_1.sql)(
                  templateObject_1 ||
                    (templateObject_1 = __makeTemplateObject(
                      ["", ""],
                      ["", ""],
                    )),
                  row.schema,
                ).as("schema"),
              })
              .from((0, index_js_2.getPonderMetaTable)(row.schema))
              .where(
                (0, drizzle_orm_1.eq)(
                  (0, index_js_2.getPonderMetaTable)(row.schema).key,
                  "app",
                ),
              );
          });
          if (!(queries.length === 0)) return [3 /*break*/, 11];
          logger.warn({
            service: "list",
            msg: "No 'ponder start' apps found in this database.",
          });
          return [4 /*yield*/, exit({ reason: "Success", code: 0 })];
        case 10:
          _b.sent();
          return [2 /*return*/];
        case 11:
          if (!(queries.length === 1)) return [3 /*break*/, 13];
          return [4 /*yield*/, queries[0]];
        case 12:
          result = _b.sent();
          return [3 /*break*/, 15];
        case 13:
          return [4 /*yield*/, pg_core_1.unionAll.apply(void 0, queries)];
        case 14:
          // @ts-ignore
          result = _b.sent();
          _b.label = 15;
        case 15:
          columns = [
            { title: "Schema", key: "table_schema", align: "left" },
            { title: "Active", key: "active", align: "right" },
            { title: "Last active", key: "last_active", align: "right" },
            { title: "Table count", key: "table_count", align: "right" },
            { title: "Is view", key: "is_view", align: "right" },
          ];
          rows = result
            .filter(function (row) {
              return row.value.is_dev === 0;
            })
            .map(function (row) {
              return {
                table_schema: row.schema,
                active:
                  row.value.is_locked === 1 &&
                  row.value.heartbeat_at +
                    common.options.databaseHeartbeatTimeout >
                    Date.now()
                    ? "yes"
                    : "no",
                last_active:
                  row.value.is_locked === 1
                    ? "---"
                    : "".concat(
                        (0, format_js_1.formatEta)(
                          Date.now() - row.value.heartbeat_at,
                        ),
                        " ago",
                      ),
                table_count: row.value.table_names.length,
                is_view: ponderViewSchemas.some(function (schema) {
                  return schema.schema === row.schema;
                })
                  ? "yes"
                  : "no",
              };
            });
          if (!(rows.length === 0)) return [3 /*break*/, 17];
          logger.warn({
            service: "list",
            msg: "No 'ponder start' apps found in this database.",
          });
          return [4 /*yield*/, exit({ reason: "Success", code: 0 })];
        case 16:
          _b.sent();
          return [2 /*return*/];
        case 17:
          lines = (0, app_js_1.buildTable)(rows, columns);
          text = __spreadArray(
            __spreadArray([], lines, true),
            [""],
            false,
          ).join("\n");
          console.log(text);
          return [4 /*yield*/, exit({ reason: "Success", code: 0 })];
        case 18:
          _b.sent();
          return [2 /*return*/];
      }
    });
  });
}
exports.list = list;
var templateObject_1;
