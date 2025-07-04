"use strict";
var __assign =
  (this && this.__assign) ||
  function () {
    __assign =
      Object.assign ||
      function (t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
          s = arguments[i];
          for (var p in s)
            if (Object.prototype.hasOwnProperty.call(s, p)) t[p] = s[p];
        }
        return t;
      };
    return __assign.apply(this, arguments);
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.dev = void 0;
var node_fs_1 = require("node:fs");
var node_path_1 = require("node:path");
var index_js_1 = require("@/build/index.js");
var index_js_2 = require("@/database/index.js");
var logger_js_1 = require("@/internal/logger.js");
var metrics_js_1 = require("@/internal/metrics.js");
var options_js_1 = require("@/internal/options.js");
var shutdown_js_1 = require("@/internal/shutdown.js");
var telemetry_js_1 = require("@/internal/telemetry.js");
var index_js_3 = require("@/ui/index.js");
var queue_js_1 = require("@/utils/queue.js");
var result_js_1 = require("@/utils/result.js");
var exit_js_1 = require("../utils/exit.js");
var run_js_1 = require("../utils/run.js");
var runServer_js_1 = require("../utils/runServer.js");
function dev(_a) {
  var _b, _c;
  var cliOptions = _a.cliOptions;
  return __awaiter(this, void 0, void 0, function () {
    var options,
      logger,
      _d,
      major,
      minor,
      _patch,
      configRelPath,
      metrics,
      indexingShutdown,
      apiShutdown,
      shutdown,
      telemetry,
      common,
      build,
      exit,
      isInitialBuild,
      buildQueue,
      indexingBuild,
      database,
      crashRecoveryCheckpoint,
      schema;
    var _this = this;
    return __generator(this, function (_e) {
      switch (_e.label) {
        case 0:
          options = (0, options_js_1.buildOptions)({ cliOptions: cliOptions });
          logger = (0, logger_js_1.createLogger)({
            level: options.logLevel,
            mode: options.logFormat,
          });
          (_d = process.versions.node.split(".").map(Number)),
            (major = _d[0]),
            (minor = _d[1]),
            (_patch = _d[2]);
          if (major < 18 || (major === 18 && minor < 14)) {
            logger.fatal({
              service: "process",
              msg: "Invalid Node.js version. Expected >=18.14, detected "
                .concat(major, ".")
                .concat(minor, "."),
            });
            process.exit(1);
          }
          if (
            !node_fs_1.default.existsSync(
              node_path_1.default.join(options.rootDir, ".env.local"),
            )
          ) {
            logger.warn({
              service: "app",
              msg: "Local environment file (.env.local) not found",
            });
          }
          configRelPath = node_path_1.default.relative(
            options.rootDir,
            options.configFile,
          );
          logger.debug({
            service: "app",
            msg: "Started using config file: ".concat(configRelPath),
          });
          metrics = new metrics_js_1.MetricsService();
          indexingShutdown = (0, shutdown_js_1.createShutdown)();
          apiShutdown = (0, shutdown_js_1.createShutdown)();
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
          };
          if (options.version) {
            metrics.ponder_version_info.set(
              {
                version: options.version.version,
                major: options.version.major,
                minor: options.version.minor,
                patch: options.version.patch,
              },
              1,
            );
          }
          return [
            4 /*yield*/,
            (0, index_js_1.createBuild)({
              common: __assign(__assign({}, common), { shutdown: shutdown }),
              cliOptions: cliOptions,
            }),
          ];
        case 1:
          build = _e.sent();
          shutdown.add(function () {
            return __awaiter(_this, void 0, void 0, function () {
              return __generator(this, function (_a) {
                switch (_a.label) {
                  case 0:
                    return [4 /*yield*/, indexingShutdown.kill()];
                  case 1:
                    _a.sent();
                    return [4 /*yield*/, apiShutdown.kill()];
                  case 2:
                    _a.sent();
                    return [2 /*return*/];
                }
              });
            });
          });
          if (cliOptions.disableUi !== true) {
            (0, index_js_3.createUi)({
              common: __assign(__assign({}, common), { shutdown: shutdown }),
            });
          }
          exit = (0, exit_js_1.createExit)({
            common: __assign(__assign({}, common), { shutdown: shutdown }),
          });
          isInitialBuild = true;
          buildQueue = (0, queue_js_1.createQueue)({
            initialStart: true,
            concurrency: 1,
            worker: function (result) {
              return __awaiter(_this, void 0, void 0, function () {
                var configResult,
                  schemaResult,
                  buildResult1,
                  _a,
                  preBuild,
                  schemaBuild,
                  indexingResult,
                  indexingBuildResult,
                  apiResult,
                  apiBuildResult,
                  apiResult,
                  buildResult,
                  apiBuild;
                return __generator(this, function (_b) {
                  switch (_b.label) {
                    case 0:
                      if (!(result.kind === "indexing"))
                        return [3 /*break*/, 2];
                      return [4 /*yield*/, indexingShutdown.kill()];
                    case 1:
                      _b.sent();
                      indexingShutdown = (0, shutdown_js_1.createShutdown)();
                      _b.label = 2;
                    case 2:
                      return [4 /*yield*/, apiShutdown.kill()];
                    case 3:
                      _b.sent();
                      apiShutdown = (0, shutdown_js_1.createShutdown)();
                      if (result.status === "error") {
                        // This handles indexing function build failures on hot reload.
                        metrics.ponder_indexing_has_error.set(1);
                        return [2 /*return*/];
                      }
                      if (!(result.kind === "indexing"))
                        return [3 /*break*/, 12];
                      metrics.resetIndexingMetrics();
                      return [4 /*yield*/, build.executeConfig()];
                    case 4:
                      configResult = _b.sent();
                      if (configResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "indexing",
                          error: configResult.error,
                        });
                        return [2 /*return*/];
                      }
                      return [4 /*yield*/, build.executeSchema()];
                    case 5:
                      schemaResult = _b.sent();
                      if (schemaResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "indexing",
                          error: schemaResult.error,
                        });
                        return [2 /*return*/];
                      }
                      buildResult1 = (0, result_js_1.mergeResults)([
                        build.preCompile(configResult.result),
                        build.compileSchema(schemaResult.result),
                      ]);
                      if (buildResult1.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "indexing",
                          error: buildResult1.error,
                        });
                        return [2 /*return*/];
                      }
                      (_a = buildResult1.result),
                        (preBuild = _a[0]),
                        (schemaBuild = _a[1]);
                      return [4 /*yield*/, build.executeIndexingFunctions()];
                    case 6:
                      indexingResult = _b.sent();
                      if (indexingResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "indexing",
                          error: indexingResult.error,
                        });
                        return [2 /*return*/];
                      }
                      return [
                        4 /*yield*/,
                        build.compileIndexing({
                          configResult: configResult.result,
                          schemaResult: schemaResult.result,
                          indexingResult: indexingResult.result,
                        }),
                      ];
                    case 7:
                      indexingBuildResult = _b.sent();
                      if (indexingBuildResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "indexing",
                          error: indexingBuildResult.error,
                        });
                        return [2 /*return*/];
                      }
                      indexingBuild = indexingBuildResult.result;
                      return [
                        4 /*yield*/,
                        (0, index_js_2.createDatabase)({
                          common: __assign(__assign({}, common), {
                            shutdown: indexingShutdown,
                          }),
                          namespace: { schema: schema, viewsSchema: undefined },
                          preBuild: preBuild,
                          schemaBuild: schemaBuild,
                        }),
                      ];
                    case 8:
                      database = _b.sent();
                      return [
                        4 /*yield*/,
                        database.migrate(indexingBuildResult.result),
                      ];
                    case 9:
                      crashRecoveryCheckpoint = _b.sent();
                      return [
                        4 /*yield*/,
                        build.executeApi({
                          indexingBuild: indexingBuild,
                          database: database,
                        }),
                      ];
                    case 10:
                      apiResult = _b.sent();
                      if (apiResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "indexing",
                          error: apiResult.error,
                        });
                        return [2 /*return*/];
                      }
                      return [
                        4 /*yield*/,
                        build.compileApi({
                          apiResult: apiResult.result,
                        }),
                      ];
                    case 11:
                      apiBuildResult = _b.sent();
                      if (apiBuildResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "indexing",
                          error: apiBuildResult.error,
                        });
                        return [2 /*return*/];
                      }
                      if (isInitialBuild) {
                        isInitialBuild = false;
                        telemetry.record({
                          name: "lifecycle:session_start",
                          properties: __assign(
                            { cli_command: "dev" },
                            (0, telemetry_js_1.buildPayload)({
                              preBuild: preBuild,
                              schemaBuild: schemaBuild,
                              indexingBuild: indexingBuildResult.result,
                            }),
                          ),
                        });
                      }
                      metrics.resetApiMetrics();
                      metrics.ponder_settings_info.set(
                        {
                          ordering: preBuild.ordering,
                          database: preBuild.databaseConfig.kind,
                          command: cliOptions.command,
                        },
                        1,
                      );
                      (0, runServer_js_1.runServer)({
                        common: __assign(__assign({}, common), {
                          shutdown: apiShutdown,
                        }),
                        database: database,
                        apiBuild: apiBuildResult.result,
                      });
                      (0, run_js_1.run)({
                        common: __assign(__assign({}, common), {
                          shutdown: indexingShutdown,
                        }),
                        database: database,
                        preBuild: preBuild,
                        namespaceBuild: {
                          schema: schema,
                          viewsSchema: undefined,
                        },
                        schemaBuild: schemaBuild,
                        indexingBuild: indexingBuildResult.result,
                        crashRecoveryCheckpoint: crashRecoveryCheckpoint,
                        onFatalError: function () {
                          exit({ reason: "Received fatal error", code: 1 });
                        },
                        onReloadableError: function (error) {
                          buildQueue.clear();
                          buildQueue.add({
                            status: "error",
                            kind: "indexing",
                            error: error,
                          });
                        },
                      });
                      return [3 /*break*/, 15];
                    case 12:
                      metrics.resetApiMetrics();
                      return [
                        4 /*yield*/,
                        build.executeApi({
                          indexingBuild: indexingBuild,
                          database: database,
                        }),
                      ];
                    case 13:
                      apiResult = _b.sent();
                      if (apiResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "api",
                          error: apiResult.error,
                        });
                        return [2 /*return*/];
                      }
                      return [
                        4 /*yield*/,
                        build.compileApi({
                          apiResult: apiResult.result,
                        }),
                      ];
                    case 14:
                      buildResult = _b.sent();
                      if (buildResult.status === "error") {
                        buildQueue.add({
                          status: "error",
                          kind: "api",
                          error: buildResult.error,
                        });
                        return [2 /*return*/];
                      }
                      apiBuild = buildResult.result;
                      (0, runServer_js_1.runServer)({
                        common: __assign(__assign({}, common), {
                          shutdown: apiShutdown,
                        }),
                        database: database,
                        apiBuild: apiBuild,
                      });
                      _b.label = 15;
                    case 15:
                      return [2 /*return*/];
                  }
                });
              });
            },
          });
          schema =
            (_c =
              (_b = cliOptions.schema) !== null && _b !== void 0
                ? _b
                : process.env.DATABASE_SCHEMA) !== null && _c !== void 0
              ? _c
              : "public";
          globalThis.PONDER_NAMESPACE_BUILD = {
            schema: schema,
            viewsSchema: undefined,
          };
          build.startDev({
            onReload: function (kind) {
              buildQueue.clear();
              buildQueue.add({ status: "success", kind: kind });
            },
          });
          buildQueue.add({ status: "success", kind: "indexing" });
          return [2 /*return*/, shutdown.kill];
      }
    });
  });
}
exports.dev = dev;
