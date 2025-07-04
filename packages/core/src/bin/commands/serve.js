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
exports.serve = void 0;
var node_path_1 = require("node:path");
var index_js_1 = require("@/build/index.js");
var index_js_2 = require("@/database/index.js");
var logger_js_1 = require("@/internal/logger.js");
var metrics_js_1 = require("@/internal/metrics.js");
var options_js_1 = require("@/internal/options.js");
var shutdown_js_1 = require("@/internal/shutdown.js");
var telemetry_js_1 = require("@/internal/telemetry.js");
var index_js_3 = require("@/server/index.js");
var result_js_1 = require("@/utils/result.js");
var exit_js_1 = require("../utils/exit.js");
function serve(_a) {
  var cliOptions = _a.cliOptions;
  return __awaiter(this, void 0, void 0, function () {
    var options,
      logger,
      _b,
      major,
      minor,
      _patch,
      configRelPath,
      metrics,
      shutdown,
      telemetry,
      common,
      build,
      exit,
      namespaceResult,
      configResult,
      schemaResult,
      buildResult1,
      _c,
      preBuild,
      schemaBuild,
      indexingResult,
      indexingBuildResult,
      database,
      apiResult,
      buildResult2,
      apiBuild;
    return __generator(this, function (_d) {
      switch (_d.label) {
        case 0:
          options = (0, options_js_1.buildOptions)({ cliOptions: cliOptions });
          logger = (0, logger_js_1.createLogger)({
            level: options.logLevel,
            mode: options.logFormat,
          });
          (_b = process.versions.node.split(".").map(Number)),
            (major = _b[0]),
            (minor = _b[1]),
            (_patch = _b[2]);
          if (major < 18 || (major === 18 && minor < 14)) {
            logger.fatal({
              service: "process",
              msg: "Invalid Node.js version. Expected >=18.14, detected "
                .concat(major, ".")
                .concat(minor, "."),
            });
            process.exit(1);
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
              common: common,
              cliOptions: cliOptions,
            }),
          ];
        case 1:
          build = _d.sent();
          exit = (0, exit_js_1.createExit)({ common: common });
          namespaceResult = build.namespaceCompile();
          if (!(namespaceResult.status === "error")) return [3 /*break*/, 3];
          return [
            4 /*yield*/,
            exit({ reason: "Failed to initialize namespace", code: 1 }),
          ];
        case 2:
          _d.sent();
          return [2 /*return*/];
        case 3:
          return [4 /*yield*/, build.executeConfig()];
        case 4:
          configResult = _d.sent();
          if (!(configResult.status === "error")) return [3 /*break*/, 6];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 5:
          _d.sent();
          return [2 /*return*/];
        case 6:
          return [4 /*yield*/, build.executeSchema()];
        case 7:
          schemaResult = _d.sent();
          if (!(schemaResult.status === "error")) return [3 /*break*/, 9];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 8:
          _d.sent();
          return [2 /*return*/];
        case 9:
          buildResult1 = (0, result_js_1.mergeResults)([
            build.preCompile(configResult.result),
            build.compileSchema(schemaResult.result),
          ]);
          if (!(buildResult1.status === "error")) return [3 /*break*/, 11];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 10:
          _d.sent();
          return [2 /*return*/];
        case 11:
          (_c = buildResult1.result), (preBuild = _c[0]), (schemaBuild = _c[1]);
          if (!(preBuild.databaseConfig.kind === "pglite"))
            return [3 /*break*/, 13];
          return [
            4 /*yield*/,
            exit({
              reason: "The 'ponder serve' command does not support PGlite",
              code: 1,
            }),
          ];
        case 12:
          _d.sent();
          return [2 /*return*/];
        case 13:
          return [4 /*yield*/, build.executeIndexingFunctions()];
        case 14:
          indexingResult = _d.sent();
          if (!(indexingResult.status === "error")) return [3 /*break*/, 16];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 15:
          _d.sent();
          return [2 /*return*/];
        case 16:
          return [
            4 /*yield*/,
            build.compileIndexing({
              configResult: configResult.result,
              schemaResult: schemaResult.result,
              indexingResult: indexingResult.result,
            }),
          ];
        case 17:
          indexingBuildResult = _d.sent();
          if (!(indexingBuildResult.status === "error"))
            return [3 /*break*/, 19];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 18:
          _d.sent();
          return [2 /*return*/];
        case 19:
          return [
            4 /*yield*/,
            (0, index_js_2.createDatabase)({
              common: common,
              namespace: namespaceResult.result,
              preBuild: preBuild,
              schemaBuild: schemaBuild,
            }),
          ];
        case 20:
          database = _d.sent();
          return [
            4 /*yield*/,
            build.executeApi({
              indexingBuild: indexingBuildResult.result,
              database: database,
            }),
          ];
        case 21:
          apiResult = _d.sent();
          if (!(apiResult.status === "error")) return [3 /*break*/, 23];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 22:
          _d.sent();
          return [2 /*return*/];
        case 23:
          return [
            4 /*yield*/,
            build.compileApi({ apiResult: apiResult.result }),
          ];
        case 24:
          buildResult2 = _d.sent();
          if (!(buildResult2.status === "error")) return [3 /*break*/, 26];
          return [
            4 /*yield*/,
            exit({ reason: "Failed intial build", code: 1 }),
          ];
        case 25:
          _d.sent();
          return [2 /*return*/];
        case 26:
          apiBuild = buildResult2.result;
          telemetry.record({
            name: "lifecycle:session_start",
            properties: __assign(
              { cli_command: "serve" },
              (0, telemetry_js_1.buildPayload)({
                preBuild: preBuild,
                schemaBuild: schemaBuild,
              }),
            ),
          });
          metrics.ponder_settings_info.set(
            {
              database: preBuild.databaseConfig.kind,
              command: cliOptions.command,
            },
            1,
          );
          (0, index_js_3.createServer)({
            common: common,
            database: database,
            apiBuild: apiBuild,
          });
          return [2 /*return*/, shutdown.kill];
      }
    });
  });
}
exports.serve = serve;
