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
exports.createExit = void 0;
var node_os_1 = require("node:os");
var node_readline_1 = require("node:readline");
var errors_js_1 = require("@/internal/errors.js");
var SHUTDOWN_GRACE_PERIOD_MS = 5000;
/** Sets up shutdown handlers for the process. Accepts additional cleanup logic to run. */
var createExit = function (_a) {
  var common = _a.common;
  var isShuttingDown = false;
  var exit = function (_a) {
    var reason = _a.reason,
      code = _a.code;
    return __awaiter(void 0, void 0, void 0, function () {
      var timeout;
      return __generator(this, function (_b) {
        switch (_b.label) {
          case 0:
            if (isShuttingDown) return [2 /*return*/];
            isShuttingDown = true;
            timeout = setTimeout(function () {
              return __awaiter(void 0, void 0, void 0, function () {
                return __generator(this, function (_a) {
                  common.logger.fatal({
                    service: "process",
                    msg: "Failed to shutdown within 5 seconds, terminating (exit code 1)",
                  });
                  process.exit(1);
                  return [2 /*return*/];
                });
              });
            }, SHUTDOWN_GRACE_PERIOD_MS);
            if (reason !== undefined) {
              common.logger[code === 0 ? "info" : "warn"]({
                service: "process",
                msg: "".concat(reason, ", starting shutdown sequence"),
              });
            }
            common.telemetry.record({
              name: "lifecycle:session_end",
              properties: { duration_seconds: process.uptime() },
            });
            return [4 /*yield*/, common.shutdown.kill()];
          case 1:
            _b.sent();
            clearTimeout(timeout);
            if (process.stdin.isTTY) {
              process.stdin.setRawMode(false);
              process.stdin.pause();
            }
            process.exit(code);
            return [2 /*return*/];
        }
      });
    });
  };
  if (node_os_1.default.platform() === "win32") {
    var readlineInterface = node_readline_1.default.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    readlineInterface.on("SIGINT", function () {
      return exit({ reason: "Received SIGINT", code: 0 });
    });
  }
  process.on("SIGINT", function () {
    return exit({ reason: "Received SIGINT", code: 0 });
  });
  process.on("SIGTERM", function () {
    return exit({ reason: "Received SIGTERM", code: 0 });
  });
  process.on("SIGQUIT", function () {
    return exit({ reason: "Received SIGQUIT", code: 0 });
  });
  process.on("uncaughtException", function (error) {
    if (error instanceof errors_js_1.ShutdownError) return;
    common.logger.error({
      service: "process",
      msg: "Caught uncaughtException event",
      error: error,
    });
    exit({ reason: "Received uncaughtException", code: 1 });
  });
  process.on("unhandledRejection", function (error) {
    if (error instanceof errors_js_1.ShutdownError) return;
    common.logger.error({
      service: "process",
      msg: "Caught unhandledRejection event",
      error: error,
    });
    exit({ reason: "Received unhandledRejection", code: 1 });
  });
  return exit;
};
exports.createExit = createExit;
