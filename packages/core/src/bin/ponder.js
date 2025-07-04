#!/usr/bin/env node
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
var node_fs_1 = require("node:fs");
var node_path_1 = require("node:path");
var node_url_1 = require("node:url");
var extra_typings_1 = require("@commander-js/extra-typings");
var dotenv_1 = require("dotenv");
var codegen_js_1 = require("./commands/codegen.js");
var createViews_js_1 = require("./commands/createViews.js");
var dev_js_1 = require("./commands/dev.js");
var list_js_1 = require("./commands/list.js");
var prune_js_1 = require("./commands/prune.js");
var serve_js_1 = require("./commands/serve.js");
var start_js_1 = require("./commands/start.js");
dotenv_1.default.config({ path: ".env.local" });
var __dirname = (0, node_path_1.dirname)(
  (0, node_url_1.fileURLToPath)(import.meta.url),
);
var packageJsonPath = (0, node_path_1.resolve)(
  __dirname,
  "../../../package.json",
);
var packageJson = JSON.parse(
  (0, node_fs_1.readFileSync)(packageJsonPath, { encoding: "utf8" }),
);
var ponder = new extra_typings_1.Command("ponder")
  .usage("<command> [OPTIONS]")
  .helpOption("-h, --help", "Show this help message")
  .helpCommand(false)
  .option(
    "--root <PATH>",
    "Path to the project root directory (default: working directory)",
  )
  .option(
    "--config <PATH>",
    "Path to the project config file",
    "ponder.config.ts",
  )
  .option(
    "-v, --debug",
    "Enable debug logs, e.g. realtime blocks, internal events",
  )
  .option(
    "-vv, --trace",
    "Enable trace logs, e.g. db queries, indexing checkpoints",
  )
  .option(
    "--log-level <LEVEL>",
    'Minimum log level ("error", "warn", "info", "debug", or "trace", default: "info")',
  )
  .option(
    "--log-format <FORMAT>",
    'The log format ("pretty" or "json")',
    "pretty",
  )
  .version(packageJson.version, "-V, --version", "Show the version number")
  .configureHelp({ showGlobalOptions: true })
  .allowExcessArguments(false)
  .showHelpAfterError()
  .enablePositionalOptions(false);
var devCommand = new extra_typings_1.Command("dev")
  .description("Start the development server with hot reloading")
  .option("--schema <SCHEMA>", "Database schema", String)
  .option("-p, --port <PORT>", "Port for the web server", Number, 42069)
  // NOTE: Do not set a default for hostname. We currently rely on the Node.js
  // default behavior when passing undefined to http.Server.listen(), which
  // detects the available interfaces (IPv4 and/or IPv6) and uses them.
  // Documentation: https://arc.net/l/quote/dnjmtumq
  .option(
    "-H, --hostname <HOSTNAME>",
    'Hostname for the web server (default: "0.0.0.0" or "::")',
  )
  .option("--disable-ui", "Disable the terminal UI")
  .showHelpAfterError()
  .action(function (_, command) {
    return __awaiter(void 0, void 0, void 0, function () {
      var cliOptions;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            cliOptions = __assign(__assign({}, command.optsWithGlobals()), {
              command: command.name(),
              version: packageJson.version,
            });
            return [4 /*yield*/, (0, dev_js_1.dev)({ cliOptions: cliOptions })];
          case 1:
            _a.sent();
            return [2 /*return*/];
        }
      });
    });
  });
var startCommand = new extra_typings_1.Command("start")
  .description("Start the production server")
  .option("--schema <SCHEMA>", "Database schema", String)
  .option("--views-schema <SCHEMA>", "Views database schema", String)
  .option("-p, --port <PORT>", "Port for the web server", Number, 42069)
  .option(
    "-H, --hostname <HOSTNAME>",
    'Hostname for the web server (default: "0.0.0.0" or "::")',
  )
  .showHelpAfterError()
  .action(function (_, command) {
    return __awaiter(void 0, void 0, void 0, function () {
      var cliOptions;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            cliOptions = __assign(__assign({}, command.optsWithGlobals()), {
              command: command.name(),
              version: packageJson.version,
            });
            return [
              4 /*yield*/,
              (0, start_js_1.start)({ cliOptions: cliOptions }),
            ];
          case 1:
            _a.sent();
            return [2 /*return*/];
        }
      });
    });
  });
var serveCommand = new extra_typings_1.Command("serve")
  .description("Start the production HTTP server without the indexer")
  .option("--schema <SCHEMA>", "Database schema", String)
  .option("-p, --port <PORT>", "Port for the web server", Number, 42069)
  .option(
    "-H, --hostname <HOSTNAME>",
    'Hostname for the web server (default: "0.0.0.0" or "::")',
  )
  .showHelpAfterError()
  .action(function (_, command) {
    return __awaiter(void 0, void 0, void 0, function () {
      var cliOptions;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            cliOptions = __assign(__assign({}, command.optsWithGlobals()), {
              command: command.name(),
              version: packageJson.version,
            });
            return [
              4 /*yield*/,
              (0, serve_js_1.serve)({ cliOptions: cliOptions }),
            ];
          case 1:
            _a.sent();
            return [2 /*return*/];
        }
      });
    });
  });
var createViewsCommand = new extra_typings_1.Command("create-views")
  .description("Create database views")
  .option("--schema <SCHEMA>", "Source database schema", String)
  .option("--views-schema <SCHEMA>", "Target database schema", String)
  .showHelpAfterError()
  .action(function (_, command) {
    return __awaiter(void 0, void 0, void 0, function () {
      var cliOptions;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            cliOptions = __assign(__assign({}, command.optsWithGlobals()), {
              command: command.name(),
              version: packageJson.version,
            });
            return [
              4 /*yield*/,
              (0, createViews_js_1.createViews)({ cliOptions: cliOptions }),
            ];
          case 1:
            _a.sent();
            return [2 /*return*/];
        }
      });
    });
  });
var dbCommand = new extra_typings_1.Command("db").description(
  "Database management commands",
);
var listCommand = new extra_typings_1.Command("list")
  .description("List all Ponder deployments")
  .showHelpAfterError()
  .action(function (_, command) {
    return __awaiter(void 0, void 0, void 0, function () {
      var cliOptions;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            cliOptions = __assign(__assign({}, command.optsWithGlobals()), {
              command: command.name(),
              version: packageJson.version,
            });
            return [
              4 /*yield*/,
              (0, list_js_1.list)({ cliOptions: cliOptions }),
            ];
          case 1:
            _a.sent();
            return [2 /*return*/];
        }
      });
    });
  });
var pruneCommand = new extra_typings_1.Command("prune")
  .description(
    "Drop all database tables, functions, and schemas created by Ponder deployments that are not active",
  )
  .showHelpAfterError()
  .action(function (_, command) {
    return __awaiter(void 0, void 0, void 0, function () {
      var cliOptions;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            cliOptions = __assign(__assign({}, command.optsWithGlobals()), {
              command: command.name(),
              version: packageJson.version,
            });
            return [
              4 /*yield*/,
              (0, prune_js_1.prune)({ cliOptions: cliOptions }),
            ];
          case 1:
            _a.sent();
            return [2 /*return*/];
        }
      });
    });
  });
var codegenCommand = new extra_typings_1.Command("codegen")
  .description("Generate the ponder-env.d.ts file, then exit")
  .showHelpAfterError()
  .action(function (_, command) {
    return __awaiter(void 0, void 0, void 0, function () {
      var cliOptions;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            cliOptions = __assign(__assign({}, command.optsWithGlobals()), {
              command: command.name(),
              version: packageJson.version,
            });
            return [
              4 /*yield*/,
              (0, codegen_js_1.codegen)({ cliOptions: cliOptions }),
            ];
          case 1:
            _a.sent();
            return [2 /*return*/];
        }
      });
    });
  });
dbCommand.addCommand(listCommand);
dbCommand.addCommand(pruneCommand);
dbCommand.addCommand(createViewsCommand);
ponder.addCommand(devCommand);
ponder.addCommand(startCommand);
ponder.addCommand(serveCommand);
ponder.addCommand(dbCommand);
ponder.addCommand(codegenCommand);
await ponder.parseAsync();
