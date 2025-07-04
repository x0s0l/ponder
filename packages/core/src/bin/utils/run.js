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
var __asyncValues =
  (this && this.__asyncValues) ||
  function (o) {
    if (!Symbol.asyncIterator)
      throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator],
      i;
    return m
      ? m.call(o)
      : ((o =
          typeof __values === "function" ? __values(o) : o[Symbol.iterator]()),
        (i = {}),
        verb("next"),
        verb("throw"),
        verb("return"),
        (i[Symbol.asyncIterator] = function () {
          return this;
        }),
        i);
    function verb(n) {
      i[n] =
        o[n] &&
        function (v) {
          return new Promise(function (resolve, reject) {
            (v = o[n](v)), settle(resolve, reject, v.done, v.value);
          });
        };
    }
    function settle(resolve, reject, d, v) {
      Promise.resolve(v).then(function (v) {
        resolve({ value: v, done: d });
      }, reject);
    }
  };
Object.defineProperty(exports, "__esModule", { value: true });
exports.run = void 0;
var codegen_js_1 = require("@/bin/utils/codegen.js");
var cache_js_1 = require("@/indexing-store/cache.js");
var historical_js_1 = require("@/indexing-store/historical.js");
var realtime_js_1 = require("@/indexing-store/realtime.js");
var client_js_1 = require("@/indexing/client.js");
var index_js_1 = require("@/indexing/index.js");
var errors_js_1 = require("@/internal/errors.js");
var metrics_js_1 = require("@/internal/metrics.js");
var index_js_2 = require("@/sync-store/index.js");
var index_js_3 = require("@/sync/index.js");
var checkpoint_js_1 = require("@/utils/checkpoint.js");
var chunk_js_1 = require("@/utils/chunk.js");
var format_js_1 = require("@/utils/format.js");
var generators_js_1 = require("@/utils/generators.js");
var mutex_js_1 = require("@/utils/mutex.js");
var never_js_1 = require("@/utils/never.js");
var timer_js_1 = require("@/utils/timer.js");
var drizzle_orm_1 = require("drizzle-orm");
var pg_core_1 = require("drizzle-orm/pg-core");
/** Starts the sync and indexing services for the specified build. */
function run(_a) {
  var _b, e_1, _c, _d;
  var common = _a.common,
    preBuild = _a.preBuild,
    namespaceBuild = _a.namespaceBuild,
    schemaBuild = _a.schemaBuild,
    indexingBuild = _a.indexingBuild,
    crashRecoveryCheckpoint = _a.crashRecoveryCheckpoint,
    database = _a.database,
    onFatalError = _a.onFatalError,
    onReloadableError = _a.onReloadableError;
  return __awaiter(this, void 0, void 0, function () {
    var syncStore,
      sync,
      eventCount,
      _i,
      _e,
      eventName,
      cachedViemClient,
      indexing,
      indexingCache,
      _f,
      _g,
      chain,
      label,
      startTimestamp,
      _loop_1,
      _h,
      _j,
      _k,
      e_1_1,
      _l,
      _m,
      chain,
      label,
      endTimestamp,
      realtimeIndexingStore,
      onRealtimeEvent;
    var _this = this;
    return __generator(this, function (_o) {
      switch (_o.label) {
        case 0:
          return [4 /*yield*/, database.migrateSync()];
        case 1:
          _o.sent();
          (0, codegen_js_1.runCodegen)({ common: common });
          syncStore = (0, index_js_2.createSyncStore)({
            common: common,
            database: database,
          });
          return [
            4 /*yield*/,
            (0, index_js_3.createSync)({
              common: common,
              indexingBuild: indexingBuild,
              syncStore: syncStore,
              onRealtimeEvent: function (realtimeEvent) {
                return onRealtimeEvent(realtimeEvent);
              },
              onFatalError: onFatalError,
              crashRecoveryCheckpoint: crashRecoveryCheckpoint,
              ordering: preBuild.ordering,
            }),
          ];
        case 2:
          sync = _o.sent();
          eventCount = {};
          for (
            _i = 0, _e = Object.keys(indexingBuild.indexingFunctions);
            _i < _e.length;
            _i++
          ) {
            eventName = _e[_i];
            eventCount[eventName] = 0;
          }
          cachedViemClient = (0, client_js_1.createCachedViemClient)({
            common: common,
            indexingBuild: indexingBuild,
            syncStore: syncStore,
            eventCount: eventCount,
          });
          indexing = (0, index_js_1.createIndexing)({
            common: common,
            indexingBuild: indexingBuild,
            client: cachedViemClient,
            eventCount: eventCount,
          });
          indexingCache = (0, cache_js_1.createIndexingCache)({
            common: common,
            schemaBuild: schemaBuild,
            crashRecoveryCheckpoint: crashRecoveryCheckpoint,
            eventCount: eventCount,
          });
          for (_f = 0, _g = indexingBuild.chains; _f < _g.length; _f++) {
            chain = _g[_f];
            label = { chain: chain.name };
            common.metrics.ponder_historical_total_indexing_seconds.set(
              label,
              Math.max(
                sync.seconds[chain.name].end - sync.seconds[chain.name].start,
                0,
              ),
            );
            common.metrics.ponder_historical_cached_indexing_seconds.set(
              label,
              Math.max(
                sync.seconds[chain.name].cached -
                  sync.seconds[chain.name].start,
                0,
              ),
            );
            common.metrics.ponder_historical_completed_indexing_seconds.set(
              label,
              0,
            );
            common.metrics.ponder_indexing_timestamp.set(
              label,
              Math.max(
                sync.seconds[chain.name].cached,
                sync.seconds[chain.name].start,
              ),
            );
          }
          startTimestamp = Math.round(Date.now() / 1000);
          common.metrics.ponder_historical_start_timestamp_seconds.set(
            startTimestamp,
          );
          // Reset the start timestamp so the eta estimate doesn't include
          // the startup time.
          common.metrics.start_timestamp = Date.now();
          if (!(crashRecoveryCheckpoint === undefined)) return [3 /*break*/, 5];
          return [
            4 /*yield*/,
            database.retry(function () {
              return __awaiter(_this, void 0, void 0, function () {
                var _this = this;
                return __generator(this, function (_a) {
                  switch (_a.label) {
                    case 0:
                      return [
                        4 /*yield*/,
                        database.transaction(function (client, tx) {
                          return __awaiter(_this, void 0, void 0, function () {
                            var historicalIndexingStore, result, error_1;
                            return __generator(this, function (_a) {
                              switch (_a.label) {
                                case 0:
                                  historicalIndexingStore = (0,
                                  historical_js_1.createHistoricalIndexingStore)(
                                    {
                                      common: common,
                                      schemaBuild: schemaBuild,
                                      indexingCache: indexingCache,
                                      db: tx,
                                      client: client,
                                    },
                                  );
                                  return [
                                    4 /*yield*/,
                                    indexing.processSetupEvents({
                                      db: historicalIndexingStore,
                                    }),
                                  ];
                                case 1:
                                  result = _a.sent();
                                  if (result.status === "error") {
                                    onReloadableError(result.error);
                                    return [2 /*return*/];
                                  }
                                  _a.label = 2;
                                case 2:
                                  _a.trys.push([2, 4, , 5]);
                                  return [
                                    4 /*yield*/,
                                    indexingCache.flush({ client: client }),
                                  ];
                                case 3:
                                  _a.sent();
                                  return [3 /*break*/, 5];
                                case 4:
                                  error_1 = _a.sent();
                                  if (
                                    error_1 instanceof errors_js_1.FlushError
                                  ) {
                                    onReloadableError(error_1);
                                    return [2 /*return*/];
                                  }
                                  throw error_1;
                                case 5:
                                  return [2 /*return*/];
                              }
                            });
                          });
                        }),
                      ];
                    case 1:
                      _a.sent();
                      return [2 /*return*/];
                  }
                });
              });
            }),
          ];
        case 3:
          _o.sent();
          // Note: `_ponder_checkpoint` must be updated after the setup events are processed.
          return [
            4 /*yield*/,
            database.setCheckpoints({
              checkpoints: indexingBuild.chains.map(function (chain) {
                return {
                  chainName: chain.name,
                  chainId: chain.id,
                  latestCheckpoint: sync.getStartCheckpoint(chain),
                  safeCheckpoint: sync.getStartCheckpoint(chain),
                };
              }),
              db: database.qb.drizzle,
            }),
          ];
        case 4:
          // Note: `_ponder_checkpoint` must be updated after the setup events are processed.
          _o.sent();
          _o.label = 5;
        case 5:
          _o.trys.push([5, 11, 12, 17]);
          _loop_1 = function () {
            var events, endClock;
            return __generator(this, function (_p) {
              switch (_p.label) {
                case 0:
                  _d = _k.value;
                  _h = false;
                  _p.label = 1;
                case 1:
                  _p.trys.push([1, , 6, 7]);
                  events = _d;
                  endClock = (0, timer_js_1.startClock)();
                  return [
                    4 /*yield*/,
                    Promise.all([
                      indexingCache.prefetch({
                        events: events.events,
                        db: database.qb.drizzle,
                      }),
                      cachedViemClient.prefetch({
                        events: events.events,
                      }),
                    ]),
                  ];
                case 2:
                  _p.sent();
                  common.metrics.ponder_historical_transform_duration.inc(
                    { step: "prefetch" },
                    endClock(),
                  );
                  if (!(events.events.length > 0)) return [3 /*break*/, 5];
                  endClock = (0, timer_js_1.startClock)();
                  return [
                    4 /*yield*/,
                    database.retry(function () {
                      return __awaiter(_this, void 0, void 0, function () {
                        var _this = this;
                        return __generator(this, function (_a) {
                          switch (_a.label) {
                            case 0:
                              return [
                                4 /*yield*/,
                                database
                                  .transaction(function (client, tx) {
                                    return __awaiter(
                                      _this,
                                      void 0,
                                      void 0,
                                      function () {
                                        var historicalIndexingStore,
                                          eventChunks,
                                          _loop_2,
                                          _i,
                                          eventChunks_1,
                                          eventChunk,
                                          state_1,
                                          _a,
                                          eta,
                                          progress,
                                          error_2;
                                        return __generator(this, function (_b) {
                                          switch (_b.label) {
                                            case 0:
                                              common.metrics.ponder_historical_transform_duration.inc(
                                                { step: "begin" },
                                                endClock(),
                                              );
                                              endClock = (0,
                                              timer_js_1.startClock)();
                                              historicalIndexingStore = (0,
                                              historical_js_1.createHistoricalIndexingStore)(
                                                {
                                                  common: common,
                                                  schemaBuild: schemaBuild,
                                                  indexingCache: indexingCache,
                                                  db: tx,
                                                  client: client,
                                                },
                                              );
                                              eventChunks = (0,
                                              chunk_js_1.chunk)(
                                                events.events,
                                                93,
                                              );
                                              _loop_2 = function (eventChunk) {
                                                var result,
                                                  checkpoint,
                                                  chain,
                                                  _c,
                                                  _d,
                                                  chain;
                                                return __generator(
                                                  this,
                                                  function (_e) {
                                                    switch (_e.label) {
                                                      case 0:
                                                        return [
                                                          4 /*yield*/,
                                                          indexing.processEvents(
                                                            {
                                                              events:
                                                                eventChunk,
                                                              db: historicalIndexingStore,
                                                              cache:
                                                                indexingCache,
                                                            },
                                                          ),
                                                        ];
                                                      case 1:
                                                        result = _e.sent();
                                                        if (
                                                          result.status ===
                                                          "error"
                                                        ) {
                                                          onReloadableError(
                                                            result.error,
                                                          );
                                                          return [
                                                            2 /*return*/,
                                                            { value: void 0 },
                                                          ];
                                                        }
                                                        checkpoint = (0,
                                                        checkpoint_js_1.decodeCheckpoint)(
                                                          eventChunk[
                                                            eventChunk.length -
                                                              1
                                                          ].checkpoint,
                                                        );
                                                        if (
                                                          preBuild.ordering ===
                                                          "multichain"
                                                        ) {
                                                          chain =
                                                            indexingBuild.chains.find(
                                                              function (chain) {
                                                                return (
                                                                  chain.id ===
                                                                  Number(
                                                                    checkpoint.chainId,
                                                                  )
                                                                );
                                                              },
                                                            );
                                                          common.metrics.ponder_historical_completed_indexing_seconds.set(
                                                            {
                                                              chain: chain.name,
                                                            },
                                                            Math.max(
                                                              Number(
                                                                checkpoint.blockTimestamp,
                                                              ) -
                                                                Math.max(
                                                                  sync.seconds[
                                                                    chain.name
                                                                  ].cached,
                                                                  sync.seconds[
                                                                    chain.name
                                                                  ].start,
                                                                ),
                                                              0,
                                                            ),
                                                          );
                                                          common.metrics.ponder_indexing_timestamp.set(
                                                            {
                                                              chain: chain.name,
                                                            },
                                                            Number(
                                                              checkpoint.blockTimestamp,
                                                            ),
                                                          );
                                                        } else {
                                                          for (
                                                            _c = 0,
                                                              _d =
                                                                indexingBuild.chains;
                                                            _c < _d.length;
                                                            _c++
                                                          ) {
                                                            chain = _d[_c];
                                                            common.metrics.ponder_historical_completed_indexing_seconds.set(
                                                              {
                                                                chain:
                                                                  chain.name,
                                                              },
                                                              Math.min(
                                                                Math.max(
                                                                  Number(
                                                                    checkpoint.blockTimestamp,
                                                                  ) -
                                                                    Math.max(
                                                                      sync
                                                                        .seconds[
                                                                        chain
                                                                          .name
                                                                      ].cached,
                                                                      sync
                                                                        .seconds[
                                                                        chain
                                                                          .name
                                                                      ].start,
                                                                    ),
                                                                  0,
                                                                ),
                                                                Math.max(
                                                                  sync.seconds[
                                                                    chain.name
                                                                  ].end -
                                                                    sync
                                                                      .seconds[
                                                                      chain.name
                                                                    ].start,
                                                                  0,
                                                                ),
                                                              ),
                                                            );
                                                            common.metrics.ponder_indexing_timestamp.set(
                                                              {
                                                                chain:
                                                                  chain.name,
                                                              },
                                                              Math.max(
                                                                Number(
                                                                  checkpoint.blockTimestamp,
                                                                ),
                                                                sync.seconds[
                                                                  chain.name
                                                                ].end,
                                                              ),
                                                            );
                                                          }
                                                        }
                                                        if (
                                                          !(
                                                            preBuild
                                                              .databaseConfig
                                                              .kind === "pglite"
                                                          )
                                                        )
                                                          return [
                                                            3 /*break*/, 3,
                                                          ];
                                                        return [
                                                          4 /*yield*/,
                                                          new Promise(
                                                            setImmediate,
                                                          ),
                                                        ];
                                                      case 2:
                                                        _e.sent();
                                                        _e.label = 3;
                                                      case 3:
                                                        return [2 /*return*/];
                                                    }
                                                  },
                                                );
                                              };
                                              (_i = 0),
                                                (eventChunks_1 = eventChunks);
                                              _b.label = 1;
                                            case 1:
                                              if (!(_i < eventChunks_1.length))
                                                return [3 /*break*/, 4];
                                              eventChunk = eventChunks_1[_i];
                                              return [
                                                5 /*yield**/,
                                                _loop_2(eventChunk),
                                              ];
                                            case 2:
                                              state_1 = _b.sent();
                                              if (typeof state_1 === "object")
                                                return [
                                                  2 /*return*/,
                                                  state_1.value,
                                                ];
                                              _b.label = 3;
                                            case 3:
                                              _i++;
                                              return [3 /*break*/, 1];
                                            case 4:
                                              return [
                                                4 /*yield*/,
                                                new Promise(setImmediate),
                                              ];
                                            case 5:
                                              _b.sent();
                                              return [
                                                4 /*yield*/,
                                                (0,
                                                metrics_js_1.getAppProgress)(
                                                  common.metrics,
                                                ),
                                              ];
                                            case 6:
                                              (_a = _b.sent()),
                                                (eta = _a.eta),
                                                (progress = _a.progress);
                                              if (
                                                eta === undefined ||
                                                progress === undefined
                                              ) {
                                                common.logger.info({
                                                  service: "app",
                                                  msg: "Indexed ".concat(
                                                    events.events.length,
                                                    " events",
                                                  ),
                                                });
                                              } else {
                                                common.logger.info({
                                                  service: "app",
                                                  msg: "Indexed "
                                                    .concat(
                                                      events.events.length,
                                                      " events with ",
                                                    )
                                                    .concat(
                                                      (0,
                                                      format_js_1.formatPercentage)(
                                                        progress,
                                                      ),
                                                      " complete and ",
                                                    )
                                                    .concat(
                                                      (0,
                                                      format_js_1.formatEta)(
                                                        eta * 1000,
                                                      ),
                                                      " remaining",
                                                    ),
                                                });
                                              }
                                              common.metrics.ponder_historical_transform_duration.inc(
                                                { step: "index" },
                                                endClock(),
                                              );
                                              endClock = (0,
                                              timer_js_1.startClock)();
                                              _b.label = 7;
                                            case 7:
                                              _b.trys.push([7, 9, , 10]);
                                              return [
                                                4 /*yield*/,
                                                indexingCache.flush({
                                                  client: client,
                                                }),
                                              ];
                                            case 8:
                                              _b.sent();
                                              return [3 /*break*/, 10];
                                            case 9:
                                              error_2 = _b.sent();
                                              if (
                                                error_2 instanceof
                                                errors_js_1.FlushError
                                              ) {
                                                onReloadableError(error_2);
                                                return [2 /*return*/];
                                              }
                                              throw error_2;
                                            case 10:
                                              common.metrics.ponder_historical_transform_duration.inc(
                                                { step: "load" },
                                                endClock(),
                                              );
                                              endClock = (0,
                                              timer_js_1.startClock)();
                                              return [
                                                4 /*yield*/,
                                                database.setCheckpoints({
                                                  checkpoints:
                                                    events.checkpoints.map(
                                                      function (_a) {
                                                        var chainId =
                                                            _a.chainId,
                                                          checkpoint =
                                                            _a.checkpoint;
                                                        return {
                                                          chainName:
                                                            indexingBuild.chains.find(
                                                              function (chain) {
                                                                return (
                                                                  chain.id ===
                                                                  chainId
                                                                );
                                                              },
                                                            ).name,
                                                          chainId: chainId,
                                                          latestCheckpoint:
                                                            checkpoint,
                                                          safeCheckpoint:
                                                            checkpoint,
                                                        };
                                                      },
                                                    ),
                                                  db: tx,
                                                }),
                                              ];
                                            case 11:
                                              _b.sent();
                                              common.metrics.ponder_historical_transform_duration.inc(
                                                { step: "finalize" },
                                                endClock(),
                                              );
                                              endClock = (0,
                                              timer_js_1.startClock)();
                                              return [2 /*return*/];
                                          }
                                        });
                                      },
                                    );
                                  })
                                  .catch(function (error) {
                                    indexingCache.rollback();
                                    throw error;
                                  }),
                              ];
                            case 1:
                              _a.sent();
                              return [2 /*return*/];
                          }
                        });
                      });
                    }),
                  ];
                case 3:
                  _p.sent();
                  cachedViemClient.clear();
                  common.metrics.ponder_historical_transform_duration.inc(
                    { step: "commit" },
                    endClock(),
                  );
                  return [4 /*yield*/, new Promise(setImmediate)];
                case 4:
                  _p.sent();
                  _p.label = 5;
                case 5:
                  return [3 /*break*/, 7];
                case 6:
                  _h = true;
                  return [7 /*endfinally*/];
                case 7:
                  return [2 /*return*/];
              }
            });
          };
          (_h = true),
            (_j = __asyncValues(
              (0, generators_js_1.recordAsyncGenerator)(
                sync.getEvents(),
                function (params) {
                  common.metrics.ponder_historical_concurrency_group_duration.inc(
                    { group: "extract" },
                    params.await,
                  );
                  common.metrics.ponder_historical_concurrency_group_duration.inc(
                    { group: "transform" },
                    params.yield,
                  );
                },
              ),
            ));
          _o.label = 6;
        case 6:
          return [4 /*yield*/, _j.next()];
        case 7:
          if (!((_k = _o.sent()), (_b = _k.done), !_b))
            return [3 /*break*/, 10];
          return [5 /*yield**/, _loop_1()];
        case 8:
          _o.sent();
          _o.label = 9;
        case 9:
          return [3 /*break*/, 6];
        case 10:
          return [3 /*break*/, 17];
        case 11:
          e_1_1 = _o.sent();
          e_1 = { error: e_1_1 };
          return [3 /*break*/, 17];
        case 12:
          _o.trys.push([12, , 15, 16]);
          if (!(!_h && !_b && (_c = _j.return))) return [3 /*break*/, 14];
          return [4 /*yield*/, _c.call(_j)];
        case 13:
          _o.sent();
          _o.label = 14;
        case 14:
          return [3 /*break*/, 16];
        case 15:
          if (e_1) throw e_1.error;
          return [7 /*endfinally*/];
        case 16:
          return [7 /*endfinally*/];
        case 17:
          indexingCache.clear();
          // Manually update metrics to fix a UI bug that occurs when the end
          // checkpoint is between the last processed event and the finalized
          // checkpoint.
          for (_l = 0, _m = indexingBuild.chains; _l < _m.length; _l++) {
            chain = _m[_l];
            label = { chain: chain.name };
            common.metrics.ponder_historical_completed_indexing_seconds.set(
              label,
              Math.max(
                sync.seconds[chain.name].end -
                  Math.max(
                    sync.seconds[chain.name].cached,
                    sync.seconds[chain.name].start,
                  ),
                0,
              ),
            );
            common.metrics.ponder_indexing_timestamp.set(
              { chain: chain.name },
              sync.seconds[chain.name].end,
            );
          }
          endTimestamp = Math.round(Date.now() / 1000);
          common.metrics.ponder_historical_end_timestamp_seconds.set(
            endTimestamp,
          );
          common.logger.info({
            service: "indexing",
            msg: "Completed historical indexing",
          });
          return [4 /*yield*/, database.createIndexes()];
        case 18:
          _o.sent();
          return [4 /*yield*/, database.createTriggers()];
        case 19:
          _o.sent();
          if (!namespaceBuild.viewsSchema) return [3 /*break*/, 21];
          return [
            4 /*yield*/,
            database.wrap({ method: "create-views" }, function () {
              return __awaiter(_this, void 0, void 0, function () {
                var tables, _i, tables_1, table, trigger, notification, channel;
                return __generator(this, function (_a) {
                  switch (_a.label) {
                    case 0:
                      return [
                        4 /*yield*/,
                        database.qb.drizzle.execute(
                          drizzle_orm_1.sql.raw(
                            'CREATE SCHEMA IF NOT EXISTS "'.concat(
                              namespaceBuild.viewsSchema,
                              '"',
                            ),
                          ),
                        ),
                      ];
                    case 1:
                      _a.sent();
                      tables = Object.values(schemaBuild.schema).filter(
                        function (table) {
                          return (0, drizzle_orm_1.is)(
                            table,
                            pg_core_1.PgTable,
                          );
                        },
                      );
                      (_i = 0), (tables_1 = tables);
                      _a.label = 2;
                    case 2:
                      if (!(_i < tables_1.length)) return [3 /*break*/, 6];
                      table = tables_1[_i];
                      // Note: drop views before creating new ones to avoid enum errors.
                      return [
                        4 /*yield*/,
                        database.qb.drizzle.execute(
                          drizzle_orm_1.sql.raw(
                            'DROP VIEW IF EXISTS "'
                              .concat(namespaceBuild.viewsSchema, '"."')
                              .concat(
                                (0, drizzle_orm_1.getTableName)(table),
                                '"',
                              ),
                          ),
                        ),
                      ];
                    case 3:
                      // Note: drop views before creating new ones to avoid enum errors.
                      _a.sent();
                      return [
                        4 /*yield*/,
                        database.qb.drizzle.execute(
                          drizzle_orm_1.sql.raw(
                            'CREATE VIEW "'
                              .concat(namespaceBuild.viewsSchema, '"."')
                              .concat(
                                (0, drizzle_orm_1.getTableName)(table),
                                '" AS SELECT * FROM "',
                              )
                              .concat(namespaceBuild.schema, '"."')
                              .concat(
                                (0, drizzle_orm_1.getTableName)(table),
                                '"',
                              ),
                          ),
                        ),
                      ];
                    case 4:
                      _a.sent();
                      _a.label = 5;
                    case 5:
                      _i++;
                      return [3 /*break*/, 2];
                    case 6:
                      common.logger.info({
                        service: "app",
                        msg: "Created "
                          .concat(tables.length, ' views in schema "')
                          .concat(namespaceBuild.viewsSchema, '"'),
                      });
                      return [
                        4 /*yield*/,
                        database.qb.drizzle.execute(
                          drizzle_orm_1.sql.raw(
                            'CREATE OR REPLACE VIEW "'
                              .concat(
                                namespaceBuild.viewsSchema,
                                '"."_ponder_meta" AS SELECT * FROM "',
                              )
                              .concat(
                                namespaceBuild.schema,
                                '"."_ponder_meta"',
                              ),
                          ),
                        ),
                      ];
                    case 7:
                      _a.sent();
                      return [
                        4 /*yield*/,
                        database.qb.drizzle.execute(
                          drizzle_orm_1.sql.raw(
                            'CREATE OR REPLACE VIEW "'
                              .concat(
                                namespaceBuild.viewsSchema,
                                '"."_ponder_checkpoint" AS SELECT * FROM "',
                              )
                              .concat(
                                namespaceBuild.schema,
                                '"."_ponder_checkpoint"',
                              ),
                          ),
                        ),
                      ];
                    case 8:
                      _a.sent();
                      trigger = "status_".concat(
                        namespaceBuild.viewsSchema,
                        "_trigger",
                      );
                      notification = "status_notify()";
                      channel = "".concat(
                        namespaceBuild.viewsSchema,
                        "_status_channel",
                      );
                      return [
                        4 /*yield*/,
                        database.qb.drizzle.execute(
                          drizzle_orm_1.sql.raw(
                            '\n  CREATE OR REPLACE FUNCTION "'
                              .concat(namespaceBuild.viewsSchema, '".')
                              .concat(
                                notification,
                                '\n  RETURNS TRIGGER\n  LANGUAGE plpgsql\n  AS $$\n  BEGIN\n  NOTIFY "',
                              )
                              .concat(
                                channel,
                                '";\n  RETURN NULL;\n  END;\n  $$;',
                              ),
                          ),
                        ),
                      ];
                    case 9:
                      _a.sent();
                      return [
                        4 /*yield*/,
                        database.qb.drizzle.execute(
                          drizzle_orm_1.sql.raw(
                            '\n  CREATE OR REPLACE TRIGGER "'
                              .concat(
                                trigger,
                                '"\n  AFTER INSERT OR UPDATE OR DELETE\n  ON "',
                              )
                              .concat(
                                namespaceBuild.schema,
                                '"._ponder_checkpoint\n  FOR EACH STATEMENT\n  EXECUTE PROCEDURE "',
                              )
                              .concat(namespaceBuild.viewsSchema, '".')
                              .concat(notification, ";"),
                          ),
                        ),
                      ];
                    case 10:
                      _a.sent();
                      return [2 /*return*/];
                  }
                });
              });
            }),
          ];
        case 20:
          _o.sent();
          _o.label = 21;
        case 21:
          return [4 /*yield*/, database.setReady()];
        case 22:
          _o.sent();
          realtimeIndexingStore = (0,
          realtime_js_1.createRealtimeIndexingStore)({
            common: common,
            schemaBuild: schemaBuild,
            database: database,
          });
          onRealtimeEvent = (0, mutex_js_1.mutex)(function (event) {
            return __awaiter(_this, void 0, void 0, function () {
              var _a,
                perBlockEvents,
                _loop_3,
                _i,
                perBlockEvents_1,
                _b,
                checkpoint,
                events;
              var _this = this;
              return __generator(this, function (_c) {
                switch (_c.label) {
                  case 0:
                    _a = event.type;
                    switch (_a) {
                      case "block":
                        return [3 /*break*/, 1];
                      case "reorg":
                        return [3 /*break*/, 7];
                      case "finalize":
                        return [3 /*break*/, 11];
                    }
                    return [3 /*break*/, 14];
                  case 1:
                    if (!(event.events.length > 0)) return [3 /*break*/, 5];
                    perBlockEvents = (0, index_js_3.splitEvents)(event.events);
                    common.logger.debug({
                      service: "app",
                      msg: "Partitioned events into ".concat(
                        perBlockEvents.length,
                        " blocks",
                      ),
                    });
                    _loop_3 = function (checkpoint, events) {
                      var chain, result, _d, _e, chain_1;
                      return __generator(this, function (_f) {
                        switch (_f.label) {
                          case 0:
                            chain = indexingBuild.chains.find(function (chain) {
                              return (
                                chain.id ===
                                Number(
                                  (0, checkpoint_js_1.decodeCheckpoint)(
                                    checkpoint,
                                  ).chainId,
                                )
                              );
                            });
                            return [
                              4 /*yield*/,
                              indexing.processEvents({
                                events: events,
                                db: realtimeIndexingStore,
                              }),
                            ];
                          case 1:
                            result = _f.sent();
                            common.logger.info({
                              service: "app",
                              msg: "Indexed "
                                .concat(events.length, " '")
                                .concat(chain.name, "' events for block ")
                                .concat(
                                  Number(
                                    (0, checkpoint_js_1.decodeCheckpoint)(
                                      checkpoint,
                                    ).blockNumber,
                                  ),
                                ),
                            });
                            if (result.status === "error")
                              onReloadableError(result.error);
                            return [
                              4 /*yield*/,
                              database.commitBlock({
                                checkpoint: checkpoint,
                                db: database.qb.drizzle,
                              }),
                            ];
                          case 2:
                            _f.sent();
                            if (preBuild.ordering === "multichain") {
                              common.metrics.ponder_indexing_timestamp.set(
                                { chain: chain.name },
                                Number(
                                  (0, checkpoint_js_1.decodeCheckpoint)(
                                    checkpoint,
                                  ).blockTimestamp,
                                ),
                              );
                            } else {
                              for (
                                _d = 0, _e = indexingBuild.chains;
                                _d < _e.length;
                                _d++
                              ) {
                                chain_1 = _e[_d];
                                common.metrics.ponder_indexing_timestamp.set(
                                  { chain: chain_1.name },
                                  Number(
                                    (0, checkpoint_js_1.decodeCheckpoint)(
                                      checkpoint,
                                    ).blockTimestamp,
                                  ),
                                );
                              }
                            }
                            return [2 /*return*/];
                        }
                      });
                    };
                    (_i = 0), (perBlockEvents_1 = perBlockEvents);
                    _c.label = 2;
                  case 2:
                    if (!(_i < perBlockEvents_1.length))
                      return [3 /*break*/, 5];
                    (_b = perBlockEvents_1[_i]),
                      (checkpoint = _b.checkpoint),
                      (events = _b.events);
                    return [5 /*yield**/, _loop_3(checkpoint, events)];
                  case 3:
                    _c.sent();
                    _c.label = 4;
                  case 4:
                    _i++;
                    return [3 /*break*/, 2];
                  case 5:
                    return [
                      4 /*yield*/,
                      database.wrap({ method: "setCheckpoints" }, function () {
                        return __awaiter(_this, void 0, void 0, function () {
                          return __generator(this, function (_a) {
                            switch (_a.label) {
                              case 0:
                                if (event.checkpoints.length === 0)
                                  return [2 /*return*/];
                                return [
                                  4 /*yield*/,
                                  database.qb.drizzle
                                    .insert(database.PONDER_CHECKPOINT)
                                    .values(
                                      event.checkpoints.map(function (_a) {
                                        var chainId = _a.chainId,
                                          checkpoint = _a.checkpoint;
                                        return {
                                          chainName: indexingBuild.chains.find(
                                            function (chain) {
                                              return chain.id === chainId;
                                            },
                                          ).name,
                                          chainId: chainId,
                                          safeCheckpoint: checkpoint,
                                          latestCheckpoint: checkpoint,
                                        };
                                      }),
                                    )
                                    .onConflictDoUpdate({
                                      target:
                                        database.PONDER_CHECKPOINT.chainName,
                                      set: {
                                        latestCheckpoint: (0,
                                        drizzle_orm_1.sql)(
                                          templateObject_1 ||
                                            (templateObject_1 =
                                              __makeTemplateObject(
                                                ["excluded.latest_checkpoint"],
                                                ["excluded.latest_checkpoint"],
                                              )),
                                        ),
                                      },
                                    }),
                                ];
                              case 1:
                                _a.sent();
                                return [2 /*return*/];
                            }
                          });
                        });
                      }),
                    ];
                  case 6:
                    _c.sent();
                    return [3 /*break*/, 15];
                  case 7:
                    // Note: `_ponder_checkpoint` is not called here, instead it is called
                    // in the `block` case.
                    return [4 /*yield*/, database.removeTriggers()];
                  case 8:
                    // Note: `_ponder_checkpoint` is not called here, instead it is called
                    // in the `block` case.
                    _c.sent();
                    return [
                      4 /*yield*/,
                      database.retry(function () {
                        return __awaiter(_this, void 0, void 0, function () {
                          var _this = this;
                          return __generator(this, function (_a) {
                            switch (_a.label) {
                              case 0:
                                return [
                                  4 /*yield*/,
                                  database.qb.drizzle.transaction(
                                    function (tx) {
                                      return __awaiter(
                                        _this,
                                        void 0,
                                        void 0,
                                        function () {
                                          return __generator(
                                            this,
                                            function (_a) {
                                              switch (_a.label) {
                                                case 0:
                                                  return [
                                                    4 /*yield*/,
                                                    database.revert({
                                                      checkpoint:
                                                        event.checkpoint,
                                                      tx: tx,
                                                    }),
                                                  ];
                                                case 1:
                                                  _a.sent();
                                                  return [2 /*return*/];
                                              }
                                            },
                                          );
                                        },
                                      );
                                    },
                                  ),
                                ];
                              case 1:
                                _a.sent();
                                return [2 /*return*/];
                            }
                          });
                        });
                      }),
                    ];
                  case 9:
                    _c.sent();
                    return [4 /*yield*/, database.createTriggers()];
                  case 10:
                    _c.sent();
                    return [3 /*break*/, 15];
                  case 11:
                    return [
                      4 /*yield*/,
                      database.qb.drizzle
                        .update(database.PONDER_CHECKPOINT)
                        .set({
                          safeCheckpoint: event.checkpoint,
                        }),
                    ];
                  case 12:
                    _c.sent();
                    return [
                      4 /*yield*/,
                      database.finalize({
                        checkpoint: event.checkpoint,
                        db: database.qb.drizzle,
                      }),
                    ];
                  case 13:
                    _c.sent();
                    return [3 /*break*/, 15];
                  case 14:
                    (0, never_js_1.never)(event);
                    _c.label = 15;
                  case 15:
                    return [2 /*return*/];
                }
              });
            });
          });
          return [4 /*yield*/, sync.startRealtime()];
        case 23:
          _o.sent();
          common.logger.info({
            service: "server",
            msg: "Started returning 200 responses from /ready endpoint",
          });
          return [2 /*return*/];
      }
    });
  });
}
exports.run = run;
var templateObject_1;
