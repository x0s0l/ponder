import type { Common } from "@/internal/common.js";
import { ShutdownError } from "@/internal/errors.js";
import type {
  BlockFilter,
  Factory,
  Filter,
  FilterWithoutBlocks,
  Fragment,
  LogFactory,
  LogFilter,
  Network,
  Source,
  SyncBlock,
  SyncLog,
  SyncTransactionReceipt,
  TraceFilter,
  TransactionFilter,
  TransferFilter,
} from "@/internal/types.js";
import type { SyncStore } from "@/sync-store/index.js";
import {
  getChildAddress,
  isAddressFactory,
  isAddressMatched,
  isBlockFilterMatched,
  isLogFactoryMatched,
  isLogFilterMatched,
  isTraceFilterMatched,
  isTransactionFilterMatched,
  isTransferFilterMatched,
} from "@/sync/filter.js";
import { shouldGetTransactionReceipt } from "@/sync/filter.js";
import { getFragments, recoverFilter } from "@/sync/fragments.js";
import { dedupe } from "@/utils/dedupe.js";
import {
  type Interval,
  getChunks,
  intervalBounds,
  intervalDifference,
  intervalRange,
} from "@/utils/interval.js";
import type { RequestQueue } from "@/utils/requestQueue.js";
import {
  _debug_traceBlockByNumber,
  _eth_getBlockByNumber,
  _eth_getBlockReceipts,
  _eth_getLogs,
  _eth_getTransactionReceipt,
} from "@/utils/rpc.js";
import { getLogsRetryHelper } from "@ponder/utils";
import {
  type Address,
  type Hash,
  type RpcError,
  hexToNumber,
  toHex,
  zeroHash,
} from "viem";

export type HistoricalSync = {
  intervalsCache: Map<Filter, { fragment: Fragment; intervals: Interval[] }[]>;
  calculateMissingIntervals(params: { interval: Interval }): Promise<
    IntervalWithFilter[]
  >;
  sync1(params: { missingIntervals: IntervalWithFilter[] }): Promise<{
    logs: Map<IntervalWithFilter, SyncLog[]>;
  }>;
  sync2(params: {
    interval: Interval;
    missingIntervals: IntervalWithFilter[];
    sync1Result: { logs: Map<IntervalWithFilter, SyncLog[]> };
  }): Promise<void>;
};

type IntervalWithFilter = {
  interval: Interval;
  filter: FilterWithoutBlocks;
};

type CreateHistoricalSyncParameters = {
  common: Common;
  sources: Source[];
  syncStore: SyncStore;
  network: Network;
  requestQueue: RequestQueue;
  onFatalError: (error: Error) => void;
};

export const createHistoricalSync = async (
  args: CreateHistoricalSyncParameters,
): Promise<HistoricalSync> => {
  /**
   * Flag to fetch transaction receipts through _eth_getBlockReceipts (true) or _eth_getTransactionReceipt (false)
   */
  let isBlockReceipts = true;
  /**
   * Data about the range passed to "eth_getLogs" share among all log
   * filters and log factories.
   */
  let logsRequestMetadata: {
    /** Estimate optimal range to use for "eth_getLogs" requests */
    estimatedRange: number;
    /** Range suggested by an error message */
    confirmedRange?: number;
  } = {
    estimatedRange: 500,
  };
  /**
   * Intervals that have been completed for all filters in `args.sources`.
   *
   * Note: `intervalsCache` is not updated after a new interval is synced.
   */
  let intervalsCache: Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >;
  if (args.network.disableCache) {
    intervalsCache = new Map();
    for (const { filter } of args.sources) {
      intervalsCache.set(filter, []);
      for (const { fragment } of getFragments(filter)) {
        intervalsCache.get(filter)!.push({ fragment, intervals: [] });
      }
    }
  } else {
    intervalsCache = await args.syncStore.getIntervals({
      filters: args.sources.map(({ filter }) => filter),
    });
  }

  ////////
  // Helper functions for sync tasks
  ////////

  /**
   * Split "eth_getLogs" requests into ranges inferred from errors
   * and batch requests.
   */
  const syncLogsDynamic = async ({
    filter,
    address,
    interval,
  }: {
    filter: LogFilter | LogFactory;
    interval: Interval;
    /** Explicitly set because of the complexity of factory contracts. */
    address: Address | Address[] | undefined;
  }): Promise<SyncLog[]> => {
    //  Use the recommended range if available, else don't chunk the interval at all.

    const intervals = getChunks({
      interval,
      maxChunkSize:
        logsRequestMetadata.confirmedRange ??
        logsRequestMetadata.estimatedRange,
    });

    const topics =
      "eventSelector" in filter
        ? [filter.eventSelector]
        : [
            filter.topic0 ?? null,
            filter.topic1 ?? null,
            filter.topic2 ?? null,
            filter.topic3 ?? null,
          ];

    // Note: the `topics` field is very fragile for many rpc providers, and
    // cannot handle extra "null" topics

    if (topics[3] === null) {
      topics.pop();
      if (topics[2] === null) {
        topics.pop();
        if (topics[1] === null) {
          topics.pop();
          if (topics[0] === null) {
            topics.pop();
          }
        }
      }
    }

    // Batch large arrays of addresses, handling arrays that are empty

    let addressBatches: (Address | Address[] | undefined)[];

    if (address === undefined) {
      // no address (match all)
      addressBatches = [undefined];
    } else if (typeof address === "string") {
      // single address
      addressBatches = [address];
    } else if (address.length === 0) {
      // no address (factory with no children)
      return [];
    } else {
      // many addresses
      // Note: it is assumed that `address` is deduplicated
      addressBatches = [];
      for (let i = 0; i < address.length; i += 50) {
        addressBatches.push(address.slice(i, i + 50));
      }
    }

    const logs = await Promise.all(
      intervals.flatMap((interval) =>
        addressBatches.map((address) =>
          _eth_getLogs(args.requestQueue, {
            address,
            topics,
            fromBlock: interval[0],
            toBlock: interval[1],
          }).catch((error) => {
            const getLogsErrorResponse = getLogsRetryHelper({
              params: [
                {
                  address,
                  topics,
                  fromBlock: toHex(interval[0]),
                  toBlock: toHex(interval[1]),
                },
              ],
              error: error as RpcError,
            });

            if (getLogsErrorResponse.shouldRetry === false) throw error;

            const range =
              hexToNumber(getLogsErrorResponse.ranges[0]!.toBlock) -
              hexToNumber(getLogsErrorResponse.ranges[0]!.fromBlock);

            args.common.logger.debug({
              service: "sync",
              msg: `Caught eth_getLogs error on '${
                args.network.name
              }', updating recommended range to ${range}.`,
            });

            logsRequestMetadata = {
              estimatedRange: range,
              confirmedRange: getLogsErrorResponse.isSuggestedRange
                ? range
                : undefined,
            };

            return syncLogsDynamic({ address, interval, filter });
          }),
        ),
      ),
    ).then((logs) => logs.flat());

    const logIds = new Set<string>();
    for (const log of logs) {
      const id = `${log.blockNumber}-${log.logIndex}`;
      if (logIds.has(id)) {
        args.common.logger.warn({
          service: "sync",
          msg: `Detected invalid eth_getLogs response. Duplicate log index ${log.logIndex} for block ${log.blockNumber}.`,
        });
      } else {
        logIds.add(id);
      }
    }

    /**
     * Dynamically increase the range used in "eth_getLogs" if an
     * error has been received but the error didn't suggest a range.
     */

    if (logsRequestMetadata.confirmedRange === undefined) {
      logsRequestMetadata.estimatedRange = Math.round(
        logsRequestMetadata.estimatedRange * 1.05,
      );
    }

    return logs;
  };

  const syncTransactionReceipts = async (
    block: SyncBlock,
    transactionHashes: Set<Hash>,
  ): Promise<SyncTransactionReceipt[]> => {
    if (transactionHashes.size === 0) {
      return [];
    }

    if (isBlockReceipts === false) {
      const transactionReceipts = await Promise.all(
        Array.from(transactionHashes).map((hash) =>
          _eth_getTransactionReceipt(args.requestQueue, { hash }),
        ),
      );

      return transactionReceipts;
    }

    let blockReceipts: SyncTransactionReceipt[];
    try {
      blockReceipts = await _eth_getBlockReceipts(args.requestQueue, {
        blockHash: block.hash,
      });
    } catch (_error) {
      const error = _error as Error;
      args.common.logger.warn({
        service: "sync",
        msg: `Caught eth_getBlockReceipts error on '${
          args.network.name
        }', switching to eth_getTransactionReceipt method.`,
        error,
      });

      isBlockReceipts = false;
      return syncTransactionReceipts(block, transactionHashes);
    }

    const blockReceiptsTransactionHashes = new Set(
      blockReceipts.map((r) => r.transactionHash),
    );
    // Validate that block transaction receipts include all required transactions
    for (const hash of Array.from(transactionHashes)) {
      if (blockReceiptsTransactionHashes.has(hash) === false) {
        throw new Error(
          `Detected inconsistent RPC responses. 'transaction.hash' ${hash} not found in eth_getBlockReceipts response for block '${block}'`,
        );
      }
    }
    const transactionReceipts = blockReceipts.filter((receipt) =>
      transactionHashes.has(receipt.transactionHash),
    );

    return transactionReceipts;
  };

  /** Extract and insert the log-based addresses that match `filter` + `interval`. */
  const syncLogFactory = async (factory: LogFactory, interval: Interval) => {
    const logs = await syncLogsDynamic({
      filter: factory,
      interval,
      address: factory.address,
    });

    const childAddresses = new Map<Address, number>();
    for (const log of logs) {
      if (isLogFactoryMatched({ factory, log })) {
        const address = getChildAddress({ log, factory });
        if (childAddresses.has(address) === false) {
          childAddresses.set(address, hexToNumber(log.blockNumber));
        }
      }
    }

    // Note: `factory` must refer to the same original `factory` in `filter`
    // and not be a recovered factory from `recoverFilter`.
    await args.syncStore.insertChildAddresses({
      factory,
      childAddresses,
      chainId: args.network.chainId,
    });
  };

  return {
    intervalsCache,
    async calculateMissingIntervals({ interval: _interval }) {
      const missingIntervals: IntervalWithFilter[] = [];

      // Determine the requests that need to be made, and which intervals need to be inserted.
      // Fragments are used to create a minimal filter, to avoid refetching data even if a filter
      // is only partially synced.

      for (const { filter } of args.sources) {
        if (
          (filter.fromBlock !== undefined && filter.fromBlock > _interval[1]) ||
          (filter.toBlock !== undefined && filter.toBlock < _interval[0])
        ) {
          continue;
        }

        const interval: Interval = [
          Math.max(filter.fromBlock ?? 0, _interval[0]),
          Math.min(filter.toBlock ?? Number.POSITIVE_INFINITY, _interval[1]),
        ];

        const completedIntervals = intervalsCache.get(filter)!;
        const requiredIntervals: {
          fragment: Fragment;
          intervals: Interval[];
        }[] = [];

        for (const {
          fragment,
          intervals: fragmentIntervals,
        } of completedIntervals) {
          const requiredFragmentIntervals = intervalDifference(
            [interval],
            fragmentIntervals,
          );

          if (requiredFragmentIntervals.length > 0) {
            requiredIntervals.push({
              fragment,
              intervals: requiredFragmentIntervals,
            });
          }
        }

        if (requiredIntervals.length > 0) {
          const requiredInterval = intervalBounds(
            requiredIntervals.flatMap(({ intervals }) => intervals),
          );

          const requiredFilter = recoverFilter(
            filter,
            requiredIntervals.map(({ fragment }) => fragment),
          );

          missingIntervals.push({
            filter: requiredFilter,
            interval: requiredInterval,
          });
        }
      }

      return missingIntervals;
    },
    async sync1({ missingIntervals }) {
      const logsResult: Map<IntervalWithFilter, SyncLog[]> = new Map();

      await Promise.all(
        missingIntervals.map(async ({ filter, interval }) => {
          try {
            switch (filter.type) {
              case "log": {
                let logs: SyncLog[];
                if (isAddressFactory(filter.address)) {
                  await syncLogFactory(filter.address, interval);
                  const childAddresses = await args.syncStore.getChildAddresses(
                    { factory: filter.address },
                  );
                  logs = await syncLogsDynamic({
                    filter: filter as LogFilter,
                    interval,
                    address:
                      childAddresses.size >=
                      args.common.options.factoryAddressCountThreshold
                        ? undefined
                        : Array.from(childAddresses.keys()),
                  });

                  logs = logs.filter((log) =>
                    isAddressMatched({
                      address: log.address,
                      blockNumber: hexToNumber(log.blockNumber),
                      childAddresses,
                    }),
                  );
                } else {
                  logs = await syncLogsDynamic({
                    filter: filter as LogFilter,
                    interval,
                    address: filter.address,
                  });
                }

                logsResult.set({ filter, interval }, logs);

                break;
              }

              case "transaction":
              case "trace":
              case "transfer": {
                await Promise.all([
                  isAddressFactory(filter.fromAddress)
                    ? syncLogFactory(filter.fromAddress, interval)
                    : Promise.resolve(),
                  isAddressFactory(filter.toAddress)
                    ? syncLogFactory(filter.toAddress, interval)
                    : Promise.resolve(),
                ]);

                break;
              }
            }
          } catch (_error) {
            const error = _error as Error;

            if (args.common.shutdown.isKilled) {
              throw new ShutdownError();
            }

            args.common.logger.error({
              service: "sync",
              msg: `Fatal error: Unable to sync '${args.network.name}' from ${interval[0]} to ${interval[1]}.`,
              error,
            });

            args.onFatalError(error);

            return;
          }
        }),
      );

      return { logs: logsResult };
    },
    async sync2({ interval, missingIntervals, sync1Result }) {
      if (missingIntervals.length === 0) return;

      const blockFilters: BlockFilter[] = [];
      const transactionFilters: TransactionFilter[] = [];
      const traceFilters: TraceFilter[] = [];
      const logFilters: LogFilter[] = [];
      const transferFilters: TransferFilter[] = [];
      const childAddresses: Map<Factory, Map<Address, number>> = new Map();

      for (const { filter } of missingIntervals) {
        switch (filter.type) {
          case "block": {
            blockFilters.push(filter as BlockFilter);
            break;
          }

          case "transaction": {
            transactionFilters.push(filter as TransactionFilter);
            break;
          }

          case "trace": {
            traceFilters.push(filter as TraceFilter);
            break;
          }

          case "log": {
            logFilters.push(filter as LogFilter);
            break;
          }

          case "transfer": {
            transferFilters.push(filter as TransferFilter);
            break;
          }
        }
      }

      // TODO(kyle) query child addresses

      const perBlockLogs: Map<number, SyncLog[]> = new Map();
      for (const [, logs] of sync1Result.logs) {
        for (const log of logs) {
          const number = hexToNumber(log.blockNumber);
          if (perBlockLogs.has(number) === false) {
            perBlockLogs.set(number, []);
          }
          perBlockLogs.get(number)!.push(log);
        }
      }

      for (const blockNumber of intervalRange(interval)) {
        const shouldRequestBlock =
          blockFilters.some((filter) =>
            isBlockFilterMatched({ filter, block }),
          ) ||
          transactionFilters.length > 0 ||
          perBlockLogs.has(blockNumber);
        const shouldRequestTraces =
          traceFilters.length > 0 || transferFilters.length > 0;

        if (shouldRequestBlock === false) continue;

        const block = await _eth_getBlockByNumber(args.requestQueue, {
          blockNumber,
        });

        const requiredTransactions = new Set<Hash>();
        const requiredTransactionReceipts = new Set<Hash>();

        if (perBlockLogs.has(blockNumber)) {
          let logs = perBlockLogs.get(blockNumber)!;

          logs = logs.filter((log) => {
            let isMatched = false;
            for (const filter of logFilters) {
              const isAddressMatched = isAddressFactory(filter.address);
              if (isLogFilterMatched({ filter, log }) && isAddressMatched) {
                requiredTransactions.add(log.transactionHash);
                isMatched = true;
                if (shouldGetTransactionReceipt(filter)) {
                  requiredTransactionReceipts.add(log.transactionHash);
                }
              }
            }
            return isMatched;
          });
        }

        if (shouldRequestTraces) {
          let traces = await _debug_traceBlockByNumber(args.requestQueue, {
            blockNumber,
          });

          traces = traces.filter((trace) => {
            let isMatched = false;

            for (const filter of traceFilters) {
              const isFromAddressMatched = isAddressFactory(filter.fromAddress)
                ? isAddressMatched({
                    address: trace.trace.from,
                    blockNumber: hexToNumber(block.number),
                    childAddresses: childAddresses.get(filter.fromAddress)!,
                  })
                : true;

              const isToAddressMatched = isAddressFactory(filter.toAddress)
                ? isAddressMatched({
                    address: trace.trace.to,
                    blockNumber: hexToNumber(block.number),
                    childAddresses: childAddresses.get(filter.toAddress)!,
                  })
                : true;

              if (
                isTraceFilterMatched({ filter, trace: trace.trace, block }) &&
                isFromAddressMatched &&
                isToAddressMatched
              ) {
                requiredTransactions.add(trace.transactionHash);
                isMatched = true;
                if (shouldGetTransactionReceipt(filter)) {
                  requiredTransactionReceipts.add(trace.transactionHash);
                }
              }
            }

            for (const filter of transferFilters) {
              const isFromAddressMatched = isAddressFactory(filter.fromAddress)
                ? isAddressMatched({
                    address: trace.trace.from,
                    blockNumber: hexToNumber(block.number),
                    childAddresses: childAddresses.get(filter.fromAddress)!,
                  })
                : true;

              const isToAddressMatched = isAddressFactory(filter.toAddress)
                ? isAddressMatched({
                    address: trace.trace.to,
                    blockNumber: hexToNumber(block.number),
                    childAddresses: childAddresses.get(filter.toAddress)!,
                  })
                : true;

              if (
                isTransferFilterMatched({
                  filter,
                  trace: trace.trace,
                  block,
                }) &&
                isFromAddressMatched &&
                isToAddressMatched
              ) {
                requiredTransactions.add(trace.transactionHash);
                isMatched = true;
                if (shouldGetTransactionReceipt(filter)) {
                  requiredTransactionReceipts.add(trace.transactionHash);
                }
              }
            }

            return isMatched;
          });
        }

        const transactions = block.transactions.filter((transaction) => {
          let isMatched = requiredTransactions.has(transaction.hash);

          for (const filter of transactionFilters) {
            const isFromAddressMatched = isAddressFactory(filter.fromAddress)
              ? isAddressMatched({
                  address: transaction.from,
                  blockNumber: hexToNumber(block.number),
                  childAddresses: childAddresses.get(filter.fromAddress)!,
                })
              : true;

            const isToAddressMatched = isAddressFactory(filter.toAddress)
              ? isAddressMatched({
                  address: transaction.to ?? undefined,
                  blockNumber: hexToNumber(block.number),
                  childAddresses: childAddresses.get(filter.toAddress)!,
                })
              : true;

            if (
              isTransactionFilterMatched({ filter, transaction }) &&
              isFromAddressMatched &&
              isToAddressMatched
            ) {
              requiredTransactions.add(transaction.hash);
              requiredTransactionReceipts.add(transaction.hash);
              isMatched = true;
            }
          }

          return isMatched;
        });

        const transactionReceipts = await syncTransactionReceipts(
          block,
          requiredTransactionReceipts,
        );
      }
    },
  };
};
