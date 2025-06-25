import type { Common } from "@/internal/common.js";
import { ShutdownError } from "@/internal/errors.js";
import type {
  BlockFilter,
  Chain,
  Factory,
  Filter,
  FilterWithoutBlocks,
  Fragment,
  LogFactory,
  LogFilter,
  Source,
  SyncBlock,
  SyncLog,
  SyncTrace,
  SyncTransaction,
  SyncTransactionReceipt,
  TraceFilter,
  TransactionFilter,
  TransferFilter,
} from "@/internal/types.js";
import type { Rpc } from "@/rpc/index.js";
import type { SyncStore } from "@/sync-store/index.js";
import {
  getChildAddress,
  isAddressFactory,
  isAddressMatched,
  isBlockFilterMatched,
  isLogFactoryMatched,
  isTraceFilterMatched,
  isTransactionFilterMatched,
  isTransferFilterMatched,
} from "@/sync/filter.js";
import { shouldGetTransactionReceipt } from "@/sync/filter.js";
import { getFragments, recoverFilter } from "@/sync/fragments.js";
import {
  type Interval,
  getChunks,
  intervalBounds,
  intervalDifference,
  intervalRange,
  intervalUnion,
} from "@/utils/interval.js";
import { createQueue } from "@/utils/queue.js";
import {
  _debug_traceBlockByNumber,
  _eth_getBlockByNumber,
  _eth_getBlockReceipts,
  _eth_getLogs,
  _eth_getTransactionReceipt,
  // validateLogsAndBlock,
  validateReceiptsAndBlock,
  validateTracesAndBlock,
  validateTransactionsAndBlock,
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
  calculateMissingIntervals(params: {
    interval: Interval;
  }): IntervalWithFilter[];
  sync1(params: { missingIntervals: IntervalWithFilter[] }): Promise<
    Map<
      number,
      {
        transactions: Set<Hash>;
        transactionReceipts: Set<Hash>;
      }
    >
  >;
  sync2(params: {
    interval: Interval;
    missingIntervals: IntervalWithFilter[];
    sync1Result: Map<
      number,
      {
        transactions: Set<Hash>;
        transactionReceipts: Set<Hash>;
      }
    >;
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
  chain: Chain;
  rpc: Rpc;
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
  if (args.chain.disableCache) {
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
          _eth_getLogs(args.rpc, {
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
                args.chain.name
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
          _eth_getTransactionReceipt(args.rpc, { hash }),
        ),
      );

      validateReceiptsAndBlock(
        transactionReceipts,
        block,
        "eth_getTransactionReceipt",
      );

      return transactionReceipts;
    }

    let blockReceipts: SyncTransactionReceipt[];
    try {
      blockReceipts = await _eth_getBlockReceipts(args.rpc, {
        blockHash: block.hash,
      });
    } catch (_error) {
      const error = _error as Error;
      args.common.logger.warn({
        service: "sync",
        msg: `Caught eth_getBlockReceipts error on '${
          args.chain.name
        }', switching to eth_getTransactionReceipt method.`,
        error,
      });

      isBlockReceipts = false;
      return syncTransactionReceipts(block, transactionHashes);
    }

    validateReceiptsAndBlock(blockReceipts, block, "eth_getBlockReceipts");

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
      chainId: args.chain.id,
    });
  };

  /**
   * Return all addresses that match `filter` after extracting addresses
   * that match `filter` and `interval`.
   */
  const syncAddressFactory = async (
    factory: Factory,
    interval: Interval,
  ): Promise<Map<Address, number>> => {
    const factoryInterval: Interval = [
      Math.max(factory.fromBlock ?? 0, interval[0]),
      Math.min(factory.toBlock ?? Number.POSITIVE_INFINITY, interval[1]),
    ];

    if (factoryInterval[0] <= factoryInterval[1]) {
      await syncLogFactory(factory, factoryInterval);
    }

    // Note: `factory` must refer to the same original `factory` in `filter`
    // and not be a recovered factory from `recoverFilter`.
    return args.syncStore.getChildAddresses({ factory });
  };

  ////////
  // Helper function for filter types
  ////////

  return {
    intervalsCache,
    calculateMissingIntervals({ interval }) {
      const intervalsToSync: {
        interval: Interval;
        filter: FilterWithoutBlocks;
      }[] = [];

      // Determine the requests that need to be made, and which intervals need to be inserted.
      // Fragments are used to create a minimal filter, to avoid refetching data even if a filter
      // is only partially synced.

      for (const { filter } of args.sources) {
        let filterIntervals: Interval[] = [
          [
            Math.max(filter.fromBlock ?? 0, interval[0]),
            Math.min(filter.toBlock ?? Number.POSITIVE_INFINITY, interval[1]),
          ],
        ];

        switch (filter.type) {
          case "log":
            if (isAddressFactory(filter.address)) {
              filterIntervals.push([
                Math.max(filter.address.fromBlock ?? 0, interval[0]),
                Math.min(
                  filter.address.toBlock ?? Number.POSITIVE_INFINITY,
                  interval[1],
                ),
              ]);
            }
            break;
          case "trace":
          case "transaction":
          case "transfer":
            if (isAddressFactory(filter.fromAddress)) {
              filterIntervals.push([
                Math.max(filter.fromAddress.fromBlock ?? 0, interval[0]),
                Math.min(
                  filter.fromAddress.toBlock ?? Number.POSITIVE_INFINITY,
                  interval[1],
                ),
              ]);
            }

            if (isAddressFactory(filter.toAddress)) {
              filterIntervals.push([
                Math.max(filter.toAddress.fromBlock ?? 0, interval[0]),
                Math.min(
                  filter.toAddress.toBlock ?? Number.POSITIVE_INFINITY,
                  interval[1],
                ),
              ]);
            }
        }

        filterIntervals = filterIntervals.filter(
          ([start, end]) => start <= end,
        );

        if (filterIntervals.length === 0) {
          continue;
        }

        filterIntervals = intervalUnion(filterIntervals);

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
            filterIntervals,
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

          intervalsToSync.push({
            filter: requiredFilter,
            interval: requiredInterval,
          });
        }
      }

      return intervalsToSync;
    },
    async sync1({ missingIntervals }) {
      const result = new Map<
        number,
        {
          transactions: Set<Hash>;
          transactionReceipts: Set<Hash>;
        }
      >();

      await Promise.all(
        missingIntervals.map(async ({ filter, interval }) => {
          try {
            switch (filter.type) {
              case "log": {
                let logs: SyncLog[];

                if (isAddressFactory(filter.address)) {
                  const childAddresses = await syncAddressFactory(
                    filter.address,
                    interval,
                  );

                  // Note: Exit early when only the factory needs to be synced
                  if (((filter as LogFilter).fromBlock ?? 0) > interval[1]) {
                    return;
                  }

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

                // TODO(kyle) validate logs and block

                const logsPerBlock = new Map<number, SyncLog[]>();
                for (const log of logs) {
                  const blockNumber = hexToNumber(log.blockNumber);
                  if (logsPerBlock.has(blockNumber) === false) {
                    logsPerBlock.set(blockNumber, []);
                  }
                  logsPerBlock.get(blockNumber)!.push(log);
                }

                for (const [blockNumber, logs] of logsPerBlock) {
                  if (result.has(blockNumber) === false) {
                    result.set(blockNumber, {
                      transactions: new Set(),
                      transactionReceipts: new Set(),
                    });
                  }

                  for (const log of logs) {
                    if (log.transactionHash === zeroHash) {
                      args.common.logger.warn({
                        service: "sync",
                        msg: `Detected '${args.chain.name}' log with empty transaction hash in block ${blockNumber} at log index ${hexToNumber(log.logIndex)}. This is expected for some chains like ZKsync.`,
                      });

                      continue;
                    }

                    result
                      .get(blockNumber)!
                      .transactions.add(log.transactionHash);
                    if (shouldGetTransactionReceipt(filter)) {
                      result
                        .get(blockNumber)!
                        .transactionReceipts.add(log.transactionHash);
                    }
                  }
                }

                await args.syncStore.insertLogs({
                  logs,
                  chainId: args.chain.id,
                });

                break;
              }
              case "transaction":
              case "trace":
              case "transfer": {
                await Promise.all([
                  isAddressFactory(filter.fromAddress)
                    ? syncAddressFactory(filter.fromAddress, interval)
                    : Promise.resolve(),
                  isAddressFactory(filter.toAddress)
                    ? syncAddressFactory(filter.toAddress, interval)
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
              msg: `Fatal error: Unable to sync '${args.chain.name}' from ${interval[0]} to ${interval[1]}.`,
              error,
            });

            args.onFatalError(error);

            return;
          }
        }),
      );

      return result;
    },
    async sync2({ interval, missingIntervals, sync1Result }) {
      const blockFilters: BlockFilter[] = [];
      const transactionFilters: TransactionFilter[] = [];
      const traceFilters: TraceFilter[] = [];
      const transferFilters: TransferFilter[] = [];
      const childAddresses: Map<Factory, Map<Address, number>> = new Map();

      for (const { filter, interval } of missingIntervals) {
        switch (filter.type) {
          case "block": {
            blockFilters.push(filter as BlockFilter);
            break;
          }

          case "transaction": {
            if (((filter as TransactionFilter).fromBlock ?? 0) > interval[1]) {
              continue;
            }

            transactionFilters.push(filter as TransactionFilter);
            break;
          }

          case "trace": {
            if (((filter as TraceFilter).fromBlock ?? 0) > interval[1]) {
              continue;
            }

            traceFilters.push(filter as TraceFilter);
            break;
          }

          case "transfer": {
            if (((filter as TransferFilter).fromBlock ?? 0) > interval[1]) {
              continue;
            }

            transferFilters.push(filter as TransferFilter);
            break;
          }
        }
      }

      for (const { filter } of missingIntervals) {
        switch (filter.type) {
          case "transaction":
          case "trace":
          case "transfer": {
            if (isAddressFactory(filter.fromAddress)) {
              childAddresses.set(
                filter.fromAddress,
                await args.syncStore.getChildAddresses({
                  factory: filter.fromAddress,
                }),
              );
            }

            if (isAddressFactory(filter.toAddress)) {
              childAddresses.set(
                filter.toAddress,
                await args.syncStore.getChildAddresses({
                  factory: filter.toAddress,
                }),
              );
            }
            break;
          }

          case "log": {
            if (isAddressFactory(filter.address)) {
              childAddresses.set(
                filter.address,
                await args.syncStore.getChildAddresses({
                  factory: filter.address,
                }),
              );
            }
            break;
          }
        }
      }

      const syncBlock = async (blockNumber: number) => {
        let block: SyncBlock | undefined;

        let requiredTransactions = new Set<Hash>();
        let requiredTransactionReceipts = new Set<Hash>();

        if (sync1Result.has(blockNumber)) {
          if (block === undefined) {
            block = await _eth_getBlockByNumber(args.rpc, { blockNumber });
          }

          requiredTransactions = sync1Result.get(blockNumber)!.transactions;
          requiredTransactionReceipts =
            sync1Result.get(blockNumber)!.transactionReceipts;
        }

        ////////
        // Traces
        ////////

        const shouldRequestTraces =
          traceFilters.length > 0 || transferFilters.length > 0;

        let traces: SyncTrace[] = [];
        if (shouldRequestTraces) {
          if (block === undefined) {
            [block, traces] = await Promise.all([
              _eth_getBlockByNumber(args.rpc, { blockNumber }),
              _debug_traceBlockByNumber(args.rpc, { blockNumber }),
            ]);
          } else {
            traces = await _debug_traceBlockByNumber(args.rpc, { blockNumber });
          }

          validateTracesAndBlock(traces, block);

          traces = traces.filter((trace) => {
            let isMatched = false;
            for (const filter of transferFilters) {
              if (
                isTransferFilterMatched({
                  filter,
                  trace: trace.trace,
                  block: { number: BigInt(blockNumber) },
                }) &&
                (isAddressFactory(filter.fromAddress)
                  ? isAddressMatched({
                      address: trace.trace.from,
                      blockNumber,
                      childAddresses: childAddresses.get(filter.fromAddress)!,
                    })
                  : true) &&
                (isAddressFactory(filter.toAddress)
                  ? isAddressMatched({
                      address: trace.trace.to,
                      blockNumber,
                      childAddresses: childAddresses.get(filter.toAddress)!,
                    })
                  : true)
              ) {
                isMatched = true;
                requiredTransactions.add(trace.transactionHash);
                if (shouldGetTransactionReceipt(filter)) {
                  requiredTransactionReceipts.add(trace.transactionHash);
                  // skip to next trace
                  break;
                }
              }
            }

            for (const filter of traceFilters) {
              if (
                isTraceFilterMatched({
                  filter,
                  trace: trace.trace,
                  block: { number: BigInt(blockNumber) },
                }) &&
                (isAddressFactory(filter.fromAddress)
                  ? isAddressMatched({
                      address: trace.trace.from,
                      blockNumber,
                      childAddresses: childAddresses.get(filter.fromAddress)!,
                    })
                  : true) &&
                (isAddressFactory(filter.toAddress)
                  ? isAddressMatched({
                      address: trace.trace.to,
                      blockNumber,
                      childAddresses: childAddresses.get(filter.toAddress)!,
                    })
                  : true)
              ) {
                isMatched = true;
                requiredTransactions.add(trace.transactionHash);
                if (shouldGetTransactionReceipt(filter)) {
                  requiredTransactionReceipts.add(trace.transactionHash);
                  // skip to next trace
                  break;
                }
              }
            }

            return isMatched;
          });
        }

        ////////
        // Block
        ////////

        if (
          block === undefined &&
          blockFilters.some((filter) =>
            isBlockFilterMatched({
              filter,
              block: { number: BigInt(blockNumber) },
            }),
          )
        ) {
          block = await _eth_getBlockByNumber(args.rpc, { blockNumber });
        }

        ////////
        // Transactions
        ////////

        if (block === undefined && transactionFilters.length === 0) {
          return;
        }

        if (block === undefined) {
          block = await _eth_getBlockByNumber(args.rpc, { blockNumber });
        }
        validateTransactionsAndBlock(block);

        const transactions = block.transactions.filter((transaction) => {
          let isMatched = requiredTransactions.has(transaction.hash);
          for (const filter of transactionFilters) {
            if (isTransactionFilterMatched({ filter, transaction })) {
              requiredTransactions.add(transaction.hash);
              requiredTransactionReceipts.add(transaction.hash);
              isMatched = true;
            }
          }
          return isMatched;
        });

        ////////
        // Transaction Receipts
        ////////

        const transactionReceipts = await syncTransactionReceipts(
          block,
          requiredTransactionReceipts,
        );

        const transactionsByHash = new Map<Hash, SyncTransaction>();
        for (const transaction of transactions) {
          transactionsByHash.set(transaction.hash, transaction);
        }

        await args.syncStore.insertBlocks({
          blocks: [block],
          chainId: args.chain.id,
        });

        await args.syncStore.insertTransactions({
          transactions,
          chainId: args.chain.id,
        });

        await args.syncStore.insertTransactionReceipts({
          transactionReceipts,
          chainId: args.chain.id,
        });

        await args.syncStore.insertTraces({
          traces: traces.map((trace) => ({
            trace,
            block: block!,
            transaction: transactionsByHash.get(trace.transactionHash)!,
          })),
          chainId: args.chain.id,
        });
      };

      const queue = createQueue({
        browser: false,
        concurrency: 50,
        initialStart: true,
        worker: syncBlock,
      });

      await Promise.all(intervalRange(interval).map(queue.add));

      await args.syncStore.insertIntervals({
        intervals: missingIntervals,
        chainId: args.chain.id,
      });
    },
  };
};
