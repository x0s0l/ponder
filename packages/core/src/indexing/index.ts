import type { IndexingStore } from "@/indexing-store/index.js";
import { ShutdownError } from "@/internal/errors.js";
import type {
  Event,
  Network,
  PonderApp,
  Schema,
  SetupEvent,
  TraceFilter,
} from "@/internal/types.js";
import type { SyncStore } from "@/sync-store/index.js";
import { isAddressFactory } from "@/sync/filter.js";
import { cachedTransport } from "@/sync/transport.js";
import type { Db } from "@/types/db.js";
import type { Block, Log, Trace, Transaction } from "@/types/eth.js";
import type { DeepPartial } from "@/types/utils.js";
import {
  ZERO_CHECKPOINT,
  decodeCheckpoint,
  encodeCheckpoint,
} from "@/utils/checkpoint.js";
import { prettyPrint } from "@/utils/print.js";
import { startClock } from "@/utils/timer.js";
import { type Abi, type Address, createClient } from "viem";
import { checksumAddress } from "viem";
import { addStackTrace } from "./addStackTrace.js";
import type { ReadOnlyClient } from "./ponderActions.js";
import { getPonderActions } from "./ponderActions.js";

export type Context = {
  network: { chainId: number; name: string };
  client: ReadOnlyClient;
  db: Db<Schema>;
  contracts: Record<
    string,
    {
      abi: Abi;
      address?: Address | readonly Address[];
      startBlock?: number;
      endBlock?: number;
    }
  >;
};

export type Indexing = {
  processSetupEvents: ({
    db,
  }: { db: IndexingStore }) => Promise<
    { status: "error"; error: Error } | { status: "success" }
  >;
  processEvents: ({
    events,
    db,
  }: { events: Event[]; db: IndexingStore }) => Promise<
    { status: "error"; error: Error } | { status: "success" }
  >;
};

export const createIndexing = (
  app: PonderApp,
  { syncStore }: { syncStore: SyncStore },
): Indexing => {
  let blockNumber: bigint = undefined!;
  const context: Context = {
    network: { name: undefined!, chainId: undefined! },
    contracts: undefined!,
    client: undefined!,
    db: undefined!,
  };

  const eventCount: { [eventName: string]: number } = {};
  const perChainClients = new Map<Network, ReadOnlyClient>();
  const perChainContracts = new Map<
    Network,
    {
      [contractName: string]: {
        abi: Abi;
        address?: Address | readonly Address[];
        startBlock?: number;
        endBlock?: number;
      };
    }
  >();

  const eventNames = app.indexingBuild.flatMap(({ eventCallbacks }) =>
    eventCallbacks.map(({ name }) => name),
  );

  for (const eventName of eventNames) {
    eventCount[eventName] = 0;
  }

  for (const indexingBuild of app.indexingBuild) {
    perChainContracts.set(indexingBuild.network, {});

    for (const eventCallback of indexingBuild.eventCallbacks) {
      if (eventCallback.type !== "contract") continue;

      let address: Address | undefined;

      if (eventCallback.filter.type === "log") {
        const _address = eventCallback.filter.address;
        if (
          isAddressFactory(_address) === false &&
          Array.isArray(_address) === false &&
          _address !== undefined
        ) {
          address = _address as Address;
        }
      } else {
        const _address = (eventCallback.filter as TraceFilter).toAddress;
        if (isAddressFactory(_address) === false && _address !== undefined) {
          address = (_address as Address[])[0];
        }
      }

      perChainContracts.get(indexingBuild.network)![eventCallback.name] = {
        abi: eventCallback.metadata.abi,
        address: address ? checksumAddress(address) : address,
        startBlock: eventCallback.filter.fromBlock,
        endBlock: eventCallback.filter.toBlock,
      };
    }
  }

  for (const { network, requestQueue } of app.indexingBuild) {
    perChainClients.set(
      network,
      createClient({
        transport: cachedTransport({ requestQueue, syncStore }),
        chain: network.chain,
        // @ts-ignore
      }).extend(getPonderActions(() => blockNumber!)),
    );
  }

  const updateCompletedEvents = () => {
    for (const event of Object.keys(eventCount)) {
      const metricLabel = {
        event,
      };
      app.common.metrics.ponder_indexing_completed_events.set(
        metricLabel,
        eventCount[event]!,
      );
    }
  };

  const executeSetup = async ({
    event,
  }: { event: SetupEvent }): Promise<
    { status: "error"; error: Error } | { status: "success" }
  > => {
    const metricLabel = { event: event.eventCallback.name };

    try {
      blockNumber = BigInt(event.eventCallback.filter.fromBlock ?? 0);
      context.network.chainId = event.network.chainId;
      context.network.name = event.network.name;
      context.client = perChainClients.get(event.network)!;
      context.contracts = perChainContracts.get(event.network)!;

      const endClock = startClock();

      await event.eventCallback.callback({ context });

      app.common.metrics.ponder_indexing_function_duration.observe(
        metricLabel,
        endClock(),
      );
    } catch (_error) {
      const error =
        _error instanceof Error ? _error : new Error(String(_error));

      if (app.common.shutdown.isKilled) {
        throw new ShutdownError();
      }

      addStackTrace(app, { error });
      addErrorMeta({ error, meta: toErrorMeta(event) });

      const decodedCheckpoint = decodeCheckpoint(event.checkpoint);
      app.common.logger.error({
        service: "indexing",
        msg: `Error while processing '${event.eventCallback.name}' event in '${event.network.name}' block ${decodedCheckpoint.blockNumber}`,
        error,
      });

      app.common.metrics.ponder_indexing_has_error.set(1);

      return { status: "error", error: error };
    }

    return { status: "success" };
  };

  const executeEvent = async ({
    event,
  }: { event: Event }): Promise<
    { status: "error"; error: Error } | { status: "success" }
  > => {
    const metricLabel = { event: event.eventCallback.name };

    try {
      blockNumber = event.event.block.number;
      context.network.chainId = event.network.chainId;
      context.network.name = event.network.name;
      context.client = perChainClients.get(event.network)!;
      context.contracts = perChainContracts.get(event.network)!;

      const endClock = startClock();

      await event.eventCallback.callback({ event: event.event, context });

      app.common.metrics.ponder_indexing_function_duration.observe(
        metricLabel,
        endClock(),
      );
    } catch (_error) {
      const error =
        _error instanceof Error ? _error : new Error(String(_error));

      if (app.common.shutdown.isKilled) {
        throw new ShutdownError();
      }

      addStackTrace(app, { error });
      addErrorMeta({ error, meta: toErrorMeta(event) });

      const decodedCheckpoint = decodeCheckpoint(event.checkpoint);

      app.common.logger.error({
        service: "indexing",
        msg: `Error while processing '${event.eventCallback.name}' event in '${event.network.name}' block ${decodedCheckpoint.blockNumber}`,
        error,
      });

      app.common.metrics.ponder_indexing_has_error.set(1);

      return { status: "error", error };
    }

    return { status: "success" };
  };

  return {
    async processSetupEvents({ db }) {
      context.db = db;
      for (const indexingBuild of app.indexingBuild) {
        for (const eventCallback of indexingBuild.eventCallbacks) {
          if (eventCallback.type !== "setup") continue;

          eventCount[eventCallback.name]!++;

          const result = await executeSetup({
            event: {
              type: "setup",
              checkpoint: encodeCheckpoint({
                ...ZERO_CHECKPOINT,
                chainId: BigInt(indexingBuild.network.chainId),
                blockNumber: BigInt(eventCallback.filter.fromBlock ?? 0),
              }),
              network: indexingBuild.network,
              eventCallback,
            },
          });

          if (result.status !== "success") {
            return result;
          }
        }
      }

      return { status: "success" };
    },
    async processEvents({ events, db }) {
      context.db = db;
      for (let i = 0; i < events.length; i++) {
        const event = events[i]!;
        db.event = event;

        eventCount[event.eventCallback.name]!++;

        app.common.logger.trace({
          service: "indexing",
          msg: `Started indexing function (event="${event.eventCallback.name}", checkpoint=${event.checkpoint})`,
        });

        const result = await executeEvent({ event });
        if (result.status !== "success") {
          return result;
        }

        app.common.logger.trace({
          service: "indexing",
          msg: `Completed indexing function (event="${event.eventCallback.name}", checkpoint=${event.checkpoint})`,
        });
      }

      // set completed events
      updateCompletedEvents();

      return { status: "success" };
    },
  };
};

export const toErrorMeta = (
  event: DeepPartial<Event> | DeepPartial<SetupEvent>,
) => {
  switch (event?.type) {
    case "setup": {
      return `Block:\n${prettyPrint({
        number: event?.eventCallback?.filter?.fromBlock ?? 0,
      })}`;
    }

    case "log": {
      return [
        `Event arguments:\n${prettyPrint(event?.event?.args)}`,
        logText(event?.event?.log),
        transactionText(event?.event?.transaction),
        blockText(event?.event?.block),
      ].join("\n");
    }

    case "trace": {
      return [
        `Call trace arguments:\n${prettyPrint(event?.event?.args)}`,
        traceText(event?.event?.trace),
        transactionText(event?.event?.transaction),
        blockText(event?.event?.block),
      ].join("\n");
    }

    case "transfer": {
      return [
        `Transfer arguments:\n${prettyPrint(event?.event?.transfer)}`,
        traceText(event?.event?.trace),
        transactionText(event?.event?.transaction),
        blockText(event?.event?.block),
      ].join("\n");
    }

    case "block": {
      return blockText(event?.event?.block);
    }

    case "transaction": {
      return [
        transactionText(event?.event?.transaction),
        blockText(event?.event?.block),
      ].join("\n");
    }

    default: {
      return undefined;
    }
  }
};

export const addErrorMeta = ({
  error,
  meta,
}: { error: unknown; meta: string | undefined }) => {
  // If error isn't an object we can modify, do nothing
  if (typeof error !== "object" || error === null) return;
  if (meta === undefined) return;

  try {
    const errorObj = error as { meta?: unknown };
    // If meta exists and is an array, try to add to it
    if (Array.isArray(errorObj.meta)) {
      errorObj.meta = [...errorObj.meta, meta];
    } else {
      // Otherwise set meta to be a new array with the meta string
      errorObj.meta = [meta];
    }
  } catch {
    // Ignore errors
  }
};

const blockText = (block?: DeepPartial<Block>) =>
  `Block:\n${prettyPrint({
    hash: block?.hash,
    number: block?.number,
    timestamp: block?.timestamp,
  })}`;

const transactionText = (transaction?: DeepPartial<Transaction>) =>
  `Transaction:\n${prettyPrint({
    hash: transaction?.hash,
    from: transaction?.from,
    to: transaction?.to,
  })}`;

const logText = (log?: DeepPartial<Log>) =>
  `Log:\n${prettyPrint({
    index: log?.logIndex,
    address: log?.address,
  })}`;

const traceText = (trace?: DeepPartial<Trace>) =>
  `Trace:\n${prettyPrint({
    traceIndex: trace?.traceIndex,
    from: trace?.from,
    to: trace?.to,
  })}`;
