import {
  setupCleanup,
  setupCommon,
  setupDatabaseConfig,
  setupPonder,
} from "@/_test/setup.js";
import { setupAnvil } from "@/_test/setup.js";
import {
  getBlocksConfigAndIndexingFunctions,
  getNetwork,
  testClient,
} from "@/_test/utils.js";
import type { BlockFilter, Event, Filter, Fragment } from "@/internal/types.js";
import { createHistoricalSync } from "@/sync-historical/index.js";
import {
  MAX_CHECKPOINT_STRING,
  ZERO_CHECKPOINT_STRING,
  decodeCheckpoint,
} from "@/utils/checkpoint.js";
import { drainAsyncGenerator } from "@/utils/generators.js";
import type { Interval } from "@/utils/interval.js";
import { promiseWithResolvers } from "@/utils/promiseWithResolvers.js";
import { _eth_getBlockByNumber } from "@/utils/rpc.js";
import { beforeEach, expect, test, vi } from "vitest";
import { getFragments } from "./fragments.js";
import {
  createSync,
  getCachedBlock,
  getChainCheckpoint,
  getLocalEventGenerator,
  getLocalSyncGenerator,
  getLocalSyncProgress,
  getPerChainOnRealtimeSyncEvent,
  mergeAsyncGeneratorsWithEventOrder,
  splitEvents,
} from "./index.js";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupDatabaseConfig);
beforeEach(setupCleanup);

test("splitEvents()", async () => {
  const events = [
    {
      chainId: 1,
      checkpoint: "0",
      event: {
        block: {
          hash: "0x1",
          timestamp: 1,
          number: 1n,
        },
      },
    },
    {
      chainId: 1,
      checkpoint: "0",
      event: {
        block: {
          hash: "0x2",
          timestamp: 2,
          number: 2n,
        },
      },
    },
  ] as unknown as Event[];

  const result = splitEvents(events);

  expect(result).toMatchInlineSnapshot(`
    [
      {
        "checkpoint": "000000000100000000000000010000000000000001999999999999999999999999999999999",
        "events": [
          {
            "chainId": 1,
            "checkpoint": "0",
            "event": {
              "block": {
                "hash": "0x1",
                "number": 1n,
                "timestamp": 1,
              },
            },
          },
        ],
      },
      {
        "checkpoint": "000000000200000000000000010000000000000002999999999999999999999999999999999",
        "events": [
          {
            "chainId": 1,
            "checkpoint": "0",
            "event": {
              "block": {
                "hash": "0x2",
                "number": 2n,
                "timestamp": 2,
              },
            },
          },
        ],
      },
    ]
  `);
});

test("getPerChainOnRealtimeSyncEvent() handles block", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >();
  for (const filter of app.indexingBuild.filters) {
    for (const { fragment } of getFragments(filter)) {
      intervalsCache.set(filter, [{ fragment, intervals: [] }]);
    }
  }

  await testClient.mine({ blocks: 1 });

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache,
  });

  const onRealtimeSyncEvent = getPerChainOnRealtimeSyncEvent(app, {
    syncProgress,
  });

  const block = await _eth_getBlockByNumber(app.indexingBuild.requestQueue, {
    blockNumber: 1,
  });

  const event = await onRealtimeSyncEvent({
    type: "block",
    hasMatchedFilter: false,
    block,
    logs: [],
    traces: [],
    transactions: [],
    transactionReceipts: [],
    childAddresses: new Map(),
  });

  expect(event.type).toBe("block");
});

test("getPerChainOnRealtimeSyncEvent() handles finalize", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });

  const app = await setupPonder(context, { config, indexingFunctions });

  const intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >();
  for (const filter of app.indexingBuild.filters) {
    for (const { fragment } of getFragments(filter)) {
      intervalsCache.set(filter, [{ fragment, intervals: [] }]);
    }
  }

  // finalized block: 0

  await testClient.mine({ blocks: 1 });

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache,
  });

  const onRealtimeSyncEvent = getPerChainOnRealtimeSyncEvent(app, {
    syncProgress,
  });

  const block = await _eth_getBlockByNumber(app.indexingBuild.requestQueue, {
    blockNumber: 1,
  });

  await onRealtimeSyncEvent({
    type: "block",
    hasMatchedFilter: true,
    block,
    logs: [],
    traces: [],
    transactions: [],
    transactionReceipts: [],
    childAddresses: new Map(),
  });

  const event = await onRealtimeSyncEvent({
    type: "finalize",
    block,
  });

  expect(event.type).toBe("finalize");

  const blocks = await app.database.qb.sync
    .selectFrom("blocks")
    .selectAll()
    .execute();

  expect(blocks).toHaveLength(1);

  const intervals = await app.database.qb.sync
    .selectFrom("intervals")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(1);
  expect(intervals[0]!.blocks).toBe("{[0,2]}");
});

test("getPerChainOnRealtimeSyncEvent() handles reorg", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >();
  for (const filter of app.indexingBuild.filters) {
    for (const { fragment } of getFragments(filter)) {
      intervalsCache.set(filter, [{ fragment, intervals: [] }]);
    }
  }

  // finalized block: 0

  await testClient.mine({ blocks: 1 });

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache,
  });

  const onRealtimeSyncEvent = getPerChainOnRealtimeSyncEvent(app, {
    syncProgress,
  });

  const block = await _eth_getBlockByNumber(app.indexingBuild.requestQueue, {
    blockNumber: 1,
  });

  await onRealtimeSyncEvent({
    type: "block",
    hasMatchedFilter: true,
    block,
    logs: [],
    traces: [],
    transactions: [],
    transactionReceipts: [],
    childAddresses: new Map(),
  });

  const event = await onRealtimeSyncEvent({
    type: "reorg",
    block,
    reorgedBlocks: [block],
  });

  expect(event.type).toBe("reorg");
});

test("getLocalEventGenerator()", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 1 });

  // finalized block: 1
  app.indexingBuild.network.finalityBlockCount = 0;

  const historicalSync = await createHistoricalSync(app, {
    onFatalError: () => {},
  });

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache: historicalSync.intervalsCache,
  });

  const syncGenerator = getLocalSyncGenerator(app, {
    syncProgress,
    historicalSync,
  });

  const eventGenerator = getLocalEventGenerator(app, {
    localSyncGenerator: syncGenerator,
    from: getChainCheckpoint({ syncProgress, network, tag: "start" })!,
    to: getChainCheckpoint({ syncProgress, network, tag: "finalized" })!,
    limit: 100,
  });

  const events = await drainAsyncGenerator(eventGenerator);
  expect(events).toHaveLength(1);
});

test("getLocalEventGenerator() pagination", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 2 });

  // finalized block: 2
  app.indexingBuild.network.finalityBlockCount = 0;

  const historicalSync = await createHistoricalSync(app, {
    onFatalError: () => {},
  });

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache: historicalSync.intervalsCache,
  });

  const syncGenerator = getLocalSyncGenerator(app, {
    syncProgress,
    historicalSync,
  });

  const eventGenerator = getLocalEventGenerator(app, {
    localSyncGenerator: syncGenerator,
    from: getChainCheckpoint({ syncProgress, network, tag: "start" })!,
    to: getChainCheckpoint({ syncProgress, network, tag: "finalized" })!,
    limit: 1,
  });

  const events = await drainAsyncGenerator(eventGenerator);
  expect(events.length).toBeGreaterThan(1);
});

test("getLocalSyncGenerator()", async (context) => {
  const network = getNetwork();

  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 1 });

  // finalized block: 1
  network.finalityBlockCount = 0;

  const historicalSync = await createHistoricalSync(app, {
    onFatalError: () => {},
  });

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache: historicalSync.intervalsCache,
  });

  const syncGenerator = getLocalSyncGenerator(app, {
    syncProgress,
    historicalSync,
  });

  await drainAsyncGenerator(syncGenerator);

  const intervals = await app.database.qb.sync
    .selectFrom("intervals")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(1);
  expect(intervals[0]!.blocks).toBe("{[0,2]}");
});

test("getLocalSyncGenerator() with partial cache", async (context) => {
  const network = getNetwork();

  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 1 });

  // finalized block: 1
  network.finalityBlockCount = 0;

  let historicalSync = await createHistoricalSync(app, {
    onFatalError: () => {},
  });

  let syncProgress = await getLocalSyncProgress(app, {
    intervalsCache: historicalSync.intervalsCache,
  });

  let syncGenerator = getLocalSyncGenerator(app, {
    syncProgress,
    historicalSync,
  });

  await drainAsyncGenerator(syncGenerator);

  await testClient.mine({ blocks: 1 });

  historicalSync = await createHistoricalSync(app, {
    onFatalError: () => {},
  });

  syncProgress = await getLocalSyncProgress(app, {
    intervalsCache: historicalSync.intervalsCache,
  });

  syncGenerator = getLocalSyncGenerator(app, {
    syncProgress,
    historicalSync,
  });

  await drainAsyncGenerator(syncGenerator);

  const intervals = await app.database.qb.sync
    .selectFrom("intervals")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(1);
  expect(intervals[0]!.blocks).toBe("{[0,3]}");
});

test("getLocalSyncGenerator() with full cache", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 1 });

  // finalized block: 1
  app.indexingBuild.network.finalityBlockCount = 0;

  let historicalSync = await createHistoricalSync(app, {
    onFatalError: () => {},
  });

  let syncProgress = await getLocalSyncProgress(app, {
    intervalsCache: historicalSync.intervalsCache,
  });

  let syncGenerator = getLocalSyncGenerator(app, {
    syncProgress,
    historicalSync,
  });

  await drainAsyncGenerator(syncGenerator);

  historicalSync = await createHistoricalSync(app, {
    onFatalError: () => {},
  });

  syncProgress = await getLocalSyncProgress(app, {
    intervalsCache: historicalSync.intervalsCache,
  });

  syncGenerator = getLocalSyncGenerator(app, {
    syncProgress,
    historicalSync,
  });

  const insertSpy = vi.spyOn(syncStore, "insertIntervals");
  const requestSpy = vi.spyOn(app.indexingBuild.requestQueue, "request");

  const checkpoints = await drainAsyncGenerator(syncGenerator);
  expect(checkpoints).toHaveLength(1);

  expect(insertSpy).toHaveBeenCalledTimes(0);
  expect(requestSpy).toHaveBeenCalledTimes(0);
});

test("getLocalSyncProgress()", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >();
  for (const filter of app.indexingBuild.filters) {
    for (const { fragment } of getFragments(filter)) {
      intervalsCache.set(filter, [{ fragment, intervals: [] }]);
    }
  }

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache,
  });

  expect(syncProgress.finalized.number).toBe("0x0");
  expect(syncProgress.start.number).toBe("0x0");
  expect(syncProgress.end).toBe(undefined);
  expect(syncProgress.current).toBe(undefined);
});

test("getLocalSyncProgress() future end block", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });

  // @ts-ignore
  config.blocks.Blocks.endBlock = 12;

  const app = await setupPonder(context, { config, indexingFunctions });

  const intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >();
  for (const filter of app.indexingBuild.filters) {
    for (const { fragment } of getFragments(filter)) {
      intervalsCache.set(filter, [{ fragment, intervals: [] }]);
    }
  }

  const syncProgress = await getLocalSyncProgress(app, {
    intervalsCache,
  });

  expect(syncProgress.finalized.number).toBe("0x0");
  expect(syncProgress.start.number).toBe("0x0");
  expect(syncProgress.end).toMatchInlineSnapshot(`
    {
      "hash": "0x",
      "number": "0xc",
      "parentHash": "0x",
      "timestamp": "0x2540be3ff",
    }
  `);
  expect(syncProgress.current).toBe(undefined);
});

test("getCachedBlock() no cached intervals", async () => {
  const filter = {
    type: "block",
    chainId: 1,
    interval: 1,
    offset: 0,
    fromBlock: 0,
    toBlock: 100,
    include: [],
  } satisfies BlockFilter;

  const intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >([[filter, []]]);

  const cachedBlock = getCachedBlock({
    filters: [filter],
    intervalsCache,
  });

  expect(cachedBlock).toBe(undefined);
});

test("getCachedBlock() with cache", async () => {
  const filter = {
    type: "block",
    chainId: 1,
    interval: 1,
    offset: 0,
    fromBlock: 0,
    toBlock: 100,
    include: [],
  } satisfies BlockFilter;

  let intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >([[filter, [{ fragment: {} as Fragment, intervals: [[0, 24]] }]]]);

  let cachedBlock = getCachedBlock({
    filters: [filter],
    intervalsCache,
  });

  expect(cachedBlock).toBe(24);

  intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >([
    [
      filter,
      [
        {
          fragment: {} as Fragment,
          intervals: [
            [0, 50],
            [50, 102],
          ],
        },
      ],
    ],
  ]);

  cachedBlock = getCachedBlock({
    filters: [filter],
    intervalsCache,
  });

  expect(cachedBlock).toBe(100);
});

test("getCachedBlock() with incomplete cache", async () => {
  const filter = {
    type: "block",
    chainId: 1,
    interval: 1,
    offset: 0,
    fromBlock: 0,
    toBlock: 100,
    include: [],
  } satisfies BlockFilter;

  const intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >([[filter, [{ fragment: {} as Fragment, intervals: [[1, 24]] }]]]);

  const cachedBlock = getCachedBlock({
    filters: [filter],
    intervalsCache,
  });

  expect(cachedBlock).toBeUndefined();
});

test("getCachedBlock() with multiple filters", async () => {
  const filters = [
    {
      type: "block",
      chainId: 1,
      interval: 1,
      offset: 0,
      fromBlock: 0,
      toBlock: 100,
      include: [],
    },
    {
      type: "block",
      chainId: 1,
      interval: 1,
      offset: 1,
      fromBlock: 50,
      toBlock: 150,
      include: [],
    },
  ] satisfies BlockFilter[];

  let intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >([
    [filters[0]!, [{ fragment: {} as Fragment, intervals: [[0, 24]] }]],
    [filters[1]!, []],
  ]);

  let cachedBlock = getCachedBlock({
    filters,
    intervalsCache,
  });

  expect(cachedBlock).toBe(24);

  intervalsCache = new Map<
    Filter,
    { fragment: Fragment; intervals: Interval[] }[]
  >([
    [filters[0]!, [{ fragment: {} as Fragment, intervals: [[0, 24]] }]],
    [filters[1]!, [{ fragment: {} as Fragment, intervals: [[50, 102]] }]],
  ]);

  cachedBlock = getCachedBlock({
    filters,
    intervalsCache,
  });

  expect(cachedBlock).toBe(24);
});

test("mergeAsyncGeneratorsWithEventOrder()", async () => {
  const p1 = promiseWithResolvers<{ events: Event[]; checkpoint: string }>();
  const p2 = promiseWithResolvers<{ events: Event[]; checkpoint: string }>();
  const p3 = promiseWithResolvers<{ events: Event[]; checkpoint: string }>();
  const p4 = promiseWithResolvers<{ events: Event[]; checkpoint: string }>();

  async function* generator1() {
    yield await p1.promise;
    yield await p2.promise;
  }

  async function* generator2() {
    yield await p3.promise;
    yield await p4.promise;
  }

  const results: { events: Event[]; checkpoint: string }[] = [];
  const generator = mergeAsyncGeneratorsWithEventOrder([
    generator1(),
    generator2(),
  ]);

  (async () => {
    for await (const result of generator) {
      results.push(result);
    }
  })();

  p1.resolve({
    events: [{ checkpoint: "01" }, { checkpoint: "07" }] as Event[],
    checkpoint: "10",
  });
  p3.resolve({
    events: [{ checkpoint: "02" }, { checkpoint: "05" }] as Event[],
    checkpoint: "06",
  });

  await new Promise((res) => setTimeout(res));

  p4.resolve({
    events: [{ checkpoint: "08" }, { checkpoint: "11" }] as Event[],
    checkpoint: "20",
  });
  p2.resolve({
    events: [{ checkpoint: "08" }, { checkpoint: "13" }] as Event[],
    checkpoint: "20",
  });

  await new Promise((res) => setTimeout(res));

  expect(results).toMatchInlineSnapshot(`
    [
      {
        "checkpoint": "06",
        "events": [
          {
            "checkpoint": "01",
          },
          {
            "checkpoint": "02",
          },
          {
            "checkpoint": "05",
          },
        ],
      },
      {
        "checkpoint": "10",
        "events": [
          {
            "checkpoint": "07",
          },
          {
            "checkpoint": "08",
          },
        ],
      },
      {
        "checkpoint": "20",
        "events": [
          {
            "checkpoint": "08",
          },
          {
            "checkpoint": "11",
          },
          {
            "checkpoint": "13",
          },
        ],
      },
    ]
  `);
});

test("createSync()", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  config.ordering = "multichain";

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  expect(sync).toBeDefined();
});

test("getEvents() multichain", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 1 });

  // finalized block: 1
  app.indexingBuild.network.finalityBlockCount = 0;

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  const events = await drainAsyncGenerator(sync.getEvents()).then((events) =>
    events.flat(),
  );

  expect(events).toBeDefined();
  expect(events).toHaveLength(2);
});

test("getEvents() omnichain", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "omnichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 1 });

  // finalized block: 1
  app.indexingBuild.network.finalityBlockCount = 0;

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  const events = await drainAsyncGenerator(sync.getEvents()).then((events) =>
    events.flat(),
  );

  expect(events).toBeDefined();
  expect(events).toHaveLength(2);
});

test("getEvents() mulitchain updates status", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 2 });

  // finalized block: 2
  app.indexingBuild.network.finalityBlockCount = 0;

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  await drainAsyncGenerator(sync.getEvents());

  const status = sync.getStatus();

  expect(status).toMatchInlineSnapshot();
});

test("getEvents() omnichain updates status", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "omnichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 2 });

  // finalized block: 2
  app.indexingBuild.network.finalityBlockCount = 0;

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  await drainAsyncGenerator(sync.getEvents());

  const status = sync.getStatus();

  expect(status).toMatchInlineSnapshot();
});

test("getEvents() with initial checkpoint", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 2 });

  // finalized block: 2
  app.indexingBuild.network.finalityBlockCount = 0;

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {},
    crashRecoveryCheckpoint: MAX_CHECKPOINT_STRING,
  });

  const events = await drainAsyncGenerator(sync.getEvents()).then((events) =>
    events.flat(),
  );

  expect(events).toBeDefined();
  expect(events).toHaveLength(0);
});

// Note: this test is causing a flake on ci.
// We need a way to figure out how to make sure queues are drained
// when shutting down.
test.skip("startRealtime()", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  await testClient.mine({ blocks: 2 });

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  await drainAsyncGenerator(sync.getEvents());

  await sync.startRealtime();

  const status = sync.getStatus();

  expect(status).toMatchInlineSnapshot();
});

test("onEvent() multichain handles block", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  const promise = promiseWithResolvers<void>();
  const events: Event[] = [];

  await testClient.mine({ blocks: 1 });

  const sync = await createSync(app, {
    onRealtimeEvent: async (event) => {
      if (event.type === "block") {
        events.push(...event.events);
        promise.resolve();
      }
    },
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  await drainAsyncGenerator(sync.getEvents());

  await sync.startRealtime();

  await promise.promise;

  expect(events).toHaveLength(1);
});

test("onEvent() omnichain handles block", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "omnichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  // finalized block: 0

  const promise = promiseWithResolvers<void>();

  const sync = await createSync(app, {
    onRealtimeEvent: async (event) => {
      if (event.type === "block") {
        promise.resolve();
      }
    },
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  await testClient.mine({ blocks: 1 });

  await drainAsyncGenerator(sync.getEvents());

  await sync.startRealtime();

  await promise.promise;
});

test("onEvent() handles finalize", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  const promise = promiseWithResolvers<void>();
  let checkpoint: string;

  // finalized block: 0

  app.indexingBuild.network.finalityBlockCount = 2;

  const sync = await createSync(app, {
    onRealtimeEvent: async (event) => {
      if (event.type === "finalize") {
        checkpoint = event.checkpoint;
        promise.resolve();
      }
    },
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  await testClient.mine({ blocks: 4 });

  await drainAsyncGenerator(sync.getEvents());

  await sync.startRealtime();

  await promise.promise;

  expect(decodeCheckpoint(checkpoint!).blockNumber).toBe(2n);
});

test("onEvent() kills realtime when finalized", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  // @ts-ignore
  config.blocks.Blocks.endBlock = 1;

  const app = await setupPonder(context, { config, indexingFunctions });

  const promise = promiseWithResolvers<void>();
  let checkpoint: string;

  // finalized block: 0

  app.indexingBuild.network.finalityBlockCount = 0;

  const sync = await createSync(app, {
    onRealtimeEvent: async (event) => {
      if (event.type === "finalize") {
        checkpoint = event.checkpoint;
        promise.resolve();
      }
    },
    onFatalError: () => {},
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
    ordering: "multichain",
  });

  await testClient.mine({ blocks: 4 });

  await drainAsyncGenerator(sync.getEvents());

  await sync.startRealtime();

  await promise.promise;

  expect(decodeCheckpoint(checkpoint!).blockNumber).toBe(1n);
});

test.todo("onEvent() handles reorg");

test("onEvent() handles errors", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  config.ordering = "multichain";
  const app = await setupPonder(context, { config, indexingFunctions });

  const promise = promiseWithResolvers<void>();

  // finalized block: 0

  const sync = await createSync(app, {
    onRealtimeEvent: async () => {},
    onFatalError: () => {
      promise.resolve();
    },
    crashRecoveryCheckpoint: ZERO_CHECKPOINT_STRING,
  });

  await testClient.mine({ blocks: 4 });

  await drainAsyncGenerator(sync.getEvents());

  const spy = vi.spyOn(syncStore, "insertTransactions");
  spy.mockRejectedValue(new Error());

  await sync.startRealtime();

  await promise.promise;
});
