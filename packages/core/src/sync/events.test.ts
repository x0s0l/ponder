import { ALICE, BOB } from "@/_test/constants.js";
import { erc20ABI } from "@/_test/generated.js";
import {
  setupCleanup,
  setupCommon,
  setupDatabaseConfig,
  setupPonder,
} from "@/_test/setup.js";
import {
  getAccountsConfigAndIndexingFunctions,
  getBlocksConfigAndIndexingFunctions,
  getErc20ConfigAndIndexingFunctions,
} from "@/_test/utils.js";
import type {
  BlockEvent,
  LogEvent,
  RawEvent,
  TraceEvent,
  TransferEvent,
} from "@/internal/types.js";
import { ZERO_CHECKPOINT_STRING } from "@/utils/checkpoint.js";
import {
  type Hex,
  encodeEventTopics,
  padHex,
  parseEther,
  toHex,
  zeroAddress,
} from "viem";
import { encodeFunctionData, encodeFunctionResult } from "viem/utils";
import { beforeEach, expect, test } from "vitest";
import {
  decodeEventLog,
  decodeEvents,
  removeNullCharacters,
} from "./events.js";

beforeEach(setupCommon);
beforeEach(setupDatabaseConfig);
beforeEach(setupCleanup);

test("decodeEvents() log", async (context) => {
  const { config, indexingFunctions } = getErc20ConfigAndIndexingFunctions({
    address: zeroAddress,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const topics = encodeEventTopics({
    abi: erc20ABI,
    eventName: "Transfer",
    args: {
      from: zeroAddress,
      to: ALICE,
    },
  });

  const data = padHex(toHex(parseEther("1")), { size: 32 });

  const rawEvent = {
    checkpoint: ZERO_CHECKPOINT_STRING,
    network: app.indexingBuild.network,
    eventCallback: app.indexingBuild.eventCallbacks[0],
    block: {} as RawEvent["block"],
    transaction: {} as RawEvent["transaction"],
    log: { data, topics },
  } as RawEvent;

  const events = decodeEvents(app, { rawEvents: [rawEvent] }) as [LogEvent];

  expect(events).toHaveLength(1);
  expect(events[0].event.args).toMatchObject({
    from: zeroAddress,
    to: ALICE,
    amount: parseEther("1"),
  });
});

test("decodeEvents() log error", async (context) => {
  const { config, indexingFunctions } = getErc20ConfigAndIndexingFunctions({
    address: zeroAddress,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const topics = encodeEventTopics({
    abi: erc20ABI,
    eventName: "Transfer",
    args: {
      from: zeroAddress,
      to: ALICE,
    },
  });

  // invalid log.data, causing an error when decoding
  const rawEvent = {
    checkpoint: ZERO_CHECKPOINT_STRING,
    network: app.indexingBuild.network,
    eventCallback: app.indexingBuild.eventCallbacks[0],
    block: {} as RawEvent["block"],
    transaction: {} as RawEvent["transaction"],
    log: {
      data: "0x0" as Hex,
      topics,
    },
  } as RawEvent;

  const events = decodeEvents(common, sources, [rawEvent]) as [LogEvent];

  expect(events).toHaveLength(0);
});

test("decodeEvents() block", async (context) => {
  const { config, indexingFunctions } = getBlocksConfigAndIndexingFunctions({
    interval: 1,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const rawEvent = {
    checkpoint: ZERO_CHECKPOINT_STRING,
    network: app.indexingBuild.network,
    eventCallback: app.indexingBuild.eventCallbacks[0],
    block: {
      number: 1n,
    } as RawEvent["block"],
    transaction: undefined,
    log: undefined,
  } as RawEvent;

  const events = decodeEvents(app, { rawEvents: [rawEvent] }) as [BlockEvent];

  expect(events).toHaveLength(1);
  expect(events[0].event.block).toMatchObject({
    number: 1n,
  });
});

test("decodeEvents() transfer", async (context) => {
  const { config, indexingFunctions } = getAccountsConfigAndIndexingFunctions({
    address: ALICE,
  });

  const app = await setupPonder(context, { config, indexingFunctions });

  const rawEvent = {
    checkpoint: ZERO_CHECKPOINT_STRING,
    network: app.indexingBuild.network,
    eventCallback: app.indexingBuild.eventCallbacks[3],
    block: {} as RawEvent["block"],
    transaction: {} as RawEvent["transaction"],
    log: undefined,
    trace: {
      type: "CALL",
      from: ALICE,
      to: BOB,
      gas: 0n,
      gasUsed: 0n,
      input: "0x0",
      output: "0x0",
      value: parseEther("1"),
      traceIndex: 0,
      subcalls: 0,
      blockNumber: 0,
      transactionIndex: 0,
    },
  } as RawEvent;

  const events = decodeEvents(common, sources, [rawEvent]) as [TransferEvent];

  expect(events).toHaveLength(1);
  expect(events[0].event.transfer).toMatchObject({
    from: ALICE,
    to: BOB,
    value: parseEther("1"),
  });
  expect(events[0].name).toBe("Accounts:transfer:from");
});

test("decodeEvents() transaction", async (context) => {
  const { config, indexingFunctions } = getAccountsConfigAndIndexingFunctions({
    address: ALICE,
  });

  const app = await setupPonder(context, { config, indexingFunctions });

  const rawEvent = {
    checkpoint: ZERO_CHECKPOINT_STRING,
    network: app.indexingBuild.network,
    eventCallback: app.indexingBuild.eventCallbacks[1],
    block: {} as RawEvent["block"],
    transaction: {} as RawEvent["transaction"],
    log: undefined,
    trace: undefined,
  } as RawEvent;

  const events = decodeEvents(app, { rawEvents: [rawEvent] }) as [
    TransferEvent,
  ];

  expect(events).toHaveLength(1);

  expect(events[0].name).toBe("Accounts:transaction:to");
});

test("decodeEvents() trace", async (context) => {
  const { config, indexingFunctions } = getErc20ConfigAndIndexingFunctions({
    address: zeroAddress,
    includeCallTraces: true,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const rawEvent = {
    checkpoint: ZERO_CHECKPOINT_STRING,
    network: app.indexingBuild.network,
    eventCallback: app.indexingBuild.eventCallbacks[1],
    block: {} as RawEvent["block"],
    transaction: {} as RawEvent["transaction"],
    log: undefined,
    trace: {
      type: "CALL",
      from: ALICE,
      to: BOB,
      input: encodeFunctionData({
        abi: erc20ABI,
        functionName: "transfer",
        args: [BOB, parseEther("1")],
      }),
      output: encodeFunctionResult({
        abi: erc20ABI,
        functionName: "transfer",
        result: true,
      }),
      gas: 0n,
      gasUsed: 0n,
      value: 0n,
      traceIndex: 0,
      subcalls: 0,
      blockNumber: 0,
      transactionIndex: 0,
    },
  } as RawEvent;

  const events = decodeEvents(app, { rawEvents: [rawEvent] }) as [TraceEvent];

  expect(events).toHaveLength(1);
  expect(events[0].event.args).toStrictEqual([BOB, parseEther("1")]);
  expect(events[0].event.result).toBe(true);
  expect(events[0].name).toBe("Erc20.transfer()");
});

test("decodeEvents() trace error", async (context) => {
  const { config, indexingFunctions } = getErc20ConfigAndIndexingFunctions({
    address: zeroAddress,
    includeCallTraces: true,
  });
  const app = await setupPonder(context, { config, indexingFunctions });

  const rawEvent = {
    checkpoint: ZERO_CHECKPOINT_STRING,
    network: app.indexingBuild.network,
    eventCallback: app.indexingBuild.eventCallbacks[1],
    block: {} as RawEvent["block"],
    transaction: {} as RawEvent["transaction"],
    log: undefined,
    trace: {
      type: "CALL",
      from: ALICE,
      to: BOB,
      input: "0x",
      output: encodeFunctionResult({
        abi: erc20ABI,
        functionName: "transfer",
        result: true,
      }),
      gas: 0n,
      gasUsed: 0n,
      value: 0n,
      traceIndex: 0,
      subcalls: 0,
      blockNumber: 0,
      transactionIndex: 0,
    },
  } as RawEvent;

  const events = decodeEvents(app, { rawEvents: [rawEvent] }) as [TraceEvent];

  expect(events).toHaveLength(0);
});

test("removeNullCharacters removes null characters", () => {
  // NameRegistered event from this transaction contains null characters:
  // https://etherscan.io/tx/0x2e67be22d5e700e61e102b926f28ba451c53a6cd6438c53b43dbb783c2081a12#eventlog
  const log = {
    topics: [
      "0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f",
      "0x56e1003dc29ff83445ba93c493f4a76570eb667494e78c6974a745593131ae2a",
      "0x0000000000000000000000008504a09352555ff1acf9c8a8d9fb5fdcc4161cbc",
    ],
    data: "0x0000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000697a5dd2a81dc000000000000000000000000000000000000000000000000000000006457430e000000000000000000000000000000000000000000000000000000000000001174656e63656e74636c7562000000000000000000000000000000000000000000",
  } as const;

  const abiItem = {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: "string",
        name: "name",
        type: "string",
      },
      {
        indexed: true,
        internalType: "bytes32",
        name: "label",
        type: "bytes32",
      },
      {
        indexed: true,
        internalType: "address",
        name: "owner",
        type: "address",
      },
      {
        indexed: false,
        internalType: "uint256",
        name: "cost",
        type: "uint256",
      },
      {
        indexed: false,
        internalType: "uint256",
        name: "expires",
        type: "uint256",
      },
    ],
    name: "NameRegistered",
    type: "event",
  } as const;

  const args = decodeEventLog({
    abiItem,
    topics: log.topics as unknown as [signature: Hex, ...args: Hex[]],
    data: log.data,
  });

  expect(args.name).toBe("tencentclub\x00\x00\x00\x00\x00\x00");

  const cleanedArgs = removeNullCharacters(args);

  expect((cleanedArgs as any).name).toBe("tencentclub");
});
