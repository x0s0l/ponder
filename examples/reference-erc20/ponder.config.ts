import { createConfig } from "ponder";
import { http } from "viem";
import { erc20ABI } from "./abis/erc20ABI";

export default createConfig({
  networks: {
    mainnet: {
      chainId: 1,
      transport: http(process.env.PONDER_RPC_URL_1),
      maxRequestsPerSecond: 200,
    },
  },
  contracts: {
    ERC20: {
      network: "mainnet",
      abi: erc20ABI,
      address: "0xae78736cd615f374d3085123a210448e74fc6393",
      startBlock: 13325304,
    },
  },
});
