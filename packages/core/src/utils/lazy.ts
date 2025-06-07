import util from "node:util";
import { type Address, checksumAddress } from "viem";

/**
 * Lazy checksum address.
 *
 * @example
 * ```ts
 * const object = { address: "0x1234567890123456789012345678901234567890" };
 * lazyChecksumAddress(object, "address");
 * ```
 */
export const lazyChecksumAddress = <const T extends object>(
  object: T,
  key: T extends unknown[] ? number : keyof T,
): T => {
  // @ts-expect-error
  const address = object[key] as Address;

  Object.defineProperty(object, key, {
    get() {
      return checksumAddress(address);
    },
  });

  Object.assign(object, {
    [util.inspect.custom]: () => {
      return Object.fromEntries(
        Object.keys(object).map((k) =>
          // @ts-expect-error
          k === key ? [k, checksumAddress(address)] : [k, object[k]],
        ),
      );
    },
  });

  return object;
};

const cowProxies = new WeakSet<object>();

/**
 * Copy-on-write proxy.
 * @dev Objects are copied on read in order to avoid mutating inner properties.
 */
export const lazyCopy = <T extends object>(row: T): T => {
  if (cowProxies.has(row)) return row;

  let copied: T | undefined;
  const proxy = new Proxy(row, {
    get(target, prop) {
      if (prop === "then") return Reflect.get(target, prop);
      if (copied === undefined) copied = structuredClone(row);
      return Reflect.get(copied, prop);
    },
    set(_, prop, value) {
      if (copied === undefined) copied = structuredClone(row);
      return Reflect.set(copied, prop, value);
    },
  });

  cowProxies.add(proxy);
  return proxy;
};
