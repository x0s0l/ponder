import os from "node:os";
import readline from "node:readline";
import { ShutdownError } from "@/internal/errors.js";
import type { Logger } from "@/internal/logger.js";
import type { Shutdown } from "@/internal/shutdown.js";
import type { Telemetry } from "@/internal/telemetry.js";

const SHUTDOWN_GRACE_PERIOD_MS = 5_000;

/** Sets up shutdown handlers for the process. Accepts additional cleanup logic to run. */
export const createExit = ({
  logger,
  telemetry,
  shutdown,
}: { logger: Logger; telemetry: Telemetry; shutdown: Shutdown }) => {
  let isShuttingDown = false;

  const exit = async ({ reason, code }: { reason: string; code: 0 | 1 }) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    const timeout = setTimeout(async () => {
      logger.fatal({
        service: "process",
        msg: "Failed to shutdown within 5 seconds, terminating (exit code 1)",
      });
      process.exit(1);
    }, SHUTDOWN_GRACE_PERIOD_MS);

    if (reason !== undefined) {
      logger.warn({
        service: "process",
        msg: `${reason}, starting shutdown sequence`,
      });
    }
    telemetry.record({
      name: "lifecycle:session_end",
      properties: { duration_seconds: process.uptime() },
    });

    await shutdown.kill();
    clearTimeout(timeout);

    if (process.stdin.isTTY) {
      process.stdin.setRawMode(false);
      process.stdin.pause();
    }

    process.exit(code);
  };

  if (os.platform() === "win32") {
    const readlineInterface = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    readlineInterface.on("SIGINT", () =>
      exit({ reason: "Received SIGINT", code: 0 }),
    );
  }

  process.on("SIGINT", () => exit({ reason: "Received SIGINT", code: 0 }));
  process.on("SIGTERM", () => exit({ reason: "Received SIGTERM", code: 0 }));
  process.on("SIGQUIT", () => exit({ reason: "Received SIGQUIT", code: 0 }));
  process.on("uncaughtException", (error: Error) => {
    if (error instanceof ShutdownError) return;
    logger.error({
      service: "process",
      msg: "Caught uncaughtException event",
      error,
    });
    exit({ reason: "Received uncaughtException", code: 1 });
  });
  process.on("unhandledRejection", (error: Error) => {
    if (error instanceof ShutdownError) return;
    logger.error({
      service: "process",
      msg: "Caught unhandledRejection event",
      error,
    });
    exit({ reason: "Received unhandledRejection", code: 1 });
  });

  return exit;
};
