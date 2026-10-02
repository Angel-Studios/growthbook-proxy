import logger from "./logger";

process.removeAllListeners("warning");
process.on("warning", (warning: Error) => {
  logger.warn(
    {
      warning: {
        name: warning.name,
        code: (warning as NodeJS.ErrnoException).code,
        stack: warning.stack,
      },
    },
    warning.message,
  );
});
