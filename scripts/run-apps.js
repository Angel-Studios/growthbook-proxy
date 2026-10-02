#!/usr/bin/env node
"use strict";

const { spawn } = require("node:child_process");
const path = require("node:path");

const KILL_TIMEOUT_MS = (() => {
  const configured = Number(
    process.env.APP_KILL_TIMEOUT_MS || process.env.PM2_KILL_TIMEOUT,
  );
  if (configured) return configured;
  return (Number(process.env.SHUTDOWN_DELAY_MS) || 0) + 5000;
})();

function writeJson(level, message) {
  process.stdout.write(
    JSON.stringify({ level, time: Date.now(), msg: message }) + "\n",
  );
}

function forwardLine(line, level) {
  if (!line.trim()) return;
  try {
    JSON.parse(line);
    process.stdout.write(line + "\n");
  } catch {
    writeJson(level, line);
  }
}

function forward(stream, level) {
  let buffer = "";
  stream.setEncoding("utf8");
  stream.on("error", (err) =>
    writeJson(40, `child stream error: ${err.message}`),
  );
  stream.on("data", (chunk) => {
    buffer += chunk;
    let index;
    while ((index = buffer.indexOf("\n")) !== -1) {
      forwardLine(buffer.slice(0, index).replace(/\r$/, ""), level);
      buffer = buffer.slice(index + 1);
    }
  });
  stream.on("end", () => {
    forwardLine(buffer, level);
    buffer = "";
  });
}

function parseArgs(argv) {
  const parsed = { script: null, requireModule: null, cwd: null };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "start") continue;
    if (arg === "--require") parsed.requireModule = argv[++i];
    else if (arg === "--cwd") parsed.cwd = argv[++i];
    else if (!arg.startsWith("-") && !parsed.script) parsed.script = arg;
  }
  return parsed;
}

const { script, requireModule, cwd } = parseArgs(process.argv.slice(2));
if (!script) {
  writeJson(
    50,
    "usage: run-apps.js start <script> [--require <module>] [--cwd <dir>]",
  );
  process.exit(1);
}

const nodeArgs = [];
if (requireModule) nodeArgs.push("--require", requireModule);

let shuttingDown = false;

const child = spawn(process.execPath, [...nodeArgs, script], {
  cwd: cwd ? path.resolve(cwd) : process.cwd(),
  stdio: ["inherit", "pipe", "pipe"],
});

forward(child.stdout, 30);
forward(child.stderr, 40);
writeJson(30, `started ${script} (pid ${child.pid})`);

child.on("error", (err) => {
  writeJson(50, `failed to start ${script}: ${err.message}`);
  process.exit(1);
});
child.on("exit", (code, signal) => {
  writeJson(30, `${script} exited (code ${code}, signal ${signal})`);
  process.exit(shuttingDown ? 0 : (code ?? 0));
});

function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  writeJson(30, `${signal} received, stopping ${script}`);
  child.kill(signal);
  setTimeout(() => child.kill("SIGKILL"), KILL_TIMEOUT_MS).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
