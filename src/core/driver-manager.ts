import fs from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";

import type { Dialect } from "../drivers/adapter";
import type { Renderer } from "../ui/renderer";

export type PackageManager = "npm" | "pnpm" | "yarn" | "bun";

export interface ParsedConnection {
  dialect: Dialect;
  connectionString: string;
}

export interface LoadedPackage<T = unknown> {
  module: T;
  require: typeof require;
  resolvedPath: string;
  location: "project" | "self" | "cache";
}

export interface LoadPackageOptions {
  cwd?: string;
  renderer?: Renderer;

  /**
   * The package name to install via the package manager.
   * Often equals the specifier, but may differ (e.g. specifier "mysql2/promise", installName "mysql2").
   */
  installName?: string;

  /**
   * Human label for UX ("postgres driver (pg)").
   */
  label?: string;

  /**
   * Force loading from the cache (skip project/self resolution).
   */
  forceCache?: boolean;
}

const CACHE_VERSION = 2;
const SELF_REQUIRE = createRequire(__filename);

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseConnectionString(input: string): ParsedConnection {
  const raw = (input ?? "").trim();
  const lower = raw.toLowerCase();

  if (lower.startsWith("postgres://") || lower.startsWith("postgresql://")) {
    return { dialect: "postgres", connectionString: raw };
  }
  if (lower.startsWith("mysql://")) {
    return { dialect: "mysql", connectionString: raw };
  }
  if (lower.startsWith("duckdb://") || lower.startsWith("duckdb:")) {
    return { dialect: "duckdb", connectionString: raw };
  }
  if (lower.startsWith("sqlite://") || lower.startsWith("sqlite:")) {
    return { dialect: "sqlite", connectionString: raw };
  }
  if (lower.startsWith("parquet:")) {
    // Parquet files are handled via DuckDB's read_parquet()
    return { dialect: "duckdb", connectionString: raw };
  }

  // Auto-detect by file extension
  if (lower.endsWith(".parquet") || lower.endsWith(".pq")) {
    return { dialect: "duckdb", connectionString: `parquet:${raw}` };
  }

  // Default: treat as SQLite file path
  return { dialect: "sqlite", connectionString: `sqlite:${raw}` };
}

export function getUsqlCacheRoot(): string {
  const override = (process.env.USQL_CACHE_DIR ?? "").trim();
  if (override) return path.resolve(override);

  const home = os.homedir();

  if (process.platform === "win32") {
    const base =
      process.env.LOCALAPPDATA ||
      process.env.APPDATA ||
      path.join(home, "AppData", "Local");
    return path.join(base, "usql");
  }

  const base = process.env.XDG_CACHE_HOME || path.join(home, ".cache");
  return path.join(base, "usql");
}

function cacheKeyForPackage(installName: string): string {
  const safe = installName.replace(/[@/\\:]/g, "_");
  const nodeMajor = (process.versions.node || "18").split(".")[0];
  return `${safe}-cv${CACHE_VERSION}-${process.platform}-${process.arch}-node${nodeMajor}`;
}

function getInstallDir(installName: string): string {
  return path.join(getUsqlCacheRoot(), "drivers", cacheKeyForPackage(installName));
}

async function ensureCacheProject(dir: string): Promise<void> {
  await fs.mkdir(dir, { recursive: true });

  const pkgJsonPath = path.join(dir, "package.json");
  if (!existsSync(pkgJsonPath)) {
    const pkg = {
      name: "usql-driver-cache",
      private: true,
      description: "Auto-installed database drivers for usql",
      license: "UNLICENSED"
    };
    await fs.writeFile(pkgJsonPath, JSON.stringify(pkg, null, 2), "utf8");
  }
}

function pmToCommand(pm: PackageManager): string {
  return pm;
}

function isCommandAvailable(pm: PackageManager): boolean {
  const cmd = pmToCommand(pm);
  const res = spawnSync(cmd, ["--version"], { stdio: "ignore" });
  return res.status === 0;
}

function normalizePm(s: string): PackageManager | null {
  const v = s.trim().toLowerCase();
  if (v === "npm") return "npm";
  if (v === "pnpm") return "pnpm";
  if (v === "yarn") return "yarn";
  if (v === "bun") return "bun";
  return null;
}

export function detectPackageManager(cwd: string = process.cwd()): PackageManager {
  const forced = normalizePm(process.env.USQL_PM || "");
  if (forced) {
    if (isCommandAvailable(forced)) return forced;
    // Forced but missing -> fall back to npm if possible.
    if (isCommandAvailable("npm")) return "npm";
  }

  const ua = (process.env.npm_config_user_agent || "").toLowerCase();
  let preferred: PackageManager | null = null;

  if (ua.includes("bun")) preferred = "bun";
  else if (ua.includes("pnpm")) preferred = "pnpm";
  else if (ua.includes("yarn")) preferred = "yarn";
  else if (ua.includes("npm")) preferred = "npm";

  if (!preferred) {
    if (existsSync(path.join(cwd, "bun.lockb")) || existsSync(path.join(cwd, "bun.lock"))) {
      preferred = "bun";
    } else if (existsSync(path.join(cwd, "pnpm-lock.yaml"))) {
      preferred = "pnpm";
    } else if (existsSync(path.join(cwd, "yarn.lock"))) {
      preferred = "yarn";
    } else {
      preferred = "npm";
    }
  }

  if (preferred !== "npm" && !isCommandAvailable(preferred)) {
    if (isCommandAvailable("npm")) return "npm";
  }

  return preferred;
}

async function withInstallLock<T>(
  dir: string,
  fn: () => Promise<T>,
  renderer?: Renderer
): Promise<T> {
  const lockPath = path.join(dir, ".install.lock");
  const start = Date.now();
  const timeoutMs = 2 * 60 * 1000;

  while (true) {
    try {
      const handle = await fs.open(lockPath, "wx");
      try {
        const payload = {
          pid: process.pid,
          startedAt: new Date().toISOString()
        };
        await handle.writeFile(JSON.stringify(payload), { encoding: "utf8" }).catch(() => undefined);
        return await fn();
      } finally {
        await handle.close();
        await fs.unlink(lockPath).catch(() => undefined);
      }
    } catch (err: unknown) {
      const errObj = err as { code?: string };
      if (errObj && errObj.code === "EEXIST") {
        if (Date.now() - start > timeoutMs) {
          renderer?.error(
            `x timed out waiting for another usql process to finish installing drivers (${path.basename(dir)})`
          );
          throw new Error("Timed out waiting for driver install lock");
        }
        await sleep(200);
        continue;
      }
      throw err;
    }
  }
}

async function spawnAndCapture(
  cmd: string,
  args: string[],
  cwd: string,
  env: NodeJS.ProcessEnv
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
      shell: true
    });

    let stderr = "";
    let stdout = "";

    child.stdout?.on("data", (d) => {
      stdout += String(d);
      if (stdout.length > 32_000) stdout = stdout.slice(-32_000);
    });

    child.stderr?.on("data", (d) => {
      stderr += String(d);
      if (stderr.length > 64_000) stderr = stderr.slice(-64_000);
    });

    child.on("error", (e) => reject(e));

    child.on("close", (code) => {
      if (code === 0) return resolve();

      const tail = (stderr || stdout || "").trim().slice(-4000);
      reject(
        new Error(
          `Command failed (${code}): ${cmd} ${args.join(" ")}${tail ? `\n\n${tail}\n` : ""}`
        )
      );
    });
  });
}

async function installPackageIntoCache(
  installName: string,
  cacheDir: string,
  pm: PackageManager,
  renderer?: Renderer,
  label?: string
): Promise<void> {
  await ensureCacheProject(cacheDir);

  const cmd = pmToCommand(pm);

  // Install into cache project scope. We *want* it written to cache package.json.
  // npm: keep package.json untouched (no-save) to reduce churn.
  const args =
    pm === "npm"
      ? ["install", installName, "--no-save"]
      : pm === "yarn"
        ? ["add", installName]
        : pm === "pnpm"
          ? ["add", installName]
          : ["add", installName];

  const display = label || installName;

  const spin = renderer?.spinner
    ? renderer.spinner(`Installing ${display}...`)
    : null;

  try {
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      npm_config_fund: "false",
      npm_config_audit: "false",
      npm_config_update_notifier: "false",
      npm_config_loglevel: "error"
    };

    // Keep install logs quiet; we capture them and only show on error.
    await spawnAndCapture(cmd, args, cacheDir, env);

    spin?.stop(true, "ready");
  } catch (e) {
    spin?.stop(false, "failed");
    throw e;
  }
}

function createProjectRequire(cwd: string): typeof require {
  // createRequire only needs an absolute path; the file doesn't have to exist.
  return createRequire(path.join(cwd, "__usql__.cjs"));
}

function resolveOrNull(req: typeof require, specifier: string): string | null {
  try {
    return req.resolve(specifier);
  } catch {
    return null;
  }
}

export async function loadPackage<T = unknown>(
  specifier: string,
  opts: LoadPackageOptions = {}
): Promise<LoadedPackage<T>> {
  const cwd = opts.cwd ?? process.cwd();
  const installName = opts.installName ?? specifier;
  const label = opts.label ?? installName;

  const projectReq = createProjectRequire(cwd);
  const selfReq = SELF_REQUIRE;

  const cacheDir = getInstallDir(installName);
  await ensureCacheProject(cacheDir);

  const cacheReq = createRequire(path.join(cacheDir, "package.json"));

  const candidates: Array<{ location: LoadedPackage["location"]; req: typeof require }> = [];

  if (!opts.forceCache) {
    candidates.push({ location: "project", req: projectReq });
    candidates.push({ location: "self", req: selfReq });
  }
  candidates.push({ location: "cache", req: cacheReq });

  // Fast path: already resolvable somewhere.
  for (const c of candidates) {
    const resolved = resolveOrNull(c.req, specifier);
    if (!resolved) continue;

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const mod = c.req(specifier) as T;
    return { module: mod, require: c.req, resolvedPath: resolved, location: c.location };
  }

  // Not found -> install into cache, then load.
  await withInstallLock(
    cacheDir,
    async () => {
      // Another process may have installed it while we were waiting.
      if (resolveOrNull(cacheReq, specifier)) return;

      const pm = detectPackageManager(cwd);
      await installPackageIntoCache(installName, cacheDir, pm, opts.renderer, label);
    },
    opts.renderer
  );

  const resolved = resolveOrNull(cacheReq, specifier);
  if (!resolved) {
    throw new Error(`Failed to resolve "${specifier}" after installing "${installName}" into cache.`);
  }

  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
  const mod = cacheReq(specifier) as T;
  return { module: mod, require: cacheReq, resolvedPath: resolved, location: "cache" };
}
