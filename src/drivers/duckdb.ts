import fs from "node:fs/promises";
import { createReadStream, existsSync, type Stats } from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { StringDecoder } from "node:string_decoder";

import { Adapter, QueryResult } from "./adapter";
import { loadPackage, getUsqlCacheRoot } from "../core/driver-manager";
import type { Renderer } from "../ui/renderer";

interface ParsedDuckDbConn {
  dbPath: string;
  parquetFile?: string;
  sqlDumpFile?: string;
}

function parseDuckDbConnection(connectionString: string): ParsedDuckDbConn {
  const raw = connectionString.trim();
  const lower = raw.toLowerCase();

  // Parquet file: parquet:./data.parquet
  if (lower.startsWith("parquet:")) {
    const parquetPath = raw.slice("parquet:".length);
    return { dbPath: ":memory:", parquetFile: parquetPath };
  }

  // SQL dump file: sqldump:./dump.sql
  if (lower.startsWith("sqldump:")) {
    const sqlPath = raw.slice("sqldump:".length);
    return { dbPath: ":memory:", sqlDumpFile: sqlPath };
  }

  // DuckDB database file
  let s = raw;
  if (lower.startsWith("duckdb:")) s = raw.slice("duckdb:".length);
  if (s.startsWith("//")) s = s.slice(2);
  if (!s || s === ":memory:" || s === "memory") return { dbPath: ":memory:" };
  return { dbPath: s };
}

function isProbablySelect(sql: string): boolean {
  const s = sql.trim().toLowerCase();
  return (
    s.startsWith("select") ||
    s.startsWith("with") ||
    s.startsWith("pragma") ||
    s.startsWith("explain") ||
    s.startsWith("describe") ||
    s.startsWith("show")
  );
}

type DuckDbConnectionLike = {
  all(
    sql: string,
    params: unknown[] | undefined,
    cb: (err: Error | null, rows?: Array<Record<string, unknown>>) => void
  ): void;
  run(sql: string, params: unknown[] | undefined, cb: (err: Error | null) => void): void;
  close?: (cb: (err?: Error | null) => void) => void;
};

type DuckDbDatabaseLike = {
  connect(): DuckDbConnectionLike;
  close?: (cb: (err?: Error | null) => void) => void;
};

type DuckDbModuleLike = {
  Database: new (file: string) => DuckDbDatabaseLike;
};

interface FailedTable {
  name: string;
  error: string;
  stmt: string;
}

/**
 * Cache meta for SQL dump -> DuckDB file.
 * We keep it tiny and robust (so corrupted meta won't crash us).
 */
interface SqlDumpCacheMeta {
  v: number;
  builtAt: string;
  source: {
    path: string;
    size: number;
    mtimeMs: number;
  };
  tables: number;
  rows: number;
  failedTables: number;
}

const SQLDUMP_CACHE_VERSION = 3;

/** Default: >= 50MB considered "large" and will show progress. */
const LARGE_FILE_BYTES = 50 * 1024 * 1024;

function sleep(ms: number): Promise<void> {
  return new Promise<void>((r) => setTimeout(r, ms));
}

function isWhitespaceCharCode(cc: number): boolean {
  // space, tab, cr, lf, vt, ff
  return cc === 0x20 || cc === 0x09 || cc === 0x0d || cc === 0x0a || cc === 0x0b || cc === 0x0c;
}

function stripBom(s: string): string {
  if (!s) return s;
  return s.charCodeAt(0) === 0xfeff ? s.slice(1) : s;
}

function formatInt(n: number): string {
  const sign = n < 0 ? "-" : "";
  const s = Math.abs(Math.trunc(n)).toString();
  const out: string[] = [];
  for (let i = s.length; i > 0; i -= 3) {
    const start = Math.max(0, i - 3);
    out.push(s.slice(start, i));
  }
  return sign + out.reverse().join(",");
}

function safeSnippet(sql: string, max: number = 400): string {
  const s = sql.replace(/\s+/g, " ").trim();
  if (s.length <= max) return s;
  return s.slice(0, Math.max(0, max - 3)) + "...";
}

function escapeDuckDbStringLiteral(s: string): string {
  // DuckDB accepts forward slashes on Windows; avoids backslash-escape ambiguity.
  return s.replace(/\\/g, "/").replace(/'/g, "''");
}

function quoteDuckDbIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function parseEnvInt(name: string, def: number): number {
  const raw = (process.env[name] ?? "").trim();
  if (!raw) return def;
  const n = Number(raw);
  if (!Number.isFinite(n)) return def;
  return Math.floor(n);
}

function parseEnvBool(name: string, def: boolean): boolean {
  const raw = (process.env[name] ?? "").trim().toLowerCase();
  if (!raw) return def;
  if (raw === "1" || raw === "true" || raw === "yes" || raw === "y") return true;
  if (raw === "0" || raw === "false" || raw === "no" || raw === "n") return false;
  return def;
}

/**
 * Cache directory policy:
 * - If user sets USQL_SQLDUMP_CACHE_DIR -> use it.
 * - Else if user sets USQL_CACHE_DIR -> store under getUsqlCacheRoot()/sqldump-cache (explicit user intent).
 * - Else -> OS temp dir (often auto-cleaned) under <tmp>/usql/sqldump-cache.
 */
function getSqlDumpCacheDir(): string {
  const override = (process.env.USQL_SQLDUMP_CACHE_DIR ?? "").trim();
  if (override) return path.resolve(override);

  if ((process.env.USQL_CACHE_DIR ?? "").trim()) {
    return path.join(getUsqlCacheRoot(), "sqldump-cache");
  }

  return path.join(os.tmpdir(), "usql", "sqldump-cache");
}

function cacheKeyForSqlDump(absPath: string, st: Stats): string {
  // Include cache version so future schema/escape improvements invalidate old caches.
  const h = crypto
    .createHash("md5")
    .update(`${absPath}\n${st.size}\n${st.mtimeMs}\nv${SQLDUMP_CACHE_VERSION}`)
    .digest("hex");
  return h.slice(0, 16);
}

function cachePathsForSqlDump(absPath: string, st: Stats): {
  cacheDir: string;
  cachePath: string;
  metaPath: string;
  lockPath: string;
} {
  const cacheDir = getSqlDumpCacheDir();

  const base = path.basename(absPath);
  const baseNoExt = base.toLowerCase().endsWith(".sql") ? base.slice(0, -4) : base;

  // Keep filename stable but safe-ish: avoid path separators.
  const safeBase = baseNoExt.replace(/[\\/:*?"<>|]+/g, "_");

  const key = cacheKeyForSqlDump(absPath, st);
  const cachePath = path.join(cacheDir, `${safeBase}_${key}.duckdb`);
  const metaPath = `${cachePath}.meta.json`;
  const lockPath = `${cachePath}.lock`;

  return { cacheDir, cachePath, metaPath, lockPath };
}

async function readSqlDumpCacheMeta(metaPath: string): Promise<SqlDumpCacheMeta | null> {
  try {
    const raw = await fs.readFile(metaPath, "utf8");
    const parsed: unknown = JSON.parse(raw);

    if (!parsed || typeof parsed !== "object") return null;
    const obj = parsed as Record<string, unknown>;

    const v = obj["v"];
    const builtAt = obj["builtAt"];
    const source = obj["source"];
    const tables = obj["tables"];
    const rows = obj["rows"];
    const failedTables = obj["failedTables"];

    if (typeof v !== "number") return null;
    if (typeof builtAt !== "string") return null;
    if (!source || typeof source !== "object") return null;
    if (typeof tables !== "number") return null;
    if (typeof rows !== "number") return null;
    if (typeof failedTables !== "number") return null;

    const s = source as Record<string, unknown>;
    const sp = s["path"];
    const ss = s["size"];
    const sm = s["mtimeMs"];
    if (typeof sp !== "string" || typeof ss !== "number" || typeof sm !== "number") return null;

    return {
      v,
      builtAt,
      source: { path: sp, size: ss, mtimeMs: sm },
      tables,
      rows,
      failedTables
    };
  } catch {
    return null;
  }
}

async function writeSqlDumpCacheMeta(metaPath: string, meta: SqlDumpCacheMeta): Promise<void> {
  const tmp = `${metaPath}.tmp.${process.pid}.${Math.random().toString(16).slice(2)}`;
  await fs.writeFile(tmp, JSON.stringify(meta), "utf8");
  await fs.rename(tmp, metaPath).catch(async (e) => {
    await fs.unlink(tmp).catch(() => undefined);
    throw e;
  });
}

async function isSqlDumpCacheValid(cachePath: string, metaPath: string): Promise<SqlDumpCacheMeta | null> {
  if (!existsSync(cachePath) || !existsSync(metaPath)) return null;
  const meta = await readSqlDumpCacheMeta(metaPath);
  if (!meta) return null;
  if (meta.v !== SQLDUMP_CACHE_VERSION) return null;
  return meta;
}

/**
 * Cache cleanup:
 * - TTL: delete anything older than USQL_SQLDUMP_CACHE_MAX_DAYS (default 7)
 * - Keep at most USQL_SQLDUMP_CACHE_MAX_FILES (default 5)
 * - Keep at most USQL_SQLDUMP_CACHE_MAX_BYTES (default 5GB)
 */
async function cleanSqlDumpCache(cacheDir: string): Promise<void> {
  const maxDays = parseEnvInt("USQL_SQLDUMP_CACHE_MAX_DAYS", 7);
  const maxFiles = parseEnvInt("USQL_SQLDUMP_CACHE_MAX_FILES", 5);
  const maxBytes = parseEnvInt("USQL_SQLDUMP_CACHE_MAX_BYTES", 5 * 1024 * 1024 * 1024);

  try {
    if (!existsSync(cacheDir)) return;

    const names = await fs.readdir(cacheDir);
    const duckdbFiles = names.filter((n: string) => n.toLowerCase().endsWith(".duckdb"));
    if (!duckdbFiles.length) return;

    const items = await Promise.all(
      duckdbFiles.map(async (n: string) => {
        const p = path.join(cacheDir, n);
        try {
          const st = await fs.stat(p);
          return { name: n, path: p, size: st.size, mtimeMs: st.mtimeMs };
        } catch {
          return null;
        }
      })
    );

    const files = items.filter((x): x is NonNullable<typeof x> => x !== null);
    if (!files.length) return;

    // Sort newest first
    files.sort((a, b) => b.mtimeMs - a.mtimeMs);

    const now = Date.now();
    const ttlMs = maxDays * 24 * 60 * 60 * 1000;

    const removeFile = async (filePath: string): Promise<void> => {
      await fs.unlink(filePath).catch(() => undefined);
      await fs.unlink(`${filePath}.meta.json`).catch(() => undefined);
      await fs.unlink(`${filePath}.lock`).catch(() => undefined);
    };

    // TTL cleanup (only delete if older than TTL)
    for (const f of files) {
      if (now - f.mtimeMs > ttlMs) {
        await removeFile(f.path);
      }
    }

    // Re-list remaining after TTL cleanup
    const names2 = await fs.readdir(cacheDir);
    const duck2 = names2.filter((n: string) => n.toLowerCase().endsWith(".duckdb"));
    const items2 = await Promise.all(
      duck2.map(async (n: string) => {
        const p = path.join(cacheDir, n);
        try {
          const st = await fs.stat(p);
          return { name: n, path: p, size: st.size, mtimeMs: st.mtimeMs };
        } catch {
          return null;
        }
      })
    );
    const files2 = items2.filter((x): x is NonNullable<typeof x> => x !== null);
    files2.sort((a, b) => b.mtimeMs - a.mtimeMs);

    // Enforce max files (delete oldest beyond maxFiles)
    for (let i = maxFiles; i < files2.length; i++) {
      await removeFile(files2[i].path);
    }

    // Enforce max bytes (delete oldest among kept)
    const kept = files2.slice(0, Math.min(maxFiles, files2.length));
    let total = kept.reduce((acc, f) => acc + f.size, 0);

    for (let i = kept.length - 1; i >= 0 && total > maxBytes; i--) {
      const f = kept[i];
      await removeFile(f.path);
      total -= f.size;
    }
  } catch {
    // never crash on cache cleanup
  }
}

/**
 * Cross-process install/build lock.
 * Uses an advisory lock file in cache dir. Robust to crashes via stale lock cleanup.
 */
async function withFileLock<T>(
  lockPath: string,
  fn: () => Promise<T>,
  renderer?: Renderer
): Promise<T> {
  const timeoutMs = parseEnvInt("USQL_SQLDUMP_CACHE_LOCK_TIMEOUT_MS", 30 * 60 * 1000);
  const start = Date.now();

  while (true) {
    try {
      const handle = await fs.open(lockPath, "wx");
      try {
        await handle
          .writeFile(
            JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }),
            { encoding: "utf8" }
          )
          .catch(() => undefined);

        return await fn();
      } finally {
        await handle.close().catch(() => undefined);
        await fs.unlink(lockPath).catch(() => undefined);
      }
    } catch (e: unknown) {
      const err = e as { code?: string };
      if (err && err.code === "EEXIST") {
        // stale lock cleanup
        if (Date.now() - start > timeoutMs) {
          try {
            const st = await fs.stat(lockPath);
            if (Date.now() - st.mtimeMs > timeoutMs) {
              await fs.unlink(lockPath).catch(() => undefined);
              continue;
            }
          } catch {
            // ignore
          }

          renderer?.warn?.("  warning: timed out waiting for sql dump cache lock, continuing...");
          // best-effort: continue without lock (rare)
          return await fn();
        }

        await sleep(200);
        continue;
      }
      throw e;
    }
  }
}

/**
 * Identifier token with raw span (keeps quoting).
 */
interface IdentifierToken {
  raw: string;
  value: string;
  start: number;
  end: number;
}

function readMySqlIdentifierToken(input: string, startIndex: number): IdentifierToken {
  const len = input.length;
  let i = startIndex;

  while (i < len && isWhitespaceCharCode(input.charCodeAt(i))) i++;

  const start = i;
  if (i >= len) return { raw: "", value: "", start, end: start };

  const ch = input[i];

  // Backtick-quoted identifier
  if (ch === "`") {
    i++;
    let out = "";
    while (i < len) {
      const c = input[i];
      if (c === "`") {
        // Escaped backtick: `` -> `
        if (i + 1 < len && input[i + 1] === "`") {
          out += "`";
          i += 2;
          continue;
        }
        i++;
        break;
      }
      out += c;
      i++;
    }
    const end = i;
    const raw = input.slice(start, end);
    return { raw, value: out, start, end };
  }

  // Double-quoted identifier (ANSI_QUOTES / already normalized)
  if (ch === "\"") {
    i++;
    let out = "";
    while (i < len) {
      const c = input[i];
      if (c === "\"") {
        // Escaped double quote: "" -> "
        if (i + 1 < len && input[i + 1] === "\"") {
          out += "\"";
          i += 2;
          continue;
        }
        i++;
        break;
      }
      out += c;
      i++;
    }
    const end = i;
    const raw = input.slice(start, end);
    return { raw, value: out, start, end };
  }

  // Unquoted identifier: letters/digits/_/$
  let out = "";
  while (i < len) {
    const cc = input.charCodeAt(i);
    const isAZ = (cc >= 0x41 && cc <= 0x5a) || (cc >= 0x61 && cc <= 0x7a);
    const is09 = cc >= 0x30 && cc <= 0x39;
    const isUnderscore = cc === 0x5f;
    const isDollar = cc === 0x24;
    if (!isAZ && !is09 && !isUnderscore && !isDollar) break;

    out += input[i];
    i++;
  }

  const end = i;
  const raw = input.slice(start, end);
  return { raw, value: out, start, end };
}

/**
 * For `db`.`table` or db.table, return the last identifier (table).
 * Input is a slice starting at identifier.
 */
function stripDbQualifier(namePart: string): { raw: string; value: string } {
  const first = readMySqlIdentifierToken(namePart, 0);
  if (!first.value) return { raw: "", value: "" };

  let i = first.end;
  while (i < namePart.length && isWhitespaceCharCode(namePart.charCodeAt(i))) i++;

  if (i >= namePart.length || namePart[i] !== ".") {
    return { raw: first.raw, value: first.value };
  }

  i++;
  const second = readMySqlIdentifierToken(namePart, i);
  if (!second.value) return { raw: first.raw, value: first.value };
  return { raw: second.raw, value: second.value };
}

function splitTopLevelComma(input: string): string[] {
  const parts: string[] = [];
  let start = 0;

  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let escape = false;

  for (let i = 0; i < input.length; i++) {
    const ch = input[i];

    if (inSingle) {
      if (escape) {
        escape = false;
        continue;
      }
      if (ch === "\\") {
        escape = true;
        continue;
      }
      // SQL standard '' escape
      if (ch === "'" && i + 1 < input.length && input[i + 1] === "'") {
        i++;
        continue;
      }
      if (ch === "'") inSingle = false;
      continue;
    }

    if (inDouble) {
      // handle "" inside identifiers
      if (ch === "\"" && i + 1 < input.length && input[i + 1] === "\"") {
        i++;
        continue;
      }
      if (ch === "\"") inDouble = false;
      continue;
    }

    if (inBacktick) {
      if (ch === "`" && i + 1 < input.length && input[i + 1] === "`") {
        i++;
        continue;
      }
      if (ch === "`") inBacktick = false;
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === "\"") {
      inDouble = true;
      continue;
    }
    if (ch === "`") {
      inBacktick = true;
      continue;
    }

    if (ch === "(") {
      depth++;
      continue;
    }
    if (ch === ")") {
      if (depth > 0) depth--;
      continue;
    }

    if (ch === "," && depth === 0) {
      parts.push(input.slice(start, i));
      start = i + 1;
    }
  }

  parts.push(input.slice(start));
  return parts.map((s) => s.trim()).filter((s) => s.length > 0);
}

function findTopLevelParenBlock(sql: string, openParenIndex: number): number {
  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let escape = false;

  for (let i = openParenIndex; i < sql.length; i++) {
    const ch = sql[i];

    if (inSingle) {
      if (escape) {
        escape = false;
        continue;
      }
      if (ch === "\\") {
        escape = true;
        continue;
      }
      if (ch === "'" && i + 1 < sql.length && sql[i + 1] === "'") {
        i++;
        continue;
      }
      if (ch === "'") inSingle = false;
      continue;
    }

    if (inDouble) {
      if (ch === "\"" && i + 1 < sql.length && sql[i + 1] === "\"") {
        i++;
        continue;
      }
      if (ch === "\"") inDouble = false;
      continue;
    }

    if (inBacktick) {
      if (ch === "`" && i + 1 < sql.length && sql[i + 1] === "`") {
        i++;
        continue;
      }
      if (ch === "`") inBacktick = false;
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === "\"") {
      inDouble = true;
      continue;
    }
    if (ch === "`") {
      inBacktick = true;
      continue;
    }

    if (ch === "(") {
      depth++;
      continue;
    }
    if (ch === ")") {
      depth--;
      if (depth === 0) return i;
      continue;
    }
  }

  return -1;
}

type HexStrategy = "from_hex" | "x_literal" | "unmodified";

/**
 * Detect best hex literal strategy supported by current DuckDB build.
 */
async function detectHexStrategy(conn: DuckDbConnectionLike): Promise<HexStrategy> {
  // prefer from_hex() if exists
  try {
    await allAsyncConn(conn, "SELECT from_hex('00') AS v;");
    return "from_hex";
  } catch {
    // ignore
  }

  // try X'00' literal
  try {
    await allAsyncConn(conn, "SELECT X'00' AS v;");
    return "x_literal";
  } catch {
    // ignore
  }

  return "unmodified";
}

/**
 * Convert MySQL SQL statement into DuckDB-compatible SQL.
 * Handles:
 * - backtick identifiers -> double quotes
 * - MySQL string escapes -> SQL standard string ('' escaping)
 * - 0x.. / x'..' hex literals -> duckdb blob
 * - b'0101' bit literals -> integer
 */
function convertMySqlStatementToDuckDbSql(stmt: string, opts: { hexStrategy: HexStrategy }): string {
  const hexStrategy = opts.hexStrategy;

  let out = "";

  let inSingle = false;
  let inBacktickIdent = false;

  for (let i = 0; i < stmt.length; i++) {
    const ch = stmt[i];

    // Inside backtick identifier
    if (inBacktickIdent) {
      if (ch === "`") {
        // escaped backtick
        if (i + 1 < stmt.length && stmt[i + 1] === "`") {
          out += "`";
          i++;
          continue;
        }
        out += "\"";
        inBacktickIdent = false;
        continue;
      }
      if (ch === "\"") {
        out += "\"\"";
        continue;
      }
      out += ch;
      continue;
    }

    // Inside single-quoted string
    if (inSingle) {
      if (ch === "'") {
        // SQL standard: '' inside string
        if (i + 1 < stmt.length && stmt[i + 1] === "'") {
          out += "''";
          i++;
          continue;
        }
        out += "'";
        inSingle = false;
        continue;
      }

      if (ch === "\\") {
        // MySQL escapes
        const nxt = i + 1 < stmt.length ? stmt[i + 1] : "";
        i++;

        switch (nxt) {
          case "0":
            // NUL byte: drop (safe, avoids driver/db issues)
            break;
          case "b":
            out += "\b";
            break;
          case "n":
            out += "\n";
            break;
          case "r":
            out += "\r";
            break;
          case "t":
            out += "\t";
            break;
          case "Z":
            out += "\u001a";
            break;
          case "'":
            out += "''";
            break;
          case "\"":
            out += "\"";
            break;
          case "\\":
            out += "\\";
            break;
          default:
            // keep literal char
            out += nxt;
            break;
        }

        continue;
      }

      // normal char in string; escape single quote if present (shouldn't happen here)
      if (ch === "'") {
        out += "''";
      } else {
        out += ch;
      }
      continue;
    }

    // Outside strings/idents
    if (ch === "'") {
      out += "'";
      inSingle = true;
      continue;
    }

    if (ch === "`") {
      out += "\"";
      inBacktickIdent = true;
      continue;
    }

    // Bit literal: b'0101'
    if ((ch === "b" || ch === "B") && i + 1 < stmt.length && stmt[i + 1] === "'") {
      let j = i + 2;
      let val = 0;
      let ok = true;
      while (j < stmt.length) {
        const c = stmt[j];
        if (c === "'") break;
        if (c !== "0" && c !== "1") {
          ok = false;
          break;
        }
        val = val * 2 + (c === "1" ? 1 : 0);
        j++;
      }
      if (ok && j < stmt.length && stmt[j] === "'") {
        out += String(val);
        i = j; // skip to closing quote
        continue;
      }
      // fallback: output as-is
    }

    // Hex literal: 0xABCD
    if (ch === "0" && i + 2 < stmt.length && (stmt[i + 1] === "x" || stmt[i + 1] === "X")) {
      let j = i + 2;
      while (j < stmt.length) {
        const cc = stmt.charCodeAt(j);
        const is09 = cc >= 0x30 && cc <= 0x39;
        const isAF = cc >= 0x41 && cc <= 0x46;
        const isaf = cc >= 0x61 && cc <= 0x66;
        if (!is09 && !isAF && !isaf) break;
        j++;
      }

      if (j > i + 2) {
        const hex = stmt.slice(i + 2, j);
        if (hexStrategy === "from_hex") {
          out += `from_hex('${hex}')`;
        } else if (hexStrategy === "x_literal") {
          out += `X'${hex}'`;
        } else {
          out += `0x${hex}`;
        }
        i = j - 1;
        continue;
      }
    }

    // Hex literal: X'ABCD' or x'ABCD'
    if ((ch === "x" || ch === "X") && i + 1 < stmt.length && stmt[i + 1] === "'") {
      let j = i + 2;
      let ok = true;
      while (j < stmt.length) {
        const c = stmt[j];
        if (c === "'") break;
        const cc = stmt.charCodeAt(j);
        const is09 = cc >= 0x30 && cc <= 0x39;
        const isAF = cc >= 0x41 && cc <= 0x46;
        const isaf = cc >= 0x61 && cc <= 0x66;
        if (!is09 && !isAF && !isaf) {
          ok = false;
          break;
        }
        j++;
      }
      if (ok && j < stmt.length && stmt[j] === "'") {
        const hex = stmt.slice(i + 2, j);
        if (hexStrategy === "from_hex") {
          out += `from_hex('${hex}')`;
        } else if (hexStrategy === "x_literal") {
          out += `X'${hex}'`;
        } else {
          out += `${ch}'${hex}'`;
        }
        i = j;
        continue;
      }
    }

    out += ch;
  }

  return out;
}

/**
 * Parse INSERT row count from a VALUES list.
 * Accurate for INSERT ... VALUES (...),(...),... ;  Fallback to 1 when unknown.
 */
function parseInsertRowCount(stmt: string): number {
  const lower = stmt.toLowerCase();
  const valuesIdx = lower.indexOf("values");
  if (valuesIdx < 0) return 1;

  let i = valuesIdx + "values".length;
  const len = stmt.length;

  // find first '('
  while (i < len && stmt[i] !== "(") i++;
  if (i >= len) return 1;

  let rows = 0;
  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let escape = false;

  for (; i < len; i++) {
    const ch = stmt[i];

    if (inSingle) {
      if (escape) {
        escape = false;
        continue;
      }
      if (ch === "\\") {
        escape = true;
        continue;
      }
      if (ch === "'" && i + 1 < len && stmt[i + 1] === "'") {
        i++;
        continue;
      }
      if (ch === "'") inSingle = false;
      continue;
    }

    if (inDouble) {
      if (ch === "\"" && i + 1 < len && stmt[i + 1] === "\"") {
        i++;
        continue;
      }
      if (ch === "\"") inDouble = false;
      continue;
    }

    if (inBacktick) {
      if (ch === "`" && i + 1 < len && stmt[i + 1] === "`") {
        i++;
        continue;
      }
      if (ch === "`") inBacktick = false;
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === "\"") {
      inDouble = true;
      continue;
    }
    if (ch === "`") {
      inBacktick = true;
      continue;
    }

    if (ch === "(") {
      depth++;
      if (depth === 1) rows++;
      continue;
    }
    if (ch === ")") {
      if (depth > 0) depth--;
      continue;
    }
  }

  return Math.max(1, rows);
}

/**
 * Find table name in CREATE TABLE / INSERT / REPLACE.
 * Return unqualified table name (no db/schema), best-effort.
 */
function parseTableNameFromCreateOrInsert(stmt: string): string | null {
  const s = stmt.trimStart();
  if (/^create\s+table\b/i.test(s)) {
    const m = /^\s*create\s+table\s+(?:if\s+not\s+exists\s+)?/i.exec(s);
    if (!m) return null;
    const rest = s.slice(m[0].length);
    const t = stripDbQualifier(rest);
    return t.value || null;
  }

  if (/^(insert|replace)\b/i.test(s)) {
    // We scan tokens after INSERT/REPLACE to skip modifiers and optional INTO.
    let i = 0;

    // first token: INSERT or REPLACE
    const first = readMySqlIdentifierToken(s, i);
    if (!first.value) return null;
    i = first.end;

    const modifiers = new Set<string>([
      "low_priority",
      "high_priority",
      "delayed",
      "ignore",
      "into"
    ]);

    while (i < s.length) {
      while (i < s.length && isWhitespaceCharCode(s.charCodeAt(i))) i++;
      const start = i;
      const tok = readMySqlIdentifierToken(s, start);
      if (!tok.value) break;

      const v = tok.value.toLowerCase();
      i = tok.end;

      if (modifiers.has(v)) continue;

      // tok started table name (maybe qualified)
      const t = stripDbQualifier(s.slice(start));
      return t.value || tok.value || null;
    }

    return null;
  }

  return null;
}

/**
 * Strip db qualifier from INSERT/REPLACE target table:
 * INSERT INTO `db`.`t` ...  -> INSERT INTO `t` ...
 * Works with quoted/unquoted identifiers, preserves original quoting of table identifier.
 */
function stripDbQualifierInInsertOrReplace(stmt: string): string {
  const s = stmt;
  const trimmed = s.trimStart();
  if (!/^(insert|replace)\b/i.test(trimmed)) return s;

  // find first keyword start in original string (keep leading whitespace)
  const leadLen = s.length - trimmed.length;
  let i = leadLen;

  // first token: INSERT or REPLACE
  const first = readMySqlIdentifierToken(s, i);
  if (!first.value) return s;
  i = first.end;

  // skip modifiers and optional INTO
  const modifiers = new Set<string>([
    "low_priority",
    "high_priority",
    "delayed",
    "ignore",
    "into"
  ]);

  while (i < s.length) {
    while (i < s.length && isWhitespaceCharCode(s.charCodeAt(i))) i++;
    const start = i;
    const tok = readMySqlIdentifierToken(s, start);
    if (!tok.value) return s;

    const v = tok.value.toLowerCase();
    i = tok.end;
    if (modifiers.has(v)) continue;

    // tok is first identifier of table name (could be db or table)
    let j = i;
    while (j < s.length && isWhitespaceCharCode(s.charCodeAt(j))) j++;
    if (j < s.length && s[j] === ".") {
      j++;
      // second identifier: table
      const second = readMySqlIdentifierToken(s, j);
      if (!second.value) return s;

      // Replace [tok.start, second.end) with second.raw
      return s.slice(0, tok.start) + second.raw + s.slice(second.end);
    }

    // not qualified
    return s;
  }

  return s;
}

/**
 * MySQL -> DuckDB type mapping.
 *
 * Goals:
 * - Never crash CREATE TABLE because of weird MySQL-only types.
 * - Prefer data load success over perfect typing (especially for invalid MySQL dates).
 */
function normalizeMySqlType(typeToken: string, unsigned: boolean, strictDateTypes: boolean): string {
  const t = typeToken.trim();
  const lower = t.toLowerCase();

  // base and params
  const m = /^([a-zA-Z0-9_]+)\s*(\(([^)]*)\))?/i.exec(lower);
  const base = (m?.[1] ?? lower).toLowerCase();
  const params = (m?.[3] ?? "").trim();

  const parseFirstInt = (s: string): number | null => {
    const mm = /^\s*(\d+)/.exec(s);
    if (!mm) return null;
    const n = Number(mm[1]);
    return Number.isFinite(n) ? n : null;
  };

  // boolean-ish
  if (base === "bool" || base === "boolean") return "BOOLEAN";

  // BIT(n)
  if (base === "bit") {
    const w = parseFirstInt(params) ?? 1;
    if (w <= 1) return "BOOLEAN";
    return "HUGEINT";
  }

  // integer family
  if (base === "tinyint") {
    const w = parseFirstInt(params);
    if (w === 1 && !unsigned) return "BOOLEAN";
    return unsigned ? "HUGEINT" : "TINYINT";
  }
  if (base === "smallint") return unsigned ? "HUGEINT" : "SMALLINT";
  if (base === "mediumint") return unsigned ? "HUGEINT" : "INTEGER";
  if (base === "int" || base === "integer") return unsigned ? "HUGEINT" : "INTEGER";
  if (base === "bigint") return unsigned ? "HUGEINT" : "BIGINT";

  // numeric
  if (base === "decimal" || base === "numeric") {
    // MySQL can have DECIMAL(65,30). DuckDB DECIMAL precision is limited (commonly 38).
    // To guarantee load success, we clamp to TEXT when precision > 38.
    if (!params) return "DECIMAL";

    const mm = /^\s*(\d+)\s*(?:,\s*(\d+)\s*)?$/.exec(params);
    if (!mm) return "DECIMAL";
    const p = Number(mm[1]);
    const s = mm[2] ? Number(mm[2]) : 0;

    if (!Number.isFinite(p) || p <= 0) return "DECIMAL";
    if (p > 38) return "TEXT";

    if (!Number.isFinite(s) || s < 0) return `DECIMAL(${p})`;
    return `DECIMAL(${p},${s})`;
  }

  if (base === "float" || base === "double" || base === "real") return "DOUBLE";

  // datetime family — default to TEXT to accept invalid MySQL zero-dates and weird values.
  if (
    base === "date" ||
    base === "datetime" ||
    base === "timestamp" ||
    base === "time" ||
    base === "year"
  ) {
    if (!strictDateTypes) return "TEXT";
    if (base === "date") return "DATE";
    if (base === "time") return "TIME";
    if (base === "year") return "INTEGER";
    return "TIMESTAMP";
  }

  // text family
  if (
    base.includes("text") ||
    base === "char" ||
    base === "varchar" ||
    base === "tinytext" ||
    base === "mediumtext" ||
    base === "longtext"
  ) {
    return "TEXT";
  }

  // json
  if (base === "json") return "TEXT";

  // enum / set
  if (base === "enum" || base === "set") return "TEXT";

  // binary/blob family
  if (
    base.includes("blob") ||
    base === "binary" ||
    base === "varbinary" ||
    base === "tinyblob" ||
    base === "mediumblob" ||
    base === "longblob"
  ) {
    return "BLOB";
  }

  // spatial types -> store as BLOB (mysqldump often emits WKB as 0x..)
  if (
    base === "geometry" ||
    base === "point" ||
    base === "linestring" ||
    base === "polygon" ||
    base === "multipoint" ||
    base === "multilinestring" ||
    base === "multipolygon" ||
    base === "geometrycollection"
  ) {
    return "BLOB";
  }

  // fallback: safest universal type
  return "TEXT";
}

function cleanupMySqlColumnAttrs(input: string): {
  cleaned: string;
  autoIncrement: boolean;
  colPrimaryKey: boolean;
  colUnique: boolean;
} {
  let s = input;

  const autoIncrement = /\bauto_increment\b/i.test(s);
  const colPrimaryKey = /\bprimary\s+key\b/i.test(s);
  const colUnique = /\bunique\b/i.test(s);

  // Remove MySQL-only noise / options. Keep NOT NULL / NULL / DEFAULT / CHECK (optional).
  // NOTE: We intentionally DROP constraints from CREATE TABLE to maximize load success.
  s = s
    .replace(/\bauto_increment\b/gi, "")
    .replace(/\bunsigned\b/gi, "")
    .replace(/\bzerofill\b/gi, "")
    .replace(/\bon\s+update\s+current_timestamp(?:\s*\(\s*\d+\s*\))?/gi, "")
    .replace(/\bcharacter\s+set\s+\w+/gi, "")
    .replace(/\bcharset\s*=\s*\w+/gi, "")
    .replace(/\bcollate\s+\w+/gi, "")
    .replace(/\bcomment\s+('(?:\\'|[^'])*'|\"(?:\\\"|[^\"])*\")/gi, "")
    .replace(/\bcolumn_format\s+\w+/gi, "")
    .replace(/\bstorage\s+\w+/gi, "")
    .replace(/\bprimary\s+key\b/gi, "")
    .replace(/\bunique\b/gi, "");

  // GENERATED ALWAYS AS (...) [VIRTUAL|STORED]
  // We strip it to avoid syntax incompatibilities and constraint issues.
  s = s.replace(/\bgenerated\s+always\s+as\s*\([\s\S]*?\)\s*(?:virtual|stored)?/gi, "");

  // DEFAULT CURRENT_TIMESTAMP() -> DEFAULT CURRENT_TIMESTAMP
  s = s.replace(/\bdefault\s+current_timestamp\s*\(\s*\)\b/gi, "DEFAULT CURRENT_TIMESTAMP");

  // normalize whitespace
  s = s.replace(/\s+/g, " ").trim();

  return { cleaned: s, autoIncrement, colPrimaryKey, colUnique };
}

function parseIndexColumns(colsInsideParens: string): string[] {
  // Split by commas at top level (handles col(10) etc).
  const items = splitTopLevelComma(colsInsideParens);
  const cols: string[] = [];

  for (const it of items) {
    const tok = readMySqlIdentifierToken(it, 0);
    if (!tok.value) continue;
    cols.push(tok.value);
  }

  return cols;
}

function parseParenthesizedColumnsBlock(s: string): string | null {
  const firstParen = s.indexOf("(");
  if (firstParen < 0) return null;
  const close = findTopLevelParenBlock(s, firstParen);
  if (close < 0) return null;
  return s.slice(firstParen + 1, close);
}

function normalizeCreateTableStatement(stmt: string, strictDateTypes: boolean): {
  tableName: string;
  createSql: string;
  postStatements: string[];
  pkColumns: string[];
} | null {
  const s0 = stripBom(stmt).trim();
  if (!/^create\s+table\b/i.test(s0)) return null;

  // Find first '(' outside quotes.
  let openParen = -1;
  {
    let inSingle = false;
    let inDouble = false;
    let inBacktick = false;
    let escape = false;

    for (let i = 0; i < s0.length; i++) {
      const ch = s0[i];

      if (inSingle) {
        if (escape) {
          escape = false;
          continue;
        }
        if (ch === "\\") {
          escape = true;
          continue;
        }
        if (ch === "'" && i + 1 < s0.length && s0[i + 1] === "'") {
          i++;
          continue;
        }
        if (ch === "'") inSingle = false;
        continue;
      }

      if (inDouble) {
        if (ch === "\"" && i + 1 < s0.length && s0[i + 1] === "\"") {
          i++;
          continue;
        }
        if (ch === "\"") inDouble = false;
        continue;
      }

      if (inBacktick) {
        if (ch === "`" && i + 1 < s0.length && s0[i + 1] === "`") {
          i++;
          continue;
        }
        if (ch === "`") inBacktick = false;
        continue;
      }

      if (ch === "'") {
        inSingle = true;
        continue;
      }
      if (ch === "\"") {
        inDouble = true;
        continue;
      }
      if (ch === "`") {
        inBacktick = true;
        continue;
      }

      if (ch === "(") {
        openParen = i;
        break;
      }
    }
  }

  if (openParen < 0) return null;

  const closeParen = findTopLevelParenBlock(s0, openParen);
  if (closeParen < 0) return null;

  const header = s0.slice(0, openParen).trim();
  const body = s0.slice(openParen + 1, closeParen);

  const m = /^\s*create\s+table\s+(?:if\s+not\s+exists\s+)?/i.exec(header);
  if (!m) return null;
  const namePart = header.slice(m[0].length);
  const tn = stripDbQualifier(namePart);
  const tableName = tn.value;
  if (!tableName) return null;

  const entries = splitTopLevelComma(body);

  const colDefs: string[] = [];
  const postStatements: string[] = [];

  const pkCols: string[] = [];
  const uniqueIndexes: Array<{ name: string; cols: string[] }> = [];
  const normalIndexes: Array<{ name: string; cols: string[] }> = [];

  let anonIdx = 0;

  for (const eRaw of entries) {
    const e = eRaw.trim();
    if (!e) continue;

    const lower = e.toLowerCase();

    // PRIMARY KEY / UNIQUE KEY / KEY / CONSTRAINT ... -> we don't put constraints into CREATE TABLE (avoid enforcement issues)
    if (lower.startsWith("primary key")) {
      const colsBlock = parseParenthesizedColumnsBlock(e);
      const cols = colsBlock ? parseIndexColumns(colsBlock) : [];
      if (cols.length) pkCols.push(...cols);
      continue;
    }

    if (lower.startsWith("unique key") || lower.startsWith("unique index") || lower.startsWith("unique")) {
      // UNIQUE KEY `name` (`a`,`b`)
      // Some dumps: UNIQUE KEY (`a`)  (no name)
      let rest = e.replace(/^unique\s+(key|index)?\s*/i, "");
      rest = rest.trimStart();

      let idxName = "";
      const nameTok = readMySqlIdentifierToken(rest, 0);
      if (nameTok.value) {
        // if next part contains '(' after name -> treat as name; else maybe it's directly '('
        const afterName = rest.slice(nameTok.end).trimStart();
        if (afterName.startsWith("(")) {
          idxName = nameTok.value;
          rest = afterName;
        } else if (rest.trimStart().startsWith("(")) {
          idxName = "";
        } else {
          // fallback
          idxName = nameTok.value;
          rest = afterName;
        }
      }

      const colsBlock = parseParenthesizedColumnsBlock(rest);
      const cols = colsBlock ? parseIndexColumns(colsBlock) : [];
      if (cols.length) {
        if (!idxName) idxName = `uniq_${tableName}_${++anonIdx}`;
        uniqueIndexes.push({ name: idxName, cols });
      }
      continue;
    }

    if (
      lower.startsWith("key ") ||
      lower.startsWith("index ") ||
      lower.startsWith("fulltext key") ||
      lower.startsWith("fulltext index") ||
      lower.startsWith("spatial key") ||
      lower.startsWith("spatial index")
    ) {
      // KEY `name` (`a`), INDEX `name` (`a`)
      const rest0 = e
        .replace(/^fulltext\s+(key|index)\s*/i, "")
        .replace(/^spatial\s+(key|index)\s*/i, "")
        .replace(/^(key|index)\s*/i, "")
        .trimStart();

      let rest = rest0;
      let idxName = "";
      const nameTok = readMySqlIdentifierToken(rest, 0);
      if (nameTok.value) {
        const afterName = rest.slice(nameTok.end).trimStart();
        if (afterName.startsWith("(")) {
          idxName = nameTok.value;
          rest = afterName;
        } else if (rest.trimStart().startsWith("(")) {
          idxName = "";
        } else {
          idxName = nameTok.value;
          rest = afterName;
        }
      }

      const colsBlock = parseParenthesizedColumnsBlock(rest);
      const cols = colsBlock ? parseIndexColumns(colsBlock) : [];
      if (cols.length) {
        if (!idxName) idxName = `idx_${tableName}_${++anonIdx}`;
        normalIndexes.push({ name: idxName, cols });
      }
      continue;
    }

    if (lower.startsWith("constraint ")) {
      // Constraint lines: foreign keys, checks, unique constraints. We do not enforce.
      // But we *try* to preserve info by creating indexes for UNIQUE, ignoring FKs.
      if (/\bunique\b/i.test(e)) {
        // try parse as unique constraint: CONSTRAINT `n` UNIQUE (`a`,`b`)
        const colsBlock = parseParenthesizedColumnsBlock(e);
        const cols = colsBlock ? parseIndexColumns(colsBlock) : [];
        if (cols.length) {
          const nameTok = readMySqlIdentifierToken(e, "constraint".length);
          const idxName = nameTok.value ? `uniq_${nameTok.value}` : `uniq_${tableName}_${++anonIdx}`;
          uniqueIndexes.push({ name: idxName, cols });
        }
      }
      continue;
    }

    // Column definition
    const nameTok = readMySqlIdentifierToken(e, 0);
    const colName = nameTok.value;
    if (!colName) continue;

    let rest = e.slice(nameTok.end).trimStart();
    if (!rest) {
      colDefs.push(`${quoteDuckDbIdent(colName)} TEXT`);
      continue;
    }

    // Read type token (word + optional (...) )
    // e.g. int(11), varchar(255), decimal(10,2), timestamp(3)
    let typeTok = "";
    {
      let j = 0;
      while (j < rest.length && isWhitespaceCharCode(rest.charCodeAt(j))) j++;
      const start = j;

      while (j < rest.length) {
        const cc = rest.charCodeAt(j);
        const isAZ = (cc >= 0x41 && cc <= 0x5a) || (cc >= 0x61 && cc <= 0x7a);
        const is09 = cc >= 0x30 && cc <= 0x39;
        const isUnd = cc === 0x5f;
        if (!isAZ && !is09 && !isUnd) break;
        j++;
      }

      typeTok = rest.slice(start, j);

      // optional (...) right after type
      while (j < rest.length && isWhitespaceCharCode(rest.charCodeAt(j))) j++;
      if (j < rest.length && rest[j] === "(") {
        const close = findTopLevelParenBlock(rest, j);
        if (close > j) {
          typeTok += rest.slice(j, close + 1);
          j = close + 1;
        }
      }

      rest = rest.slice(j).trimStart();
    }

    if (!typeTok) {
      typeTok = "text";
    }

    // flags from attrs
    const hasUnsigned = /\bunsigned\b/i.test(rest);
    const attrsInfo = cleanupMySqlColumnAttrs(rest);

    // record column-level pk/unique for post-index building (best-effort)
    if (attrsInfo.colPrimaryKey) pkCols.push(colName);
    if (attrsInfo.colUnique) uniqueIndexes.push({ name: `uniq_${tableName}_${colName}`, cols: [colName] });

    const duckType = normalizeMySqlType(typeTok, hasUnsigned, strictDateTypes);

    // NOTE: We do not implement identity here; inserts typically include explicit ids.
    // We just strip AUTO_INCREMENT to avoid syntax errors.
    const attrs = attrsInfo.cleaned;

    const colSql = `${quoteDuckDbIdent(colName)} ${duckType}${attrs ? ` ${attrs}` : ""}`;
    colDefs.push(colSql);
  }

  // Generate postStatements for indexes.
  // We create AFTER data load for speed, and ignore failures (e.g. duplicates).
  const mkIndexSql = (uniq: boolean, idxName: string, cols: string[]): string => {
    const uq = uniq ? "UNIQUE " : "";
    const colsSql = cols.map((c) => quoteDuckDbIdent(c)).join(", ");
    return `CREATE ${uq}INDEX ${quoteDuckDbIdent(idxName)} ON ${quoteDuckDbIdent(tableName)} (${colsSql});`;
  };

  if (pkCols.length) {
    // de-dup pk columns while preserving order
    const seen = new Set<string>();
    const pk = pkCols.filter((c) => {
      const k = c.toLowerCase();
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });

    postStatements.push(mkIndexSql(true, `pk_${tableName}`, pk));
  }

  for (const u of uniqueIndexes) {
    if (!u.cols.length) continue;
    postStatements.push(mkIndexSql(true, u.name, u.cols));
  }

  for (const idx of normalIndexes) {
    if (!idx.cols.length) continue;
    postStatements.push(mkIndexSql(false, idx.name, idx.cols));
  }

  // Create TABLE statement (no constraints)
  const createSql =
    `CREATE TABLE IF NOT EXISTS ${quoteDuckDbIdent(tableName)} (\n` +
    colDefs.map((c) => `  ${c}`).join(",\n") +
    `\n);`;

  return { tableName, createSql, postStatements, pkColumns: pkCols };
}

async function runAsyncConn(conn: DuckDbConnectionLike, sql: string, params?: unknown[]): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const p = params && params.length > 0 ? params : undefined;
    conn.run(sql, p, (err: Error | null) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

async function allAsyncConn(
  conn: DuckDbConnectionLike,
  sql: string,
  params?: unknown[]
): Promise<Array<Record<string, unknown>>> {
  return new Promise<Array<Record<string, unknown>>>((resolve, reject) => {
    const p = params && params.length > 0 ? params : undefined;
    conn.all(sql, p, (err: Error | null, r?: Array<Record<string, unknown>>) => {
      if (err) return reject(err);
      resolve(r || []);
    });
  });
}

/**
 * Streaming SQL dump statement reader (handles chunk boundary, quotes, comments, custom delimiters).
 */
class SqlDumpStatementReader {
  private delimiter: string = ";";

  private stmtParts: string[] = [];

  private inSingle: boolean = false;
  private inDouble: boolean = false;
  private inBacktick: boolean = false;

  private escapeNextInSingle: boolean = false;

  private inLineComment: boolean = false;
  private inBlockComment: boolean = false;

  private delimMatch: number = 0;
  private pendingDelim: string = "";

  private firstToken: string = "";
  private firstTokenDone: boolean = false;
  private isDelimiterCmd: boolean = false;

  private resetStatementState(): void {
    this.stmtParts = [];
    this.firstToken = "";
    this.firstTokenDone = false;
    this.isDelimiterCmd = false;
    this.delimMatch = 0;
    this.pendingDelim = "";
  }

  private pushSlice(chunk: string, start: number, end: number): void {
    if (end <= start) return;
    this.stmtParts.push(chunk.slice(start, end));
  }

  private consumeStatement(): string | null {
    if (!this.stmtParts.length) {
      this.resetStatementState();
      return null;
    }

    const stmt = stripBom(this.stmtParts.join("")).trim();
    this.resetStatementState();

    if (!stmt) return null;

    // DELIMITER command handling
    if (/^delimiter\s+/i.test(stmt)) {
      const m = /^delimiter\s+(.+)\s*$/i.exec(stmt);
      if (m && m[1]) {
        this.delimiter = m[1].trim();
      }
      return null;
    }

    return stmt;
  }

  push(chunk: string): string[] {
    const out: string[] = [];
    let partStart = 0;

    const L = chunk.length;

    for (let i = 0; i < L; i++) {
      const ch = chunk[i];
      const cc = chunk.charCodeAt(i);

      // line comment
      if (this.inLineComment) {
        if (ch === "\n") {
          this.inLineComment = false;
          partStart = i + 1;
        }
        continue;
      }

      // block comment
      if (this.inBlockComment) {
        if (ch === "*" && i + 1 < L && chunk[i + 1] === "/") {
          this.inBlockComment = false;
          i++;
          partStart = i + 1;
        }
        continue;
      }

      // in single quote
      if (this.inSingle) {
        if (this.escapeNextInSingle) {
          this.escapeNextInSingle = false;
          continue;
        }
        if (ch === "\\") {
          this.escapeNextInSingle = true;
          continue;
        }
        if (ch === "'" && i + 1 < L && chunk[i + 1] === "'") {
          i++;
          continue;
        }
        if (ch === "'") this.inSingle = false;
        continue;
      }

      // in double quote
      if (this.inDouble) {
        if (ch === "\"" && i + 1 < L && chunk[i + 1] === "\"") {
          i++;
          continue;
        }
        if (ch === "\"") this.inDouble = false;
        continue;
      }

      // in backtick
      if (this.inBacktick) {
        if (ch === "`" && i + 1 < L && chunk[i + 1] === "`") {
          i++;
          continue;
        }
        if (ch === "`") this.inBacktick = false;
        continue;
      }

      // not in string/backtick/comments
      if (ch === "'") {
        this.inSingle = true;
        continue;
      }
      if (ch === "\"") {
        this.inDouble = true;
        continue;
      }
      if (ch === "`") {
        this.inBacktick = true;
        continue;
      }

      // comment start:
      //   # ...
      if (ch === "#") {
        this.pushSlice(chunk, partStart, i);
        this.inLineComment = true;
        partStart = i + 1;
        continue;
      }

      //   -- <space>
      if (
        ch === "-" &&
        i + 1 < L &&
        chunk[i + 1] === "-" &&
        (i + 2 >= L || isWhitespaceCharCode(chunk.charCodeAt(i + 2)))
      ) {
        this.pushSlice(chunk, partStart, i);
        this.inLineComment = true;
        partStart = i + 2;
        i++;
        continue;
      }

      //   /* ... */
      if (ch === "/" && i + 1 < L && chunk[i + 1] === "*") {
        this.pushSlice(chunk, partStart, i);
        this.inBlockComment = true;
        partStart = i + 2;
        i++;
        continue;
      }

      // DELIMITER line (terminated by newline)
      // We detect by first token outside strings/comments.
      if (!this.firstTokenDone) {
        if (isWhitespaceCharCode(cc)) {
          if (this.firstToken.length > 0) {
            this.firstTokenDone = true;
            this.isDelimiterCmd = this.firstToken.toLowerCase() === "delimiter";
          }
        } else {
          // cap token length to avoid pathological cases
          if (this.firstToken.length < 32) this.firstToken += ch;
          else this.firstTokenDone = true;
        }
      }

      if (this.isDelimiterCmd && (ch === "\n" || ch === "\r")) {
        this.pushSlice(chunk, partStart, i);
        partStart = i + 1;

        const stmt = this.consumeStatement();
        if (stmt) out.push(stmt);
        continue;
      }

      // statement delimiter
      if (this.delimiter.length === 1) {
        if (ch === this.delimiter) {
          this.pushSlice(chunk, partStart, i);
          partStart = i + 1;

          const stmt = this.consumeStatement();
          if (stmt) out.push(stmt);
          continue;
        }
      } else {
        // multi-char delimiter, need match state
        if (this.delimMatch > 0) {
          // currently matching
          if (ch === this.delimiter[this.delimMatch]) {
            this.pendingDelim += ch;
            this.delimMatch++;
            partStart = i + 1;

            if (this.delimMatch === this.delimiter.length) {
              // matched full delimiter -> finalize statement
              const stmt = this.consumeStatement();
              if (stmt) out.push(stmt);

              // reset match state
              this.delimMatch = 0;
              this.pendingDelim = "";
              partStart = i + 1;
            }
            continue;
          }

          // mismatch -> pendingDelim is part of statement
          this.stmtParts.push(this.pendingDelim);
          this.pendingDelim = "";
          this.delimMatch = 0;

          // re-process current char
          partStart = i;
          i--;
          continue;
        }

        // no ongoing match
        if (ch === this.delimiter[0]) {
          this.pushSlice(chunk, partStart, i);
          this.pendingDelim = ch;
          this.delimMatch = 1;
          partStart = i + 1;
          continue;
        }
      }
    }

    // append remaining
    this.pushSlice(chunk, partStart, L);
    return out;
  }

  finish(): string[] {
    const out: string[] = [];
    if (this.delimMatch > 0 && this.pendingDelim) {
      this.stmtParts.push(this.pendingDelim);
      this.pendingDelim = "";
      this.delimMatch = 0;
    }

    const stmt = this.consumeStatement();
    if (stmt) out.push(stmt);
    return out;
  }
}

export class DuckDbAdapter extends Adapter {
  private readonly duckdb: DuckDbModuleLike;
  private db: DuckDbDatabaseLike | null = null;
  private conn: DuckDbConnectionLike | null = null;

  private readonly dbPath: string;
  private readonly parquetFile?: string;
  private readonly sqlDumpFile?: string;

  private renderer?: Renderer;

  private failedTables: FailedTable[] = [];

  private constructor(connectionString: string, duckdb: DuckDbModuleLike, renderer?: Renderer) {
    super("duckdb", connectionString);
    this.duckdb = duckdb;
    this.renderer = renderer;

    const parsed = parseDuckDbConnection(connectionString);
    this.dbPath = parsed.dbPath;
    this.parquetFile = parsed.parquetFile;
    this.sqlDumpFile = parsed.sqlDumpFile;
  }

  static async create(connectionString: string, renderer?: Renderer): Promise<DuckDbAdapter> {
    const { module: duckdb } = await loadPackage<DuckDbModuleLike>("duckdb", {
      installName: "duckdb",
      label: "duckdb driver (duckdb)",
      renderer
    });

    return new DuckDbAdapter(connectionString, duckdb, renderer);
  }

  async connect(): Promise<void> {
    this.db = new this.duckdb.Database(this.dbPath);
    this.conn = this.db.connect();

    // Parquet shortcut
    if (this.parquetFile) {
      const escaped = escapeDuckDbStringLiteral(this.parquetFile);
      await this.runAsync(`CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('${escaped}');`);
    }

    // SQL dump shortcut (MySQL / phpMyAdmin dump)
    if (this.sqlDumpFile) {
      await this.loadSqlDump(this.sqlDumpFile);
    }
  }

  private async loadSqlDump(filePath: string): Promise<void> {
    const absPath = path.resolve(filePath);
    const st = await fs.stat(absPath);
    const fileSize = st.size;

    const strictDateTypes = parseEnvBool("USQL_SQLDUMP_STRICT_DATE_TYPES", false);

    const { cacheDir, cachePath, metaPath, lockPath } = cachePathsForSqlDump(absPath, st);

    // Keep cache under control before doing anything expensive
    await cleanSqlDumpCache(cacheDir);

    const isLarge = fileSize >= LARGE_FILE_BYTES;

    // Cache hit fast path
    const hitMeta = await isSqlDumpCacheValid(cachePath, metaPath);
    if (hitMeta) {
      await this.attachCacheAndCreateViews(cachePath);
      return;
    }

    // Build cache (locked)
    await fs.mkdir(cacheDir, { recursive: true });

    await withFileLock(
      lockPath,
      async () => {
        // Another process may have built it while we waited
        const meta2 = await isSqlDumpCacheValid(cachePath, metaPath);
        if (meta2) return;

        // temp file -> atomic rename
        const tmpPath = `${cachePath}.tmp.${process.pid}.${Math.random().toString(16).slice(2)}`;
        await fs.unlink(tmpPath).catch(() => undefined);
        await fs.unlink(`${tmpPath}.meta.json`).catch(() => undefined);

        if (isLarge) {
          this.renderer?.info?.(
            `  parsing ${formatInt(Math.round(fileSize / 1024 / 1024))}MB SQL dump (first time, will cache)...`
          );
        } else {
          this.renderer?.info?.("  parsing SQL dump (first time, will cache)...");
        }

        // Build into tmp duckdb file
        const buildDb = new this.duckdb.Database(tmpPath);
        const buildConn = buildDb.connect();

        let stats: { tables: number; rows: number; failedTables: number } | null = null;

        try {
          const hexStrategy = await detectHexStrategy(buildConn);
          stats = await this.executeSqlDumpIntoConnection(buildConn, {
            absPath,
            fileSize,
            isLarge,
            strictDateTypes,
            hexStrategy
          });
        } finally {
          // Close build connection/db
          await new Promise<void>((resolve) => buildConn.close?.(() => resolve()) ?? resolve());
          await new Promise<void>((resolve) => buildDb.close?.(() => resolve()) ?? resolve());
          // Extra delay for Windows to release file handles
          await new Promise((r) => setTimeout(r, 1000));
        }

        // If no tables parsed, don't cache
        if (!stats || stats.tables <= 0) {
          await fs.unlink(tmpPath).catch(() => undefined);
          return;
        }

        // Rename into place (with retry for Windows file locking)
        let renameAttempts = 0;
        const maxRenameAttempts = 10;
        while (renameAttempts < maxRenameAttempts) {
          try {
            await fs.rename(tmpPath, cachePath);
            break;
          } catch (e: unknown) {
            const err = e as { code?: string };
            // If someone else already created it, discard ours.
            if (err && (err.code === "EEXIST" || err.code === "EPERM")) {
              await fs.unlink(tmpPath).catch(() => undefined);
              break;
            }
            // Windows file locking - wait and retry
            if (err && err.code === "EBUSY" && renameAttempts < maxRenameAttempts - 1) {
              renameAttempts++;
              this.renderer?.info?.(`  waiting for file lock release (attempt ${renameAttempts}/${maxRenameAttempts})...`);
              await new Promise((r) => setTimeout(r, 1000 * renameAttempts));
              continue;
            }
            await fs.unlink(tmpPath).catch(() => undefined);
            throw e;
          }
        }

        // Write meta
        const meta: SqlDumpCacheMeta = {
          v: SQLDUMP_CACHE_VERSION,
          builtAt: new Date().toISOString(),
          source: { path: absPath, size: fileSize, mtimeMs: st.mtimeMs },
          tables: stats.tables,
          rows: stats.rows,
          failedTables: stats.failedTables
        };

        await writeSqlDumpCacheMeta(metaPath, meta);

        // Only show summary for first-time builds
        this.renderer?.success?.(
          `+ loaded ${formatInt(meta.tables)} tables, ${formatInt(meta.rows)} rows${
            meta.failedTables > 0 ? ` (${meta.failedTables} failed)` : ""
          }`
        );
        
        // Show failed table details
        if (this.failedTables.length > 0) {
          for (const f of this.failedTables.slice(0, 3)) {
            const shortErr = f.error.split('\n')[0].slice(0, 80);
            this.renderer?.warn?.(`  - ${f.name}: ${shortErr}`);
          }
          if (this.failedTables.length > 3) {
            this.renderer?.warn?.(`  ... and ${this.failedTables.length - 3} more`);
          }
        }
      },
      this.renderer
    );

    // After build, must exist (or build failed -> we still try best-effort direct load)
    const finalMeta = await isSqlDumpCacheValid(cachePath, metaPath);

    if (finalMeta) {
      await this.attachCacheAndCreateViews(cachePath);
      return;
    }

    // Fallback: cache failed (permissions/disk/etc). Load directly into memory DB.
    // This is less ideal for 2GB, but better than failing.
    this.renderer?.warn?.("  warning: sql dump cache unavailable; loading directly into memory (no cache).");

    const hexStrategy = await detectHexStrategy(this.conn!);
    const stats = await this.executeSqlDumpIntoConnection(this.conn!, {
      absPath,
      fileSize,
      isLarge,
      strictDateTypes,
      hexStrategy
    });

    this.renderer?.info?.(
      `  loaded ${formatInt(stats.tables)} tables, ${formatInt(stats.rows)} rows${
        stats.failedTables > 0 ? ` (${formatInt(stats.failedTables)} failed)` : ""
      }`
    );

    if (this.failedTables.length > 0 && process.env.USQL_DEBUG) {
      for (const f of this.failedTables) {
        this.renderer?.warn?.(`  - ${f.name}: ${f.error}`);
        this.renderer?.warn?.(`    ${f.stmt}`);
      }
    }
  }

  private async attachCacheAndCreateViews(cachePath: string): Promise<void> {
    const escaped = escapeDuckDbStringLiteral(cachePath);

    // Keep attached — views reference it.
    await this.runAsync(`ATTACH '${escaped}' AS cached (READ_ONLY);`);

    // Get tables from the attached database using duckdb_tables()
    // Note: information_schema doesn't work for attached databases
    const rows = await this.allAsync(
      `SELECT table_name FROM duckdb_tables() WHERE database_name = 'cached' AND schema_name = 'main';`
    );

    for (const r of rows) {
      const name = String(r["table_name"] ?? "");
      if (!name) continue;

      const qName = quoteDuckDbIdent(name);

      // Expose as a view in main
      try {
        await this.runAsync(`CREATE OR REPLACE VIEW ${qName} AS SELECT * FROM cached.main.${qName};`);
      } catch {
        // ignore (rare)
      }
    }
  }

  private async executeSqlDumpIntoConnection(
    targetConn: DuckDbConnectionLike,
    opts: {
      absPath: string;
      fileSize: number;
      isLarge: boolean;
      strictDateTypes: boolean;
      hexStrategy: HexStrategy;
    }
  ): Promise<{ tables: number; rows: number; failedTables: number }> {
    const { absPath, fileSize, isLarge, strictDateTypes, hexStrategy } = opts;

    const CHUNK_BYTES = parseEnvInt("USQL_SQLDUMP_CHUNK_BYTES", 16 * 1024 * 1024);

    const reader = new SqlDumpStatementReader();
    const decoder = new StringDecoder("utf8");

    this.failedTables = [];

    let tables = 0;
    let rows = 0;

    // Track tables whose CREATE failed or inserts failed (skip further inserts)
    const created = new Set<string>();
    const broken = new Set<string>();

    // Post statements (indexes etc) run after data load for speed
    const postStatements: string[] = [];

    // Transactions for speed
    let inTx = false;
    let txRows = 0;
    let txStmts = 0;

    const COMMIT_EVERY_ROWS = parseEnvInt("USQL_SQLDUMP_COMMIT_EVERY_ROWS", 250_000);
    const COMMIT_EVERY_STMTS = parseEnvInt("USQL_SQLDUMP_COMMIT_EVERY_STMTS", 2_000);

    const beginTx = async (): Promise<void> => {
      if (inTx) return;
      try {
        await runAsyncConn(targetConn, "BEGIN TRANSACTION;");
        inTx = true;
        txRows = 0;
        txStmts = 0;
      } catch {
        inTx = false;
      }
    };

    const commitTx = async (): Promise<void> => {
      if (!inTx) return;
      try {
        await runAsyncConn(targetConn, "COMMIT;");
      } catch {
        // ignore
      } finally {
        inTx = false;
        txRows = 0;
        txStmts = 0;
      }
    };

    const maybeCommit = async (): Promise<void> => {
      if (!inTx) return;
      if (txRows >= COMMIT_EVERY_ROWS || txStmts >= COMMIT_EVERY_STMTS) {
        await commitTx();
      }
    };

    // Progress rendering
    let bytesRead = 0;
    let lastRenderAt = 0;
    let lastPct = -1;

    const isTTY = !!this.renderer?.isTTY;
    const intervalMs = isTTY ? 250 : 5000;

    const renderProgress = (force: boolean = false): void => {
      if (!isLarge) return;

      const now = Date.now();
      if (!force && now - lastRenderAt < intervalMs) return;

      lastRenderAt = now;

      const pct = fileSize > 0 ? Math.min(100, Math.floor((bytesRead / fileSize) * 100)) : 0;
      if (!force && !isTTY && pct === lastPct) return;
      lastPct = pct;

      const line = `  ${String(pct).padStart(3, " ")}% - ${formatInt(tables)} tables, ${formatInt(rows)} rows...`;

      if (isTTY) {
        const clear = " ".repeat(Math.min(140, Math.max(60, line.length + 8)));
        process.stderr.write("\r" + clear + "\r" + line);
      } else {
        this.renderer?.info?.(line);
      }
    };

    const clearProgress = (): void => {
      if (isTTY && isLarge) {
        const clear = " ".repeat(160);
        process.stderr.write("\r" + clear + "\r");
      }
    };

    const stream = createReadStream(absPath, { highWaterMark: CHUNK_BYTES });
    try {
      for await (const chunk of stream) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as ArrayBuffer);
        bytesRead += buf.length;

        const text = decoder.write(buf);
        const stmts = reader.push(text);

        for (const stmt0 of stmts) {
          const stmt = stmt0.trim();
          if (!stmt) continue;

          const low = stmt.slice(0, 64).toLowerCase();

          // Skip common mysqldump noise
          if (
            low.startsWith("set ") ||
            low.startsWith("use ") ||
            low.startsWith("lock tables") ||
            low.startsWith("unlock tables") ||
            low.startsWith("start transaction") ||
            low.startsWith("commit") ||
            low.startsWith("begin") ||
            low.startsWith("delimiter ")
          ) {
            continue;
          }

          // CREATE TABLE
          if (low.startsWith("create table")) {
            const converted = normalizeCreateTableStatement(stmt, strictDateTypes);
            if (!converted) continue;

            const tName = converted.tableName;
            const tKey = tName.toLowerCase();

            if (created.has(tKey) || broken.has(tKey)) continue;

            const sql = convertMySqlStatementToDuckDbSql(converted.createSql, { hexStrategy });

            try {
              await beginTx();
              await runAsyncConn(targetConn, sql);
              tables++;
              created.add(tKey);

              for (const ps of converted.postStatements) postStatements.push(ps);

              txStmts++;
              await maybeCommit();
            } catch (e: unknown) {
              broken.add(tKey);

              const msg = e instanceof Error ? e.message : String(e);
              this.failedTables.push({ name: tName, error: msg, stmt: safeSnippet(sql, 500) });

              // don't crash; continue
            }

            continue;
          }

          // INSERT / REPLACE
          if (low.startsWith("insert") || low.startsWith("replace")) {
            const tableName = parseTableNameFromCreateOrInsert(stmt);
            const tKey = (tableName || "").toLowerCase();

            if (tKey && broken.has(tKey)) continue;

            // Strip db qualifier: INSERT INTO db.table -> INSERT INTO table
            const cleaned = stripDbQualifierInInsertOrReplace(stmt);
            const sql = convertMySqlStatementToDuckDbSql(cleaned, { hexStrategy });

            const addRows = parseInsertRowCount(cleaned);

            try {
              await beginTx();
              await runAsyncConn(targetConn, sql);

              rows += addRows;
              txRows += addRows;
              txStmts++;

              await maybeCommit();
            } catch (e: unknown) {
              if (tKey) {
                broken.add(tKey);
                const msg = e instanceof Error ? e.message : String(e);
                this.failedTables.push({
                  name: tableName || "(unknown)",
                  error: msg,
                  stmt: safeSnippet(sql, 500)
                });
              }
              // ignore single insert failure and continue
            }

            continue;
          }

          // Other statements: best-effort attempt (views, drops, alters in dumps, etc.)
          try {
            const sql = convertMySqlStatementToDuckDbSql(stmt, { hexStrategy });
            await beginTx();
            await runAsyncConn(targetConn, sql);
            txStmts++;
            await maybeCommit();
          } catch {
            // ignore
          }
        }

        renderProgress(false);
      }

      // flush decoder + reader
      const tail = decoder.end();
      for (const stmt of reader.push(tail)) {
        // very rare; but handle
        const s = stmt.trim();
        if (!s) continue;
        // process as above in minimal mode: let recursion avoid — just run best effort
        try {
          const sql = convertMySqlStatementToDuckDbSql(s, { hexStrategy });
          await runAsyncConn(targetConn, sql);
        } catch {
          // ignore
        }
      }

      for (const stmt of reader.finish()) {
        const s = stmt.trim();
        if (!s) continue;
        try {
          const sql = convertMySqlStatementToDuckDbSql(s, { hexStrategy });
          await runAsyncConn(targetConn, sql);
        } catch {
          // ignore
        }
      }

      await commitTx();

      // Build indexes after load (fast path; ignore errors)
      for (const ps of postStatements) {
        const sql = convertMySqlStatementToDuckDbSql(ps, { hexStrategy });
        try {
          await runAsyncConn(targetConn, sql);
        } catch {
          // ignore
        }
      }
    } finally {
      clearProgress();
      stream.destroy();
    }

    return { tables, rows, failedTables: this.failedTables.length };
  }

  private runAsync(sql: string, params?: unknown[]): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const p = params && params.length > 0 ? params : undefined;
      this.conn!.run(sql, p, (err: Error | null) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  private allAsync(sql: string, params?: unknown[]): Promise<Array<Record<string, unknown>>> {
    return new Promise<Array<Record<string, unknown>>>((resolve, reject) => {
      const p = params && params.length > 0 ? params : undefined;
      this.conn!.all(sql, p, (err: Error | null, r?: Array<Record<string, unknown>>) => {
        if (err) return reject(err);
        resolve(r || []);
      });
    });
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    if (!this.conn) throw new Error("DuckDbAdapter: not connected");

    const selectLike = isProbablySelect(sql);

    if (selectLike) {
      const rows = await this.allAsync(sql, params);
      const columns = rows[0] ? Object.keys(rows[0]) : [];
      return { columns, rows, rowCount: rows.length };
    }

    await this.runAsync(sql, params);
    return { columns: [], rows: [], rowCount: undefined };
  }

  async close(): Promise<void> {
    const c = this.conn;
    const d = this.db;

    this.conn = null;
    this.db = null;

    if (c?.close) {
      await new Promise<void>((resolve) => c.close!(() => resolve()));
    }

    if (d?.close) {
      await new Promise<void>((resolve) => d.close!(() => resolve()));
    }
  }

  /** Check if this adapter is connected to a parquet file */
  get isParquet(): boolean {
    return !!this.parquetFile;
  }

  /** Get the parquet file path if applicable */
  get parquetPath(): string | undefined {
    return this.parquetFile;
  }

  /** Check if this adapter loaded a SQL dump file */
  get isSqlDump(): boolean {
    return !!this.sqlDumpFile;
  }
}
