import fs from "node:fs/promises";
import path from "node:path";
import { Adapter, QueryResult } from "./adapter";
import { loadPackage } from "../core/driver-manager";
import type { Renderer } from "../ui/renderer";

function parseSqlitePath(connectionString: string): string {
  const raw = connectionString.trim();
  const lower = raw.toLowerCase();

  // Accept:
  // - sqlite:./db.sqlite
  // - sqlite:///abs/path.db
  // - sqlite://./rel/path.db
  // - sqlite::memory:
  let s = raw;
  if (lower.startsWith("sqlite:")) s = raw.slice("sqlite:".length);

  // Normalize URL-ish prefixes
  if (s.startsWith("//")) s = s.slice(2);

  if (!s || s === ":memory:" || s === "memory") return ":memory:";
  return s;
}

function isProbablySelect(sql: string): boolean {
  const s = sql.trim().toLowerCase();
  return s.startsWith("select") || s.startsWith("pragma") || s.startsWith("with") || s.startsWith("explain");
}

type BetterSqliteStmt = {
  reader: boolean;
  all(params?: unknown[]): Array<Record<string, unknown>>;
  run(params?: unknown[]): { changes?: number };
  columns(): Array<{ name: string }>;
  // Optional APIs depending on version
  safeIntegers?: (enabled?: boolean) => BetterSqliteStmt;
};

type BetterSqliteDb = {
  prepare(sql: string): BetterSqliteStmt;
  exec(sql: string): void;
  close(): void;
  defaultSafeIntegers?: (enabled: boolean) => void;
};

type BetterSqliteCtor = new (filename: string, options?: Record<string, unknown>) => BetterSqliteDb;

class BetterSqlite3Adapter extends Adapter {
  private readonly DbCtor: BetterSqliteCtor;
  private db: BetterSqliteDb | null = null;
  private readonly filePath: string;

  private constructor(connectionString: string, DbCtor: BetterSqliteCtor) {
    super("sqlite", connectionString);
    this.DbCtor = DbCtor;
    this.filePath = parseSqlitePath(connectionString);
  }

  static async create(connectionString: string, renderer?: Renderer): Promise<BetterSqlite3Adapter> {
    const { module: mod } = await loadPackage<unknown>("better-sqlite3", {
      installName: "better-sqlite3",
      label: "sqlite driver (better-sqlite3)",
      renderer
    });

    // better-sqlite3 exports the Database constructor directly
    const DbCtor = mod as BetterSqliteCtor;
    return new BetterSqlite3Adapter(connectionString, DbCtor);
  }

  async connect(): Promise<void> {
    this.db = new this.DbCtor(this.filePath, {
      fileMustExist: false
    });

    // Star-level safety: if available, ask the driver to return BigInt for unsafe integers.
    // We will later JSON-normalize BigInt -> string in the introspector.
    try {
      if (this.db.defaultSafeIntegers) this.db.defaultSafeIntegers(true);
    } catch {
      // ignore
    }
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    if (!this.db) throw new Error("SQLite (better-sqlite3): not connected");

    // Check for multi-statement SQL (contains semicolon followed by non-whitespace)
    const trimmed = sql.trim();
    const hasMultipleStatements = /;\s*\S/.test(trimmed);

    // For multi-statement without params, use exec() which handles multiple statements
    if (hasMultipleStatements && (!params || params.length === 0)) {
      this.db.exec(trimmed);
      return { columns: [], rows: [], rowCount: undefined };
    }

    const stmt = this.db.prepare(sql);

    // If supported, request safe integer mode for this statement.
    try {
      stmt.safeIntegers?.(true);
    } catch {
      // ignore
    }

    if (stmt.reader || isProbablySelect(sql)) {
      const rows = params && params.length ? stmt.all(params) : stmt.all();
      const columns = stmt.columns().map((c) => c.name);
      return { columns, rows, rowCount: rows.length };
    }

    const res = params && params.length ? stmt.run(params) : stmt.run();
    return {
      columns: [],
      rows: [],
      rowCount: typeof res?.changes === "number" ? res.changes : undefined
    };
  }

  async close(): Promise<void> {
    if (!this.db) return;
    this.db.close();
    this.db = null;
  }
}

type SqlJsDatabase = {
  prepare(sql: string): {
    bind(params: unknown[]): void;
    step(): boolean;
    getAsObject(): Record<string, unknown>;
    getColumnNames(): string[];
    free(): void;
  };
  run(sql: string, params?: unknown[]): void;
  exec(sql: string): Array<{ columns: string[]; values: unknown[][] }>;
  export(): Uint8Array;
  getRowsModified(): number;
  close(): void;
};

type SqlJsInit = (options?: { locateFile?: (file: string) => string }) => Promise<{
  Database: new (data?: Uint8Array) => SqlJsDatabase;
}>;

class SqlJsAdapter extends Adapter {
  private readonly init: SqlJsInit;
  private readonly req: typeof require;
  private db: SqlJsDatabase | null = null;
  private readonly filePath: string;
  private dirty = false;

  private constructor(connectionString: string, init: SqlJsInit, req: typeof require) {
    super("sqlite", connectionString);
    this.init = init;
    this.req = req;
    this.filePath = parseSqlitePath(connectionString);
  }

  static async create(connectionString: string, renderer?: Renderer): Promise<SqlJsAdapter> {
    const loaded = await loadPackage<Record<string, unknown>>("sql.js", {
      installName: "sql.js",
      label: "sqlite driver fallback (sql.js)",
      renderer
    });

    const maybeInit: unknown =
      typeof loaded.module === "function"
        ? loaded.module
        : loaded.module?.default ?? loaded.module?.initSqlJs ?? loaded.module;

    if (typeof maybeInit !== "function") {
      throw new Error("sql.js: cannot find init function export");
    }

    return new SqlJsAdapter(connectionString, maybeInit as SqlJsInit, loaded.require);
  }

  async connect(): Promise<void> {
    // Ensure wasm file can be located reliably
    let distDir: string | null = null;
    try {
      const pkgJsonPath = this.req.resolve("sql.js/package.json");
      distDir = path.join(path.dirname(pkgJsonPath), "dist");
    } catch {
      distDir = null;
    }

    const SQL = await this.init({
      locateFile: (file: string) => {
        if (distDir) return path.join(distDir, file);
        return file;
      }
    });

    if (this.filePath === ":memory:") {
      this.db = new SQL.Database();
      return;
    }

    try {
      const buf = await fs.readFile(this.filePath);
      this.db = new SQL.Database(new Uint8Array(buf));
    } catch {
      this.db = new SQL.Database();
    }
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    if (!this.db) throw new Error("SQLite (sql.js): not connected");

    const selectLike = isProbablySelect(sql);

    if (selectLike) {
      const stmt = this.db.prepare(sql);
      try {
        if (params && params.length) stmt.bind(params);
        const columns = stmt.getColumnNames();
        const rows: Array<Record<string, unknown>> = [];
        while (stmt.step()) {
          rows.push(stmt.getAsObject());
        }
        return { columns, rows, rowCount: rows.length };
      } finally {
        stmt.free();
      }
    }

    this.dirty = true;

    if (params && params.length) {
      const stmt = this.db.prepare(sql);
      try {
        stmt.bind(params);
        stmt.step();
        return { columns: [], rows: [], rowCount: this.db.getRowsModified() };
      } finally {
        stmt.free();
      }
    }

    this.db.run(sql);
    return { columns: [], rows: [], rowCount: this.db.getRowsModified() };
  }

  async close(): Promise<void> {
    if (!this.db) return;

    if (this.filePath !== ":memory:" && this.dirty) {
      const data = this.db.export();
      await fs.mkdir(path.dirname(this.filePath), { recursive: true }).catch(() => undefined);
      await fs.writeFile(this.filePath, Buffer.from(data));
    }

    this.db.close();
    this.db = null;
  }
}

export async function createSqliteAdapter(connectionString: string, renderer?: Renderer): Promise<Adapter> {
  // Default: better-sqlite3 (native, fast)
  // Fallback: sql.js (WASM) if compilation/install fails
  try {
    return await BetterSqlite3Adapter.create(connectionString, renderer);
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    renderer?.warn?.(
      `- better-sqlite3 unavailable (${msg.split("\n")[0]}), falling back to sql.js`
    );
    return await SqlJsAdapter.create(connectionString, renderer);
  }
}
