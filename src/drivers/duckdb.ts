import { Adapter, QueryResult } from "./adapter";
import { loadPackage } from "../core/driver-manager";
import type { Renderer } from "../ui/renderer";

interface ParsedDuckDbConn {
  dbPath: string;
  parquetFile?: string;
}

function parseDuckDbConnection(connectionString: string): ParsedDuckDbConn {
  const raw = connectionString.trim();
  const lower = raw.toLowerCase();

  // Parquet file: parquet:./data.parquet
  if (lower.startsWith("parquet:")) {
    const parquetPath = raw.slice("parquet:".length);
    return { dbPath: ":memory:", parquetFile: parquetPath };
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
  return s.startsWith("select") || s.startsWith("with") || s.startsWith("pragma") || s.startsWith("explain") || s.startsWith("describe") || s.startsWith("show");
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

export class DuckDbAdapter extends Adapter {
  private readonly duckdb: DuckDbModuleLike;
  private db: DuckDbDatabaseLike | null = null;
  private conn: DuckDbConnectionLike | null = null;
  private readonly dbPath: string;
  private readonly parquetFile?: string;

  private constructor(connectionString: string, duckdb: DuckDbModuleLike) {
    super("duckdb", connectionString);
    this.duckdb = duckdb;
    const parsed = parseDuckDbConnection(connectionString);
    this.dbPath = parsed.dbPath;
    this.parquetFile = parsed.parquetFile;
  }

  static async create(connectionString: string, renderer?: Renderer): Promise<DuckDbAdapter> {
    const { module: duckdb } = await loadPackage<DuckDbModuleLike>("duckdb", {
      installName: "duckdb",
      label: "duckdb driver (duckdb)",
      renderer
    });

    return new DuckDbAdapter(connectionString, duckdb);
  }

  async connect(): Promise<void> {
    this.db = new this.duckdb.Database(this.dbPath);
    this.conn = this.db.connect();

    // If parquet file specified, create a view for it
    if (this.parquetFile) {
      const escaped = this.parquetFile.replace(/'/g, "''");
      await this.runAsync(`CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('${escaped}');`);
    }
  }

  private runAsync(sql: string, params?: unknown[]): Promise<void> {
    return new Promise((resolve, reject) => {
      const p = params && params.length > 0 ? params : undefined;
      this.conn!.run(sql, p, (err: Error | null) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  private allAsync(sql: string, params?: unknown[]): Promise<Array<Record<string, unknown>>> {
    return new Promise((resolve, reject) => {
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
}
