import type { Adapter, Dialect } from "../drivers/adapter";
import { quoteIdent } from "../drivers/adapter";

export type NormalizedType = "n" | "s" | "b" | "dt" | "j" | "bin" | "u";

export interface TableResume {
  /**
   * Columns: name -> normalized type
   * n=number, s=string, b=boolean, dt=datetime, j=json, bin=binary, u=unknown
   */
  c: Record<string, NormalizedType>;
  pk?: string[];
  /**
   * Foreign keys:
   * [fromCol, refTable, refCol]
   * refTable is fullName where possible (e.g. "public.users" or "db.users").
   */
  fk?: Array<[string, string, string]>;
  /**
   * Sample rows (JSON-safe)
   */
  s: Array<Record<string, unknown>>;
}

export interface DatabaseResume {
  v: 1;
  d: Dialect;
  t: Record<string, TableResume>;
}

interface TableRef {
  schema?: string;
  name: string;
  fullName: string;
}

function stripNullish<T>(v: T | null | undefined): v is T {
  return v !== null && v !== undefined;
}

function normalizeType(dialect: Dialect, type: string, extra?: { udtName?: string }): NormalizedType {
  const t = (type || "").toLowerCase();
  const udt = (extra?.udtName || "").toLowerCase();

  if (dialect === "postgres") {
    // 64-bit integers and arbitrary precision -> string (JSON safe + no precision loss)
    if (udt === "int8" || t === "bigint" || t.includes("numeric") || t.includes("decimal")) return "s";

    if (
      t === "smallint" ||
      t === "integer" ||
      t.includes("int") ||
      t.includes("double") ||
      t.includes("real") ||
      t.includes("float")
    )
      return "n";

    if (t === "boolean") return "b";
    if (t.includes("timestamp") || t === "date" || t.includes("time")) return "dt";
    if (t.startsWith("json")) return "j";
    if (t === "bytea") return "bin";
    if (t.includes("char") || t.includes("text") || t === "uuid") return "s";
    return "u";
  }

  if (dialect === "mysql") {
    if (t === "bigint" || t.includes("decimal") || t.includes("numeric")) return "s";

    if (t.includes("int") || t.includes("float") || t.includes("double") || t.includes("real")) return "n";

    if (t === "bit" || t === "bool" || t === "boolean") return "b";
    if (t.includes("date") || t.includes("time") || t.includes("year")) return "dt";
    if (t.includes("json")) return "j";
    if (t.includes("blob") || t.includes("binary")) return "bin";
    if (t.includes("char") || t.includes("text") || t.includes("enum") || t.includes("set")) return "s";
    return "u";
  }

  if (dialect === "duckdb") {
    if (t.includes("hugeint") || t.includes("ubigint") || t.includes("bigint")) return "s";
    if (t.includes("decimal") || t.includes("numeric")) return "s";

    if (t.includes("int") || t.includes("double") || t.includes("real") || t.includes("float")) return "n";

    if (t.includes("bool")) return "b";
    if (t.includes("timestamp") || t === "date" || t.includes("time")) return "dt";
    if (t.includes("json")) return "j";
    if (t.includes("blob") || t.includes("binary")) return "bin";
    if (t.includes("char") || t.includes("text") || t.includes("uuid")) return "s";
    return "u";
  }

  // SQLite (affinity-based).
  // IMPORTANT: SQLite INTEGER is 64-bit. We normalize it to string to avoid precision loss / BigInt JSON issues.
  if (dialect === "sqlite") {
    if (t.includes("int")) return "s";
    if (t.includes("char") || t.includes("clob") || t.includes("text")) return "s";
    if (t.includes("blob")) return "bin";
    if (t.includes("real") || t.includes("floa") || t.includes("doub")) return "n";
    if (t.includes("bool")) return "b";
    if (t.includes("date") || t.includes("time")) return "dt";
    if (t.includes("json")) return "j";
    return "u";
  }

  return "u";
}

function jsonSafe(value: unknown): unknown {
  if (value === null || value === undefined) return value;

  const t = typeof value;

  if (t === "bigint") return (value as bigint).toString();

  if (t === "number") {
    const n = value as number;
    if (Number.isFinite(n) && Number.isInteger(n) && !Number.isSafeInteger(n)) return String(n);
    return n;
  }

  if (t === "string" || t === "boolean") return value;

  if (value instanceof Date) return value.toISOString();

  // Buffer / Uint8Array
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const anyVal: any = value as any;
  if (typeof Buffer !== "undefined" && Buffer.isBuffer(anyVal)) {
    return `base64:${Buffer.from(anyVal).toString("base64")}`;
  }
  if (anyVal instanceof Uint8Array) {
    return `base64:${Buffer.from(anyVal).toString("base64")}`;
  }

  if (Array.isArray(value)) return value.map(jsonSafe);

  if (t === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = jsonSafe(v);
    }
    return out;
  }

  return String(value);
}

function coerceRowToSchema(
  row: Record<string, unknown>,
  colTypes: Record<string, NormalizedType>
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    const ct = colTypes[k];

    // If schema says string but driver returns integer -> normalize to string for JSON stability.
    if (ct === "s" && typeof v === "number" && Number.isFinite(v) && Number.isInteger(v)) {
      out[k] = String(v);
      continue;
    }

    out[k] = v;
  }
  return out;
}

async function listTables(adapter: Adapter): Promise<TableRef[]> {
  if (adapter.dialect === "sqlite") {
    const r = await adapter.query(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
    );
    return r.rows
      .map((x) => (x["name"] as string) || "")
      .filter((s) => s.trim().length > 0)
      .map((name) => ({ name, fullName: name }));
  }

  if (adapter.dialect === "postgres") {
    const r = await adapter.query(
      `
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY table_schema, table_name;
      `
    );
    return r.rows.map((x) => ({
      schema: String(x["table_schema"]),
      name: String(x["table_name"]),
      fullName: `${String(x["table_schema"])}.${String(x["table_name"])}`
    }));
  }

  if (adapter.dialect === "mysql") {
    const dbRes = await adapter.query("SELECT DATABASE() AS db;");
    const db = (dbRes.rows[0]?.["db"] as string | null | undefined) ?? null;

    if (db) {
      const r = await adapter.query(
        `
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        `,
        [db]
      );

      return r.rows.map((x) => {
        const name = String(x["table_name"]);
        return { schema: db, name, fullName: `${db}.${name}` };
      });
    }

    const r = await adapter.query(
      `
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
      ORDER BY table_schema, table_name;
      `
    );

    return r.rows.map((x) => {
      const schema = String(x["table_schema"]);
      const name = String(x["table_name"]);
      return { schema, name, fullName: `${schema}.${name}` };
    });
  }

  if (adapter.dialect === "duckdb") {
    // For SQL dump mode, we create views pointing to cached tables
    // So we need to include both BASE TABLE and VIEW, but only from main database
    const r = await adapter.query(
      `
      SELECT table_schema, table_name, table_type
      FROM information_schema.tables
      WHERE table_type IN ('BASE TABLE', 'VIEW')
        AND table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_catalog = current_database()
      ORDER BY table_schema, table_name;
      `
    );

    return r.rows.map((x) => {
      const schema = String(x["table_schema"]);
      const name = String(x["table_name"]);
      return { schema, name, fullName: `${schema}.${name}` };
    });
  }

  return [];
}

export async function listTableNames(adapter: Adapter): Promise<string[]> {
  const refs = await listTables(adapter);
  return refs.map((t) => t.fullName);
}

export async function inspectDatabase(
  adapter: Adapter,
  opts?: { sampleRows?: number }
): Promise<DatabaseResume> {
  const sampleRows = opts?.sampleRows ?? 3;
  const tables = await listTables(adapter);

  const out: DatabaseResume = {
    v: 1,
    d: adapter.dialect,
    t: {}
  };

  for (const t of tables) {
    out.t[t.fullName] = await inspectTable(adapter, t, sampleRows);
  }

  return out;
}


async function inspectTable(adapter: Adapter, table: TableRef, sampleRows: number): Promise<TableResume> {
  if (adapter.dialect === "sqlite") {
    const qName = quoteIdent("sqlite", table.name);

    const colsRes = await adapter.query(`PRAGMA table_info(${qName});`);
    const colTypes: Record<string, NormalizedType> = {};
    const pkCols: string[] = [];

    for (const row of colsRes.rows) {
      const name = String(row["name"]);
      const type = String(row["type"] ?? "");
      const pk = Number(row["pk"] ?? 0);
      colTypes[name] = normalizeType("sqlite", type);
      if (pk > 0) pkCols.push(name);
    }

    const fkRes = await adapter.query(`PRAGMA foreign_key_list(${qName});`);
    const fks: Array<[string, string, string]> = fkRes.rows
      .map((r) => {
        const from = String(r["from"]);
        const refTable = String(r["table"]);
        const to = String(r["to"]);
        if (!from || !refTable || !to) return null;
        return [from, refTable, to] as [string, string, string];
      })
      .filter(stripNullish);

    let sample: Array<Record<string, unknown>> = [];
    try {
      const sRes = await adapter.query(`SELECT * FROM ${qName} LIMIT ${sampleRows};`);
      sample = sRes.rows.map((r) => {
        const safe = jsonSafe(r) as Record<string, unknown>;
        return coerceRowToSchema(safe, colTypes);
      });
    } catch {
      sample = [];
    }

    const tr: TableResume = { c: colTypes, s: sample };
    if (pkCols.length) tr.pk = pkCols;
    if (fks.length) tr.fk = fks;
    return tr;
  }

  if (adapter.dialect === "postgres") {
    const schema = table.schema || "public";
    const name = table.name;

    const colsRes = await adapter.query(
      `
      SELECT column_name, data_type, udt_name
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position;
      `,
      [schema, name]
    );

    const colTypes: Record<string, NormalizedType> = {};
    for (const row of colsRes.rows) {
      const col = String(row["column_name"]);
      const dt = String(row["data_type"] ?? "");
      const udt = String(row["udt_name"] ?? "");
      colTypes[col] = normalizeType("postgres", dt, { udtName: udt });
    }

    const pkRes = await adapter.query(
      `
      SELECT kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2
      ORDER BY kcu.ordinal_position;
      `,
      [schema, name]
    );

    const pkCols = pkRes.rows.map((r) => String(r["column_name"])).filter((x) => x.length > 0);

    const fkRes = await adapter.query(
      `
      SELECT
        kcu.column_name AS from_column,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
       AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2;
      `,
      [schema, name]
    );

    const fks: Array<[string, string, string]> = fkRes.rows
      .map((r) => {
        const from = String(r["from_column"]);
        const refSchema = String(r["foreign_table_schema"]);
        const refTable = String(r["foreign_table_name"]);
        const refCol = String(r["foreign_column"]);
        if (!from || !refTable || !refCol) return null;
        return [from, `${refSchema}.${refTable}`, refCol] as [string, string, string];
      })
      .filter(stripNullish);

    let sample: Array<Record<string, unknown>> = [];
    try {
      const fq = `${quoteIdent("postgres", schema)}.${quoteIdent("postgres", name)}`;
      const sRes = await adapter.query(`SELECT * FROM ${fq} LIMIT ${sampleRows};`);
      sample = sRes.rows.map((r) => {
        const safe = jsonSafe(r) as Record<string, unknown>;
        return coerceRowToSchema(safe, colTypes);
      });
    } catch {
      sample = [];
    }

    const tr: TableResume = { c: colTypes, s: sample };
    if (pkCols.length) tr.pk = pkCols;
    if (fks.length) tr.fk = fks;
    return tr;
  }

  if (adapter.dialect === "mysql") {
    const schema = table.schema || "";
    const name = table.name;

    const colsRes = await adapter.query(
      `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = ?
        AND table_name = ?
      ORDER BY ordinal_position;
      `,
      [schema, name]
    );

    const colTypes: Record<string, NormalizedType> = {};
    for (const row of colsRes.rows) {
      const col = String(row["column_name"]);
      const dt = String(row["data_type"] ?? "");
      colTypes[col] = normalizeType("mysql", dt);
    }

    const pkRes = await adapter.query(
      `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = ?
        AND table_name = ?
        AND column_key = 'PRI'
      ORDER BY ordinal_position;
      `,
      [schema, name]
    );

    const pkCols = pkRes.rows.map((r) => String(r["column_name"])).filter((x) => x.length > 0);

    const fkRes = await adapter.query(
      `
      SELECT
        column_name,
        referenced_table_schema,
        referenced_table_name,
        referenced_column_name
      FROM information_schema.key_column_usage
      WHERE table_schema = ?
        AND table_name = ?
        AND referenced_table_name IS NOT NULL;
      `,
      [schema, name]
    );

    const fks: Array<[string, string, string]> = fkRes.rows
      .map((r) => {
        const from = String(r["column_name"]);
        const refSchema = String(r["referenced_table_schema"]);
        const refTable = String(r["referenced_table_name"]);
        const refCol = String(r["referenced_column_name"]);
        if (!from || !refTable || !refCol) return null;
        return [from, `${refSchema}.${refTable}`, refCol] as [string, string, string];
      })
      .filter(stripNullish);

    let sample: Array<Record<string, unknown>> = [];
    try {
      const fq = schema
        ? `${quoteIdent("mysql", schema)}.${quoteIdent("mysql", name)}`
        : quoteIdent("mysql", name);

      const sRes = await adapter.query(`SELECT * FROM ${fq} LIMIT ${sampleRows};`);
      sample = sRes.rows.map((r) => {
        const safe = jsonSafe(r) as Record<string, unknown>;
        return coerceRowToSchema(safe, colTypes);
      });
    } catch {
      sample = [];
    }

    const tr: TableResume = { c: colTypes, s: sample };
    if (pkCols.length) tr.pk = pkCols;
    if (fks.length) tr.fk = fks;
    return tr;
  }

  if (adapter.dialect === "duckdb") {
    const schema = table.schema || "main";
    const name = table.name;

    // DuckDB doesn't support parameterized queries on information_schema
    // Use safe string escaping instead
    const escSchema = schema.replace(/'/g, "''");
    const escName = name.replace(/'/g, "''");

    const colsRes = await adapter.query(
      `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = '${escSchema}'
        AND table_name = '${escName}'
      ORDER BY ordinal_position;
      `
    );

    const colTypes: Record<string, NormalizedType> = {};
    for (const row of colsRes.rows) {
      const col = String(row["column_name"]);
      const dt = String(row["data_type"] ?? "");
      colTypes[col] = normalizeType("duckdb", dt);
    }

    let pkCols: string[] = [];
    try {
      const pkRes = await adapter.query(
        `
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = '${escSchema}'
          AND tc.table_name = '${escName}'
        ORDER BY kcu.ordinal_position;
        `
      );
      pkCols = pkRes.rows.map((r) => String(r["column_name"])).filter((x) => x.length > 0);
    } catch {
      pkCols = [];
    }

    let fks: Array<[string, string, string]> = [];
    try {
      const fkRes = await adapter.query(
        `
        SELECT
          kcu.column_name AS from_column,
          ccu.table_schema AS foreign_table_schema,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = '${escSchema}'
          AND tc.table_name = '${escName}';
        `
      );

      fks = fkRes.rows
        .map((r) => {
          const from = String(r["from_column"]);
          const refSchema = String(r["foreign_table_schema"]);
          const refTable = String(r["foreign_table_name"]);
          const refCol = String(r["foreign_column"]);
          if (!from || !refTable || !refCol) return null;
          return [from, `${refSchema}.${refTable}`, refCol] as [string, string, string];
        })
        .filter(stripNullish);
    } catch {
      fks = [];
    }

    let sample: Array<Record<string, unknown>> = [];
    try {
      const fq = `${quoteIdent("duckdb", schema)}.${quoteIdent("duckdb", name)}`;
      const sRes = await adapter.query(`SELECT * FROM ${fq} LIMIT ${sampleRows};`);
      sample = sRes.rows.map((r) => {
        const safe = jsonSafe(r) as Record<string, unknown>;
        return coerceRowToSchema(safe, colTypes);
      });
    } catch {
      sample = [];
    }

    const tr: TableResume = { c: colTypes, s: sample };
    if (pkCols.length) tr.pk = pkCols;
    if (fks.length) tr.fk = fks;
    return tr;
  }

  return { c: {}, s: [] };
}
