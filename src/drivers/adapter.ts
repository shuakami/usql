export type Dialect = "sqlite" | "postgres" | "mysql" | "duckdb";

export interface QueryResult {
  columns: string[];
  rows: Array<Record<string, unknown>>;
  rowCount?: number;
}

export function quoteIdent(dialect: Dialect, name: string): string {
  if (dialect === "mysql") {
    return `\`${name.replace(/`/g, "``")}\``;
  }
  // SQLite, Postgres, DuckDB use double quotes
  return `"${name.replace(/"/g, '""')}"`;
}

export abstract class Adapter {
  public readonly dialect: Dialect;
  public readonly connectionString: string;

  protected constructor(dialect: Dialect, connectionString: string) {
    this.dialect = dialect;
    this.connectionString = connectionString;
  }

  abstract connect(): Promise<void>;
  abstract query(sql: string, params?: unknown[]): Promise<QueryResult>;
  abstract close(): Promise<void>;
}
