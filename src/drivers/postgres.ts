import { Adapter, QueryResult } from "./adapter";
import { loadPackage } from "../core/driver-manager";
import type { Renderer } from "../ui/renderer";

type PgQueryResultLike = {
  rows: Array<Record<string, unknown>>;
  rowCount?: number;
  fields?: Array<{ name: string }>;
};

type PgClientLike = {
  connect(): Promise<void>;
  query(sql: string, params?: unknown[]): Promise<PgQueryResultLike>;
  end(): Promise<void>;
};

type PgModuleLike = {
  Client: new (opts: { connectionString: string }) => PgClientLike;
};

export class PostgresAdapter extends Adapter {
  private readonly pg: PgModuleLike;
  private client: PgClientLike | null = null;

  private constructor(connectionString: string, pg: PgModuleLike) {
    super("postgres", connectionString);
    this.pg = pg;
  }

  static async create(connectionString: string, renderer?: Renderer): Promise<PostgresAdapter> {
    const { module: pg } = await loadPackage<PgModuleLike>("pg", {
      installName: "pg",
      label: "postgres driver (pg)",
      renderer
    });

    return new PostgresAdapter(connectionString, pg);
  }

  async connect(): Promise<void> {
    this.client = new this.pg.Client({ connectionString: this.connectionString });
    await this.client.connect();
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    if (!this.client) throw new Error("PostgresAdapter: not connected");

    const res = await this.client.query(sql, params);
    const columns =
      res.fields?.map((f) => f.name) ??
      (res.rows[0] ? Object.keys(res.rows[0]) : []);

    return {
      columns,
      rows: res.rows ?? [],
      rowCount: typeof res.rowCount === "number" ? res.rowCount : undefined
    };
  }

  async close(): Promise<void> {
    if (!this.client) return;
    await this.client.end();
    this.client = null;
  }
}
