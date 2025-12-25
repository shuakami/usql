import { Adapter, QueryResult } from "./adapter";
import { loadPackage } from "../core/driver-manager";
import type { Renderer } from "../ui/renderer";

type MySqlFieldLike = { name: string };

type MySqlConnLike = {
  query(sql: string, params?: unknown[]): Promise<[unknown, MySqlFieldLike[]]>;
  end(): Promise<void>;
};

type MySqlPromiseModuleLike = {
  createConnection(connectionString: string): Promise<MySqlConnLike>;
};

export class MySqlAdapter extends Adapter {
  private readonly mysql: MySqlPromiseModuleLike;
  private conn: MySqlConnLike | null = null;

  private constructor(connectionString: string, mysql: MySqlPromiseModuleLike) {
    super("mysql", connectionString);
    this.mysql = mysql;
  }

  static async create(connectionString: string, renderer?: Renderer): Promise<MySqlAdapter> {
    const { module: mysql } = await loadPackage<MySqlPromiseModuleLike>("mysql2/promise", {
      installName: "mysql2",
      label: "mysql driver (mysql2)",
      renderer
    });

    return new MySqlAdapter(connectionString, mysql);
  }

  async connect(): Promise<void> {
    this.conn = await this.mysql.createConnection(this.connectionString);
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    if (!this.conn) throw new Error("MySqlAdapter: not connected");

    const [rows, fields] = await this.conn.query(sql, params);

    if (Array.isArray(rows)) {
      const objRows = rows as Array<Record<string, unknown>>;
      const columns =
        (fields && fields.length ? fields.map((f) => f.name) : null) ??
        (objRows[0] ? Object.keys(objRows[0]) : []);

      return { columns, rows: objRows, rowCount: objRows.length };
    }

    const ok = rows as Record<string, unknown>;
    const affected =
      typeof ok["affectedRows"] === "number"
        ? (ok["affectedRows"] as number)
        : undefined;

    return { columns: [], rows: [], rowCount: affected };
  }

  async close(): Promise<void> {
    if (!this.conn) return;
    await this.conn.end();
    this.conn = null;
  }
}
