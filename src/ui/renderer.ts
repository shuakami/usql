import pc from "picocolors";
import Table from "cli-table3";
import type { Dialect, QueryResult } from "../drivers/adapter";

const NO_COLOR = !!process.env.NO_COLOR || process.argv.includes("--no-color");

function color<T extends string>(fn: (s: T) => string, s: T): string {
  return NO_COLOR ? s : fn(s);
}

function clampString(input: string, max: number): string {
  if (input.length <= max) return input;
  if (max <= 3) return "...";
  return input.slice(0, Math.max(0, max - 3)) + "...";
}

function dialectLabel(d: Dialect | null | undefined): string {
  switch (d) {
    case "sqlite":
      return "sqlite";
    case "postgres":
      return "pg";
    case "mysql":
      return "mysql";
    case "duckdb":
      return "duckdb";
    default:
      return "sql";
  }
}

function dialectColor(d: Dialect | null | undefined): (s: string) => string {
  if (NO_COLOR) return (s: string) => s;
  switch (d) {
    case "sqlite":
      return pc.yellow;
    case "postgres":
      return pc.blue;
    case "mysql":
      return pc.magenta;
    case "duckdb":
      return pc.green;
    default:
      return (s: string) => s;
  }
}

function getTerminalWidth(): number {
  return process.stdout.columns || 80;
}

export function safeJsonStringify(obj: unknown): string {
  return JSON.stringify(obj, (_key, value) => {
    if (typeof value === "bigint") return value.toString();
    return value as unknown;
  });
}

export interface RendererOptions {
  dialect?: Dialect;
  json?: boolean;
  quiet?: boolean;
}

export class Renderer {
  private readonly dialect?: Dialect;
  private readonly json: boolean;
  private readonly quiet: boolean;
  private readonly dialectColorFn: (s: string) => string;

  constructor(opts: RendererOptions = {}) {
    this.dialect = opts.dialect;
    this.json = !!opts.json;
    this.quiet = !!opts.quiet;
    this.dialectColorFn = dialectColor(opts.dialect);
  }

  get isJson(): boolean {
    return this.json;
  }

  get isTTY(): boolean {
    return !!process.stderr.isTTY;
  }

  prompt(): string {
    const label = dialectLabel(this.dialect);
    return this.dialectColorFn(`${label}>`) + " ";
  }

  info(msg: string): void {
    if (this.quiet || this.json) return;
    process.stderr.write(color(pc.dim, msg) + "\n");
  }

  success(msg: string): void {
    if (this.quiet || this.json) return;
    process.stderr.write(color(pc.green, msg) + "\n");
  }

  warn(msg: string): void {
    if (this.quiet || this.json) return;
    process.stderr.write(color(pc.yellow, msg) + "\n");
  }

  error(msg: string): void {
    process.stderr.write(color(pc.red, msg) + "\n");
  }

  spinner(message: string): { stop: (ok: boolean, finalText?: string) => void } {
    if (this.quiet) {
      return { stop: () => undefined };
    }

    if (!this.isTTY) {
      this.info(`- ${message}`);
      return {
        stop: (ok: boolean, finalText?: string) => {
          if (ok) this.success(`+ ${finalText || "done"}`);
          else this.error(`x ${finalText || "failed"}`);
        }
      };
    }

    const frames = ["-", "\\", "|", "/"];
    let i = 0;
    let stopped = false;

    const render = () => {
      const frame = frames[i++ % frames.length];
      const line = color(pc.dim, `${frame} ${message}`);
      process.stderr.write("\r" + line);
    };

    render();
    const timer = setInterval(render, 100);

    return {
      stop: (ok: boolean, finalText?: string) => {
        if (stopped) return;
        stopped = true;
        clearInterval(timer);

        const clearWidth = Math.min(160, message.length + 8);
        process.stderr.write("\r" + " ".repeat(clearWidth) + "\r");

        if (ok) this.success(`+ ${finalText || "done"}`);
        else this.error(`x ${finalText || "failed"}`);
      }
    };
  }

  renderQueryResult(result: QueryResult): void {
    if (this.json || !process.stdout.isTTY) {
      process.stdout.write(safeJsonStringify(result) + "\n");
      return;
    }

    const rows = result.rows ?? [];
    const columns = result.columns ?? [];

    if (!rows.length) {
      const affected = typeof result.rowCount === "number" ? ` (${result.rowCount} affected)` : "";
      this.success(`+ ok${color(pc.dim, affected)}`);
      return;
    }

    const tableText = this.renderMinimalTable(columns, rows);
    process.stdout.write(tableText + "\n");
    process.stdout.write(color(pc.dim, `  ${rows.length} row(s)`) + "\n");
  }

  private renderMinimalTable(columns: string[], rows: Array<Record<string, unknown>>): string {
    const termWidth = getTerminalWidth();
    const colCount = columns.length || 1;
    const maxCell = Math.max(10, Math.floor((termWidth - colCount * 2) / colCount));

    const t = new Table({
      head: columns.map((c) => (NO_COLOR ? c : pc.bold(c))),
      style: {
        head: [],
        border: [],
        compact: true,
        "padding-left": 0,
        "padding-right": 1
      },
      chars: {
        top: "",
        "top-mid": "",
        "top-left": "",
        "top-right": "",
        bottom: "",
        "bottom-mid": "",
        "bottom-left": "",
        "bottom-right": "",
        left: "",
        "left-mid": "",
        mid: "-",
        "mid-mid": " ",
        right: "",
        "right-mid": "",
        middle: " "
      }
    });

    for (const r of rows) {
      const row = columns.map((c) => this.formatCell(r[c], maxCell));
      t.push(row);
    }

    return t.toString();
  }

  private formatCell(value: unknown, max: number): string {
    if (value === null) return color(pc.dim, "NULL");
    if (value === undefined) return color(pc.dim, "-");

    if (typeof value === "string") return clampString(value, max);
    if (typeof value === "number") return String(value);
    if (typeof value === "boolean") return value ? "true" : "false";
    if (typeof value === "bigint") return value.toString();

    if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
      return color(pc.dim, `<bin ${value.length}b>`);
    }

    try {
      const s = JSON.stringify(value);
      return clampString(s, max);
    } catch {
      return clampString(String(value), max);
    }
  }
}
