import { cac } from "cac";
import readline from "node:readline";
import { createRequire } from "node:module";
import pc from "picocolors";

import { parseConnectionString } from "./core/driver-manager";
import { inspectDatabase, listTableNames } from "./core/introspector";
import { Renderer, safeJsonStringify } from "./ui/renderer";

import type { Adapter } from "./drivers/adapter";
import { quoteIdent } from "./drivers/adapter";
import { PostgresAdapter } from "./drivers/postgres";
import { createSqliteAdapter } from "./drivers/sqlite";
import { MySqlAdapter } from "./drivers/mysql";
import { DuckDbAdapter } from "./drivers/duckdb";

const require = createRequire(__filename);
const pkg = require("../package.json") as { version?: string };

function isPipeLike(): boolean {
  return !process.stdout.isTTY;
}

function redactConnString(conn: string): string {
  try {
    const u = new URL(conn);
    if (u.password) u.password = "***";
    return u.toString();
  } catch {
    return conn;
  }
}

async function createAdapterForConn(conn: string, renderer: Renderer): Promise<Adapter> {
  const parsed = parseConnectionString(conn);

  switch (parsed.dialect) {
    case "postgres":
      return PostgresAdapter.create(parsed.connectionString, renderer);
    case "sqlite":
      return createSqliteAdapter(parsed.connectionString, renderer);
    case "mysql":
      return MySqlAdapter.create(parsed.connectionString, renderer);
    case "duckdb":
      return DuckDbAdapter.create(parsed.connectionString, renderer);
    default:
      throw new Error(`unsupported dialect: ${parsed.dialect}`);
  }
}

function getConnectionLabel(conn: string, dialect: string): string {
  const lower = conn.toLowerCase();
  if (lower.startsWith("parquet:") || lower.endsWith(".parquet") || lower.endsWith(".pq")) {
    return "parquet";
  }
  if (lower.startsWith("sqldump:") || lower.endsWith(".sql")) {
    return "sqldump";
  }
  return dialect;
}

async function runSingleQuery(
  conn: string,
  sql: string,
  opts: { json?: boolean; quiet?: boolean; full?: boolean; format?: string }
): Promise<void> {
  const parsed = parseConnectionString(conn);
  const format = opts.format || "table";
  const jsonMode = format === "json" || isPipeLike();
  const renderer = new Renderer({ dialect: parsed.dialect, json: jsonMode, quiet: !!opts.quiet, full: !!opts.full });

  let adapter: Adapter | null = null;

  try {
    adapter = await createAdapterForConn(conn, renderer);
    const label = getConnectionLabel(conn, parsed.dialect);

    await adapter.connect();

    if (!renderer.isJson && !opts.quiet) {
      renderer.success(`+ connected [${label}]`);
    }

    const res = await adapter.query(sql);
    
    // Handle different output formats
    if (format === "csv") {
      outputCsv(res);
    } else {
      renderer.renderQueryResult(res);
    }
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    renderer.error(`x ${msg}`);
    process.exitCode = 1;
  } finally {
    await adapter?.close().catch(() => undefined);
  }
}

function outputCsv(result: { columns: string[]; rows: Array<Record<string, unknown>> }): void {
  const { columns, rows } = result;
  if (!columns.length) return;
  
  const escape = (v: unknown): string => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (s.includes(",") || s.includes('"') || s.includes("\n") || s.includes("\r")) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };
  
  process.stdout.write(columns.join(",") + "\n");
  for (const row of rows) {
    process.stdout.write(columns.map(c => escape(row[c])).join(",") + "\n");
  }
}

function shouldRefreshTables(sql: string): boolean {
  const s = sql.trim().toLowerCase();
  return (
    s.startsWith("create ") ||
    s.startsWith("drop ") ||
    s.startsWith("alter ") ||
    s.startsWith("pragma ") ||
    s.startsWith("attach ") ||
    s.startsWith("detach ")
  );
}

function printReplHelp(): void {
  const lines = [
    pc.bold("Commands:"),
    pc.dim("  .help             show this help"),
    pc.dim("  .tables           list all tables"),
    pc.dim("  .schema <tbl>     show table schema"),
    pc.dim("  .sample <tbl>     preview first 5 rows"),
    pc.dim("  .count <tbl>      count rows in table"),
    pc.dim("  .indexes <tbl>    show table indexes"),
    pc.dim("  .export <format>  export last result (csv/json)"),
    pc.dim("  .time             toggle query timing"),
    pc.dim("  .full             toggle full content display (no truncation)"),
    pc.dim("  .inspect          output AI resume JSON"),
    pc.dim("  .clear            clear screen"),
    pc.dim("  .quit             exit repl"),
    ""
  ];
  process.stdout.write(lines.join("\n"));
}

async function startRepl(conn: string, opts: { quiet?: boolean; full?: boolean }): Promise<void> {
  const parsed = parseConnectionString(conn);
  const renderer = new Renderer({ dialect: parsed.dialect, json: false, quiet: !!opts.quiet, full: !!opts.full });

  const adapter = await createAdapterForConn(conn, renderer);
  const label = getConnectionLabel(conn, parsed.dialect);

  try {
    await adapter.connect();
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    renderer.error(`x connection failed: ${msg}`);
    await adapter.close().catch(() => undefined);
    process.exitCode = 1;
    return;
  }

  renderer.success(`+ connected [${label}]`);
  renderer.info(`  ${redactConnString(conn)}`);
  renderer.info(pc.dim("  type .help for commands"));

  let tables: string[] = [];
  let showTiming = false;
  let showFull = !!opts.full;
  let lastResult: { columns: string[]; rows: Array<Record<string, unknown>> } | null = null;

  const refreshTables = async (): Promise<void> => {
    try {
      tables = await listTableNames(adapter);
    } catch {
      tables = [];
    }
  };

  await refreshTables();

  const completer = (line: string): [string[], string] => {
    const trimmed = line.trimStart();

    if (trimmed.startsWith(".")) {
      const cmds = [
        ".help",
        ".tables",
        ".schema ",
        ".sample ",
        ".count ",
        ".indexes ",
        ".export ",
        ".time",
        ".full",
        ".inspect",
        ".clear",
        ".quit"
      ];
      const hits = cmds.filter((c) => c.startsWith(trimmed));
      return [hits.length ? hits : cmds, trimmed];
    }

    const last = trimmed.split(/\s+/).pop() || "";
    const pool = tables || [];
    const hits = pool.filter((t) => t.toLowerCase().startsWith(last.toLowerCase()));
    return [hits.length ? hits : pool.slice(0, 20), last];
  };

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true,
    completer,
    historySize: 100
  });

  rl.setPrompt(renderer.prompt());
  rl.prompt();

  const closeAll = async () => {
    rl.close();
    await adapter.close().catch(() => undefined);
  };

  rl.on("SIGINT", async () => {
    renderer.info(pc.dim("^C"));
    await closeAll();
    process.exitCode = 130;
  });

  for await (const line of rl) {
    const trimmed = line.trim();

    if (!trimmed) {
      rl.prompt();
      continue;
    }

    // .quit / .exit / .q
    if (trimmed === ".quit" || trimmed === ".exit" || trimmed === ".q") {
      await closeAll();
      break;
    }

    // .help / .h
    if (trimmed === ".help" || trimmed === ".h") {
      printReplHelp();
      rl.prompt();
      continue;
    }

    // .clear
    if (trimmed === ".clear" || trimmed === ".cls") {
      process.stdout.write("\x1b[2J\x1b[H");
      rl.prompt();
      continue;
    }

    // .tables
    if (trimmed === ".tables") {
      await refreshTables();
      if (!tables.length) {
        renderer.warn("- no tables found");
      } else {
        process.stdout.write(tables.join("\n") + "\n");
      }
      rl.prompt();
      continue;
    }

    // .count <table>
    if (trimmed.startsWith(".count ")) {
      const tableName = trimmed.slice(".count ".length).trim();
      if (!tableName) {
        renderer.warn("- usage: .count <table_name>");
        rl.prompt();
        continue;
      }
      try {
        const quoted = quoteIdent(parsed.dialect, tableName);
        const res = await adapter.query(`SELECT COUNT(*) AS count FROM ${quoted};`);
        const count = res.rows[0]?.["count"] ?? res.rows[0]?.["COUNT(*)"] ?? "?";
        renderer.success(`+ ${count} rows`);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        renderer.error(`x ${msg}`);
      }
      rl.prompt();
      continue;
    }

    // .schema <table>
    if (trimmed.startsWith(".schema ")) {
      const tableName = trimmed.slice(".schema ".length).trim();
      if (!tableName) {
        renderer.warn("- usage: .schema <table_name>");
        rl.prompt();
        continue;
      }
      try {
        let schemaQuery: string;
        if (parsed.dialect === "sqlite") {
          schemaQuery = `PRAGMA table_info("${tableName.replace(/"/g, '""')}");`;
        } else if (parsed.dialect === "postgres") {
          schemaQuery = `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '${tableName.replace(/'/g, "''")}' ORDER BY ordinal_position;`;
        } else if (parsed.dialect === "mysql") {
          schemaQuery = `DESCRIBE \`${tableName.replace(/`/g, "``")}\`;`;
        } else {
          // DuckDB: limit to current database to avoid duplicates from attached databases
          schemaQuery = `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '${tableName.replace(/'/g, "''")}' AND table_catalog = current_database() ORDER BY ordinal_position;`;
        }
        const res = await adapter.query(schemaQuery);
        renderer.renderQueryResult(res);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        renderer.error(`x ${msg}`);
      }
      rl.prompt();
      continue;
    }

    // .inspect
    if (trimmed === ".inspect") {
      try {
        renderer.info("- inspecting database...");
        const resume = await inspectDatabase(adapter, { sampleRows: 3 });
        process.stdout.write(JSON.stringify(resume) + "\n");
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        renderer.error(`x inspect failed: ${msg}`);
      }
      rl.prompt();
      continue;
    }

    // .sample <table> - preview first 5 rows
    if (trimmed.startsWith(".sample ")) {
      const tableName = trimmed.slice(".sample ".length).trim();
      if (!tableName) {
        renderer.warn("- usage: .sample <table_name>");
        rl.prompt();
        continue;
      }
      try {
        const quoted = quoteIdent(parsed.dialect, tableName);
        const res = await adapter.query(`SELECT * FROM ${quoted} LIMIT 5;`);
        lastResult = { columns: res.columns, rows: res.rows };
        renderer.renderQueryResult(res);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        renderer.error(`x ${msg}`);
      }
      rl.prompt();
      continue;
    }

    // .indexes <table> - show table indexes
    if (trimmed.startsWith(".indexes ")) {
      const tableName = trimmed.slice(".indexes ".length).trim();
      if (!tableName) {
        renderer.warn("- usage: .indexes <table_name>");
        rl.prompt();
        continue;
      }
      try {
        let indexQuery: string;
        if (parsed.dialect === "sqlite") {
          indexQuery = `PRAGMA index_list("${tableName.replace(/"/g, '""')}");`;
        } else if (parsed.dialect === "postgres") {
          indexQuery = `SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '${tableName.replace(/'/g, "''")}';`;
        } else if (parsed.dialect === "mysql") {
          indexQuery = `SHOW INDEX FROM \`${tableName.replace(/`/g, "``")}\`;`;
        } else {
          indexQuery = `SELECT * FROM duckdb_indexes() WHERE table_name = '${tableName.replace(/'/g, "''")}';`;
        }
        const res = await adapter.query(indexQuery);
        renderer.renderQueryResult(res);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        renderer.error(`x ${msg}`);
      }
      rl.prompt();
      continue;
    }

    // .time - toggle query timing
    if (trimmed === ".time") {
      showTiming = !showTiming;
      renderer.success(`+ timing ${showTiming ? "on" : "off"}`);
      rl.prompt();
      continue;
    }

    // .full - toggle full content display
    if (trimmed === ".full") {
      showFull = !showFull;
      renderer.setFull(showFull);
      renderer.success(`+ full display ${showFull ? "on" : "off"}`);
      rl.prompt();
      continue;
    }

    // .export <format> - export last result
    if (trimmed.startsWith(".export ")) {
      const format = trimmed.slice(".export ".length).trim().toLowerCase();
      if (!lastResult || !lastResult.rows.length) {
        renderer.warn("- no data to export (run a query first)");
        rl.prompt();
        continue;
      }
      if (format === "json") {
        process.stdout.write(safeJsonStringify(lastResult.rows) + "\n");
        renderer.success(`+ exported ${lastResult.rows.length} rows as JSON`);
      } else if (format === "csv") {
        const header = lastResult.columns.join(",");
        const rows = lastResult.rows.map((r) =>
          lastResult!.columns
            .map((c) => {
              const v = r[c];
              if (v === null || v === undefined) return "";
              const s = String(v);
              return s.includes(",") || s.includes('"') || s.includes("\n")
                ? `"${s.replace(/"/g, '""')}"`
                : s;
            })
            .join(",")
        );
        process.stdout.write(header + "\n" + rows.join("\n") + "\n");
        renderer.success(`+ exported ${lastResult.rows.length} rows as CSV`);
      } else {
        renderer.warn("- usage: .export <csv|json>");
      }
      rl.prompt();
      continue;
    }

    // unknown dot command
    if (trimmed.startsWith(".")) {
      renderer.warn(`- unknown command: ${trimmed.split(" ")[0]}`);
      renderer.info("  type .help for available commands");
      rl.prompt();
      continue;
    }

    // SQL query
    try {
      const startTime = showTiming ? performance.now() : 0;
      const res = await adapter.query(trimmed);

      // Save result for .export
      lastResult = { columns: res.columns, rows: res.rows };

      renderer.renderQueryResult(res);

      if (showTiming) {
        const elapsed = performance.now() - startTime;
        renderer.info(pc.dim(`  ${elapsed.toFixed(1)}ms`));
      }

      if (shouldRefreshTables(trimmed)) {
        await refreshTables();
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      renderer.error(`x ${msg}`);
    }

    rl.prompt();
  }
}

async function main(): Promise<void> {
  const cli = cac("usql");

  cli.option("--json", "Output JSON (auto-enabled when piped)");
  cli.option("--quiet, -q", "Reduce logs (errors still print)");
  cli.option("--color", "Enable colored output", { default: true });
  cli.option("-c, --command <sql>", "Execute SQL command and exit");
  cli.option("--full, --no-truncate", "Show full content without truncation");
  cli.option("--format <format>", "Output format: table (default), csv, json");

  cli.version(pkg.version || "0.0.0");

  cli
    .command("inspect <conn>", "Deep introspection for AI (schema + FKs + sample rows)")
    .option("--pretty, -p", "Pretty-print JSON")
    .option("--rows <n>", "Sample rows per table", { default: 3 })
    .action(async (conn: string, flags: { pretty?: boolean; quiet?: boolean; rows?: number }) => {
      const parsed = parseConnectionString(conn);
      const renderer = new Renderer({ dialect: parsed.dialect, json: true, quiet: !!flags.quiet });

      const adapter = await createAdapterForConn(conn, renderer);

      try {
        await adapter.connect();

        const sampleRows = Math.max(0, Math.min(100, Number(flags.rows) || 3));
        const resume = await inspectDatabase(adapter, { sampleRows });

        if (flags.pretty) {
          process.stdout.write(JSON.stringify(resume, null, 2) + "\n");
        } else {
          process.stdout.write(JSON.stringify(resume) + "\n");
        }
      } finally {
        await adapter.close().catch(() => undefined);
      }
    });

  cli
    .command("[conn] [sql]", "Run SQL or start REPL")
    .action(async (conn?: string, sql?: string, flags?: { json?: boolean; quiet?: boolean; command?: string; full?: boolean; noTruncate?: boolean; format?: string }) => {
      if (!conn) {
        printExtendedHelp();
        process.exitCode = 1;
        return;
      }

      // Support -c/--command option (common SQL CLI convention)
      const sqlToRun = flags?.command || sql;
      const format = flags?.format?.toLowerCase() || (flags?.json ? "json" : "table");

      if (!sqlToRun) {
        await startRepl(conn, { quiet: !!flags?.quiet, full: !!(flags?.full || flags?.noTruncate) });
        return;
      }

      await runSingleQuery(conn, sqlToRun, { 
        json: format === "json",
        quiet: !!flags?.quiet,
        full: !!(flags?.full || flags?.noTruncate),
        format
      });
    });

  cli.help();
  cli.parse();
}

function printExtendedHelp(): void {
  const lines = [
    pc.bold("usql") + " - Query any database from terminal",
    "",
    pc.bold("Usage:"),
    "  usql <connection> [sql]           Run SQL or start REPL",
    "  usql <connection> -c \"SQL\"        Execute SQL (quote-friendly)",
    "  usql inspect <connection>         Generate AI-ready schema dump",
    "",
    pc.bold("Connections:"),
    "  ./file.db                         SQLite",
    "  postgres://user:pass@host/db      PostgreSQL", 
    "  mysql://user:pass@host/db         MySQL",
    "  duckdb:./file.duckdb              DuckDB",
    "  ./file.parquet                    Parquet (via DuckDB)",
    "  ./dump.sql                        MySQL dump (auto-cached)",
    "",
    pc.bold("Options:"),
    "  -c, --command <sql>    Execute SQL and exit",
    "  --format <fmt>         Output: table, csv, json",
    "  --full                 No truncation on long fields",
    "  -q, --quiet            Minimal output",
    "  --json                 JSON output (auto when piped)",
    "",
    pc.bold("REPL Commands:"),
    "  .tables                List all tables",
    "  .schema <table>        Show table structure",
    "  .sample <table>        Preview first 5 rows",
    "  .count <table>         Count rows",
    "  .inspect               AI schema dump",
    "  .full                  Toggle full display",
    "  .export csv|json       Export last result",
    "",
    pc.bold("Examples:"),
    "  usql ./app.db \"SELECT * FROM users\"",
    "  usql ./dump.sql -c \"SELECT COUNT(*) FROM logs\"",
    "  usql inspect ./app.db --pretty",
    "  usql ./data.parquet \"SELECT * FROM data LIMIT 10\"",
    ""
  ];
  process.stderr.write(lines.join("\n"));
}

main().catch((e: unknown) => {
  const msg = e instanceof Error ? e.message : String(e);
  process.stderr.write(pc.red(`x ${msg}`) + "\n");
  process.exitCode = 1;
});
