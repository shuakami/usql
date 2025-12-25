# usql

Query any database from your terminal.

![usql](demo.png)

```bash
npm i -g @sdjz/usql
```

```bash
usql ./data.db "SELECT * FROM users"
usql postgres://localhost/mydb "SELECT now()"
usql ./report.parquet "SELECT * FROM data LIMIT 10"
```

Drivers install automatically on first use. No config needed.

## Quick Start

```bash
# Query a SQLite file
usql ./app.db "SELECT * FROM users LIMIT 5"

# Start interactive mode
usql ./app.db
```

```
sqlite> .tables
users
orders

sqlite> SELECT COUNT(*) FROM orders;
count
------
1847
  1 row(s)

sqlite> .quit
```

## AI Mode

Generate a compact schema dump for LLMs:

```bash
usql inspect ./app.db
```

```json
{"v":1,"d":"sqlite","t":{"users":{"c":{"id":"s","name":"s"},"pk":["id"],"s":[{"id":"1","name":"Alice"}]}}}
```

## Databases

| Type | Connection |
|------|------------|
| SQLite | `./file.db` |
| PostgreSQL | `postgres://user:pass@host/db` |
| MySQL | `mysql://user:pass@host/db` |
| DuckDB | `duckdb:./file.duckdb` |
| Parquet | `./file.parquet` |

## Commands

```
.tables         list tables
.schema <t>     show columns
.sample <t>     preview rows
.count <t>      count rows
.indexes <t>    show indexes
.time           toggle timing
.export csv     export result
.inspect        schema for AI
```

## Options

```
--json       JSON output
--quiet      less output
--no-color   no colors
--pretty     format JSON
```

MIT
