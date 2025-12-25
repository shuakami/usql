# usql

Feed your database to LLMs. Query anything from terminal.

![usql](demo.png)

```bash
npm i -g @sdjz/usql
```

## Give AI Your Database Context

One command to generate a token-optimized schema dump for ChatGPT, Claude, or any LLM:

```bash
usql inspect ./myapp.db
```

```json
{"v":1,"d":"sqlite","t":{"users":{"c":{"id":"s","name":"s","email":"s"},"pk":["id"],"fk":[],"s":[{"id":"1","name":"Alice"}]}}}
```

Paste this into your AI chat. It now understands your schema, relationships, and sample data.

```bash
# Pretty print for humans
usql inspect postgres://localhost/prod --pretty

# Control sample size
usql inspect ./app.db --rows 5
```

## Query Any Database

```bash
usql ./data.db "SELECT * FROM users"
usql postgres://user:pass@host/db "SELECT now()"
usql mysql://user:pass@host/db "SHOW TABLES"
usql ./analytics.parquet "SELECT * FROM data LIMIT 10"
```

Drivers auto-install on first use. Zero config.

## Interactive REPL

```bash
usql ./app.db
```

```
sqlite> .tables
users
orders

sqlite> SELECT * FROM users LIMIT 3;
id  name   email
--- ------ -----------------
1   Alice  alice@example.com
2   Bob    bob@example.com
  2 row(s)
```

## Pipe to Anything

Output is JSON when piped. Works with jq, scripts, CI/CD:

```bash
usql ./app.db "SELECT * FROM users" | jq '.rows | length'
```

## Supported

| Database | Example |
|----------|---------|
| SQLite | `./file.db` |
| PostgreSQL | `postgres://user:pass@host/db` |
| MySQL | `mysql://user:pass@host/db` |
| DuckDB | `duckdb:./file.duckdb` |
| Parquet | `./file.parquet` |
| SQL Dump | `./dump.sql` |

## Commands

```
.tables       list tables
.schema <t>   columns
.sample <t>   preview rows
.count <t>    row count
.indexes <t>  indexes
.time         query timing
.export csv   export last result
.inspect      AI schema dump
```

---

MIT
