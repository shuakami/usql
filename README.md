# usql

Query any database from terminal. Feed your database to LLMs.

![usql](demo.png)

```bash
npm i -g @sdjz/usql
```

## AI Integration

Generate a token-optimized schema dump for ChatGPT, Claude, or any LLM:

```bash
usql inspect ./myapp.db
```

```json
{"v":1,"d":"sqlite","t":{"users":{"c":{"id":"s","name":"s","email":"s"},"pk":["id"],"fk":[],"s":[{"id":"1","name":"Alice"}]}}}
```

Paste this into your AI chat. It now understands your schema, relationships, and sample data.

```bash
usql inspect postgres://localhost/prod --pretty   # Human-readable
usql inspect ./app.db --rows 5                    # More sample rows
```

## Quick Start

```bash
# Query any database
usql ./data.db "SELECT * FROM users"
usql postgres://user:pass@host/db "SELECT now()"
usql mysql://user:pass@host/db "SHOW TABLES"

# Use -c for complex SQL (avoids shell quote issues)
usql ./data.db -c "SELECT * FROM logs WHERE msg LIKE '%error%'"

# Output formats
usql ./data.db "SELECT * FROM users" --format=csv
usql ./data.db "SELECT * FROM users" --format=json
usql ./data.db "SELECT * FROM users" --full  # No truncation
```

Drivers auto-install on first use. Zero config.

## Supported Databases

| Database | Connection |
|----------|------------|
| SQLite | `./file.db` |
| PostgreSQL | `postgres://user:pass@host/db` |
| MySQL | `mysql://user:pass@host/db` |
| DuckDB | `duckdb:./file.duckdb` |
| Parquet | `./file.parquet` |
| MySQL Dump | `./dump.sql` (auto-cached) |

## Interactive REPL

```bash
usql ./app.db
```

```
sqlite> .tables
users orders

sqlite> .schema users
column_name  data_type
----------- ----------
id           INTEGER
name         TEXT
email        TEXT

sqlite> .sample users
id  name   email
--- ------ -----------------
1   Alice  alice@example.com
2   Bob    bob@example.com

sqlite> .inspect
{"v":1,"d":"sqlite",...}   # Paste to AI!
```

### REPL Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables |
| `.schema <t>` | Show table structure |
| `.sample <t>` | Preview first 5 rows |
| `.count <t>` | Count rows |
| `.indexes <t>` | Show indexes |
| `.inspect` | AI schema dump |
| `.full` | Toggle full display |
| `.time` | Toggle query timing |
| `.export csv\|json` | Export last result |

## CLI Options

```
-c, --command <sql>    Execute SQL and exit
--format <fmt>         Output: table (default), csv, json
--full, --no-truncate  Show full content
-q, --quiet            Minimal output
--json                 JSON output (auto when piped)
```

## Pipe to Anything

Output is JSON when piped:

```bash
usql ./app.db "SELECT * FROM users" | jq '.rows | length'
usql ./app.db "SELECT * FROM users" --format=csv > export.csv
```

---

MIT
