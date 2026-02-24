# fb-cli

Command-line client for [Firebolt](https://www.firebolt.io/) and [Firebolt Core](https://github.com/firebolt-db/firebolt-core).

## Quick Start

```
fb --core "SELECT 42"
```

```
fb --host api.us-east-1.app.firebolt.io \
   --extra account_id=<account_id> \
   --jwt '<token>' \
   -d <database>
```

## Install

1. Install `cargo`: https://doc.rust-lang.org/cargo/getting-started/installation.html
2. Install system dependencies (Ubuntu): `sudo apt install pkg-config libssl-dev`
3. Clone, build, and install:

```
git clone git@github.com:firebolt-db/fb-cli.git
cd fb-cli
cargo install --path . --locked
```

Run `fb` (or `~/.cargo/bin/fb` if cargo's bin directory is not on `$PATH`).

## Usage

**Single query (non-interactive):**
```
fb "SELECT 42"
fb -c "SELECT 42"
fb --core "SELECT * FROM information_schema.tables LIMIT 5"
```

**Interactive REPL:**
```
fb
fb --core
```

**Pipe mode:**
```
echo "SELECT 1; SELECT 2;" | fb --core
cat queries.sql | fb --core
```

## Interactive REPL

The REPL uses a full-terminal layout:

```
┌───────────────────────────────────────────────┐
│ OUTPUT  (scrollable)                          │
│  previous queries, results, timing, errors    │
│                                               │
├───────────────────────────────────────────────┤
│ INPUT  (multi-line editor)                    │
│  SELECT *                                     │
│  FROM orders                                  │
│                                               │
├───────────────────────────────────────────────┤
│  localhost:3473 | firebolt    Tab complete … │
└───────────────────────────────────────────────┘
```

Enter submits the query when SQL is complete (ends with `;`). An incomplete statement gets a newline instead, allowing natural multi-line editing.

### Key Bindings

| Key | Action |
|-----|--------|
| `Enter` | Submit query (if SQL complete) or insert newline |
| `Ctrl+C` | Cancel current input, or cancel an in-flight query |
| `Ctrl+D` | Exit |
| `Ctrl+Up` / `Ctrl+Down` | Navigate history (older / newer) |
| `Ctrl+R` | Reverse history search |
| `Tab` | Open completion popup (or advance to next item) |
| `Shift+Tab` | Navigate completion popup backward |
| `Ctrl+Space` | Fuzzy schema search (tables, columns, functions) |
| `Ctrl+V` | Open last result in interactive viewer (csvlens) |
| `Alt+F` | Format SQL in the editor (uppercase keywords, 2-space indent) |
| `Page Up` / `Page Down` | Scroll output pane |
| `Ctrl+H` | Show help popup |
| `Escape` | Close any open popup |

### Tab Completion

Tab completion suggests table names, column names, and functions based on the current cursor context (FROM clause, SELECT list, WHERE clause, etc.). The schema is fetched from the server and cached for `--completion-cache-ttl` seconds (default 300).

When all suggestions share a common prefix, that prefix is completed immediately (bash-style). A second Tab opens the popup to choose among remaining candidates.

`Ctrl+Space` opens a fuzzy search overlay that searches the full schema regardless of cursor context.

### Ctrl+R History Search

Incremental reverse search over the session history. Type to filter; `Ctrl+R` again for the next older match; `Up`/`Down` to navigate matches; `Enter` to accept; `Esc` to cancel.

### Ctrl+V Viewer

Opens the last query result in [csvlens](https://github.com/YS-L/csvlens) — a full-screen interactive table viewer with sorting, filtering, and fuzzy search. Requires a client-side output format (`client:*`).

## Slash Commands

Type these directly in the REPL (or pass with `-c`):

| Command | Description |
|---------|-------------|
| `/help` | Show help popup |
| `/refresh` | Refresh the schema completion cache |
| `/view` | Open last result in csvlens viewer (same as `Ctrl+V`) |
| `/qh [limit] [minutes]` | Show recent query history. Default: 100 rows, last 60 minutes |
| `/benchmark [N] <query>` | Benchmark a query: 1 warmup + N timed runs (default N=3) |
| `set key=value;` | Set a query parameter |
| `unset key;` | Remove a query parameter |
| `quit` / `exit` | Exit the REPL |

### `/qh` — Query History

```
/qh           -- last 100 queries from the past hour
/qh 50        -- last 50 queries from the past hour
/qh 200 1440  -- last 200 queries from the past day
```

### `/benchmark` — Benchmark Mode

Runs a query multiple times, discards the first result as warmup, and reports timing statistics. Result cache is automatically disabled for accurate measurements.

```
/benchmark SELECT count(*) FROM large_table
/benchmark 10 SELECT count(*) FROM large_table
```

Output example:
```
  warmup: 412.3ms
  run 1/3: 398.1ms  [result rows...]
  run 2/3: 401.7ms
  run 3/3: 395.4ms
Results: min=395.4ms  avg=398.4ms  p90=401.7ms  max=401.7ms
```

`Ctrl+C` cancels a benchmark in progress.

## Output Formats

### Client-Side Rendering (default)

The default format is `client:auto`, which fetches results as JSON and renders them in the terminal. The layout adapts to the number of columns:

- **Horizontal** (few columns): standard bordered table
- **Vertical** (many columns or `client:vertical`): one column per row
- **Auto** (`client:auto`): automatically picks horizontal or vertical based on column count and terminal width

```
=> SELECT query_id, status, duration_usec FROM information_schema.engine_query_history LIMIT 3;
+------------------+---------+--------------+
| query_id         | status  | duration_usec |
+==================+=========+==============+
| abc123...        | ENDED   |        41051 |
| def456...        | ENDED   |        40117 |
| ghi789...        | ENDED   |        87747 |
+------------------+---------+--------------+
Time: 15.2ms
```

Override with `--format client:vertical` or set at runtime: `set format = client:vertical;`

### Server-Side Rendering

Pass any Firebolt output format name (without a `client:` prefix) to receive raw server-rendered output:

```
fb --format PSQL "SELECT 42"
fb --format JSON "SELECT 42"
fb --format TabSeparatedWithNamesAndTypes "SELECT 42"
```

Common formats: `PSQL`, `JSON`, `JSON_Compact`, `JSONLines_Compact`, `CSV`, `CSVWithNames`, `TabSeparatedWithNames`, `TabSeparatedWithNamesAndTypes`.

### Changing Format at Runtime

```sql
set format = client:vertical;     -- client-side vertical
set format = JSON;                 -- server-side JSON
unset format;                      -- reset to default (client:auto)
```

## Set and Unset

Change query parameters at runtime without restarting:

```sql
set database = my_db;
set engine = my_engine;
set enable_result_cache = false;
unset enable_result_cache;
```

Active non-default settings are shown in grey between the query echo and its result.

## Defaults

Save your preferred flags so you don't have to repeat them:

```
fb --host my-host.firebolt.io --update-defaults
```

Saved defaults are stored in `~/.firebolt/fb_config` and merged with any flags you provide at run time. The `--format` flag is intentionally not saved to defaults — always specify it explicitly if you need a non-default format.

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All queries succeeded |
| `1` | One or more queries failed (bad SQL, permission denied, HTTP 400) |
| `2` | System/infrastructure error (connection refused, auth failure, HTTP 4xx/5xx) |

This distinction is useful in scripts:

```bash
fb --core "SELECT ..."
case $? in
  0) echo "ok" ;;
  1) echo "query error — check your SQL" ;;
  2) echo "system error — check connection or credentials" ;;
esac
```

## Firebolt Core

`--core` is a shortcut for `--host localhost:3473 --database firebolt` with no authentication required:

```
fb --core
fb --core "SELECT 42"
```

## Authentication

### JWT

```
fb --jwt 'eyJhbGci...'
fb --jwt-from-file   # reads ~/.firebolt/jwt
```

### Service Account

```
fb --sa-id <id> --sa-secret <secret> --oauth-env app \
   --host <account_id>.api.us-east-1.app.firebolt.io \
   -d <database>
```

The token is cached in `~/.firebolt/fb_sa_token` and reused for up to 30 minutes.

Read more: [Firebolt Service Accounts](https://docs.firebolt.io/guides/managing-your-organization/service-accounts)

## All Flags

```
Usage: fb [OPTIONS] [query...]

Positional arguments:
  query                         Query to execute (starts REPL if omitted)

Optional arguments:
  -c, --command COMMAND         Run a single command and exit
  -C, --core                    Connect to Firebolt Core (localhost:3473)
  -h, --host HOSTNAME           Hostname and port
  -d, --database DATABASE       Database name
  -f, --format FORMAT           Output format (client:auto, client:vertical,
                                client:horizontal, PSQL, JSON, CSV, ...)
  -e, --extra NAME=VALUE        Extra query parameters (repeatable)
  -l, --label LABEL             Query label for tracking
  -j, --jwt JWT                 JWT token for authentication
  --sa-id SA-ID                 Service Account ID
  --sa-secret SA-SECRET         Service Account Secret
  --jwt-from-file               Load JWT from ~/.firebolt/jwt
  --oauth-env ENV               OAuth environment: app or staging (default: staging)
  -v, --verbose                 Verbose output (shows URL, query text)
  --concise                     Suppress timing statistics
  --hide-pii                    Hide URLs containing query parameters
  --no-spinner                  Disable the query spinner
  --no-color                    Disable syntax highlighting
  --no-completion               Disable tab completion
  --completion-cache-ttl SECS   Schema cache TTL in seconds (default: 300)
  --min-col-width N             Min column width before vertical mode (default: 15)
  --max-cell-length N           Max cell content length before truncation (default: 1000)
  --update-defaults             Save current flags as defaults
  -V, --version                 Print version
  --help                        Show help
```

## Files

| Path | Purpose |
|------|---------|
| `~/.firebolt/fb_config` | Saved defaults (YAML) |
| `~/.firebolt/fb_history` | REPL history (up to 10,000 entries) |
| `~/.firebolt/fb_sa_token` | Cached service account token |
| `~/.firebolt/jwt` | JWT token file (used with `--jwt-from-file`) |

## License

See [LICENSE](LICENSE.md).
