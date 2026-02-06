# fb-cli

Firebolt CLI; work with [Firebolt](https://www.firebolt.io/) and [Firebolt Core](https://github.com/firebolt-db/firebolt-core).

## Examples

```
➤  fb select 42
 ?column?
---------
       42

Time: 41.051ms
```

### REPL

```
➤  fb
=> select 42
 ?column?
---------
       42

Time: 40.117ms

=> create table t (a int);
Table 't' already exists.

Time: 87.747ms

=> insert into t select * from generate_series(1, 100000000);
/
```

Also support history + search in it (`CTRL+R`).

## Output Formats

### Client-Side Rendering

Use `--format client:auto` (default in interactive mode) for pretty table output with smart formatting:

```
=> select * from information_schema.engine_query_history limit 3;
+--------------------+-------------+--------+
| query_id           | query_label | status |
+===============================================+
| abc123...          | NULL        | ENDED  |
| def456...          | my_query    | ENDED  |
| ghi789...          | NULL        | ENDED  |
+--------------------+-------------+--------+
Time: 15.2ms
Scanned: 3 rows, 1.5 KB (1.2 KB local, 300 B remote)
Request Id: xyz...
```

Available client modes:
- `client:auto` - Smart switching between horizontal/vertical layout
- `client:vertical` - Two-column vertical layout for wide tables
- `client:horizontal` - Standard horizontal table

### Interactive Result Exploration

Press `Ctrl+V` then `Enter` (or type `\view`) to open the last query result in an interactive viewer powered by [csvlens](https://github.com/YS-L/csvlens). **Note:** Requires client-side output formats (`client:auto`, `client:vertical`, or `client:horizontal`).

```
=> select * from information_schema.engine_query_history;
[... table output ...]

=> \view
[Opens interactive csvlens viewer with sorting, filtering, and navigation]
```

### Server-Side Rendering

Use format names without prefix for server-rendered output (default in non-interactive/piped mode):
- `PSQL` - PostgreSQL-style format
- `JSON` - JSON output
- `CSV` - CSV format
- `TabSeparatedWithNames` - TSV with headers
- And more...

## Help

```
➤ fb --help
Usage: fb [OPTIONS]

Positional arguments:
  query                    Query command(s) to execute. If not specified, starts the REPL

Optional arguments:
  -c, --command COMMAND    Run a single command and exit
  -C, --core               Preset of settings to connect to Firebolt Core
  -h, --host HOSTNAME      Hostname (and port) to connect to
  -d, --database DATABASE  Database name to use
  -f, --format FORMAT      Output format (client:auto, client:vertical, client:horizontal, TabSeparatedWithNames, PSQL, JSONLines_Compact, ...)
  -e, --extra EXTRA        Extra settings in the form --extra <name>=<value>
  -l, --label LABEL        Query label for tracking or identification
  -j, --jwt JWT            JWT for authentication
  --sa-id SA-ID            Service Account ID for OAuth authentication
  --sa-secret SA-SECRET    Service Account Secret for OAuth authentication
  --jwt-from-file          Load JWT from file (~/.firebolt/jwt)
  --oauth-env OAUTH-ENV    OAuth environment to use (e.g., 'app' or 'staging'). Used for Service Account authentication (default: staging)
  -v, --verbose            Enable extra verbose output
  --concise                Suppress time statistics in output
  --hide-pii               Hide URLs that may contain PII in query parameters
  --no-spinner             Disable the spinner in CLI output
  --update-defaults        Update default configuration values
  -V, --version            Print version
  --help                   Show help message and exit
```

## Install

1) Install `cargo`: https://doc.rust-lang.org/cargo/getting-started/installation.html 
    1) Add `source "$HOME/.cargo/env"` to your `~/.bashrc` (or `~/.zshrc`).
2) Install `pkg-config`: `sudo apt install pkg-config` (a default dependency for Ubuntu)
3) Install `openssl`: `sudo apt install libssl-dev` (a default dependency for Ubuntu)
4) Clone & Build & Install:
```
git clone git@github.com:firebolt-db/fb-cli.git
cd fb-cli
cargo install --path . --locked
```
4) That's it: you should be able to run `fb` // or at least `~/.cargo/bin/fb` if cargo env isn't caught up.

## Shortcuts

Most of them from https://github.com/kkawakam/rustyline:

| Keystroke             | Action                                                                      |
| --------------------- | --------------------------------------------------------------------------- |
| Enter                 | Finish the line entry                                                       |
| Ctrl-R                | Reverse Search history (Ctrl-S forward, Ctrl-G cancel)                      |
| Ctrl-U                | Delete from start of line to cursor                                         |
| Ctrl-W                | Delete word leading up to cursor (using white space as a word boundary)     |
| Ctrl-Y                | Paste from Yank buffer                                                      |
| Ctrl-\_               | Undo                                                                        |

Some of them specific to `fb`:
| Keystroke             | Action                                                                      |
| --------------------- | --------------------------------------------------------------------------- |
| Ctrl-V then Enter     | Open last query result in interactive csvlens viewer                        |
| Ctrl-C                | Cancel current input                                                        |
| Ctrl-O                | Insert a newline                                                            |


## Defaults

Can update defaults one and for all by specifying `--update-defaults`: during this application old defaults are **not** applied.

New defaults are going to be stored at `~/.firebolt/fb_config`.


```
~ ➤  fb select 42
 ?column?
---------
       42

Time: 40.342ms

~ ➤  fb select 42 --format CSVWithNames --concise --update-defaults
"?column?"
42

~ ➤  fb select 42
"?column?"
42

~ ➤  fb select 42 --verbose # defauls are merged with new args
URL: http://localhost:8123/?database=local_dev_db&mask_internal_errors=1
QUERY: select 42
"?column?"
42
```

## Queries against FB 2.0 using Service Account

Specify:
- Service Account ID;
- Service Account Secret;
- Environment

Note: The token is saved in `~/.firebolt/fb_sa_token/` and will be reused if the account ID and secret match and the token is less than half an hour old.


```
➤  fb --sa-id=${SA_ID} --sa-secret=${SA_SECRET} --oauth-env=app \
  -h ${ACCOUNT_ID}.api.us-east-1.app.firebolt.io -d ${DATABASE_NAME}
```

Read more about getting service accounts [here](https://docs.firebolt.io/guides/managing-your-organization/service-accounts).

## Queries against FB 2.0

Specify:
- host;
- account_id;
- JWT token (can be obtained from browser or other authentication methods);

```
➤  fb --host api.us-east-1.app.firebolt.io --verbose --extra account_id=12312312312 --jwt 'eyJhbGciOiJSUzI1NiI...'

=> show engines
URL: https://api.us-east-1.app.firebolt.io/?database=db_1&account_id=12312312&output_format=JSON&advanced_mode=1
QUERY: show engines
┌─engine_name─────────────┬─engine_owner────────────────┬─type─┬─nodes─┬─clusters─┬─status───────────────┬─auto_start─┬─auto_stop─┬─initially_stopped─┬─url────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_database───────────────────┬─version─┬─last_started──────────────────┬─last_stopped──────────────────┬─description─┐
│ pre_demo_engine1        │ user@firebolt.io            │ S    │     2 │        1 │ ENGINE_STATE_STOPPED │          t │        20 │                 f │ api.us-east-1.app.firebolt.io?account_id=1321231&engine=pre_demo_engine1                               │                                    │ latest  │ 2024-02-07 01:19:19.81689+00  │ 2024-02-07 01:44:15.930845+00 │             │
│ pre_demo_engine2        │ user@firebolt.io            │ S    │     2 │        1 │ ENGINE_STATE_STOPPED │          t │        20 │                 f │ api.us-east-1.app.firebolt.io?account_id=1321231&engine=pre_demo_engine2                               │        pre_demo_validation_testdb1 │ latest  │ 2024-02-07 01:21:36.274962+00 │ 2024-02-07 02:32:05.403539+00 │             │
└─────────────────────────┴─────────────────────────────┴──────┴───────┴──────────┴──────────────────────┴────────────┴───────────┴───────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────┴─────────┴───────────────────────────────┴───────────────────────────────┴─────────────┘

=> set engine=user5_engine_1
URL: https://api.us-east-1.app.firebolt.io/?database=db_1&engine=user5_engine_1&account_id=1321231&output_format=JSON&advanced_mode=1

=> select 42
URL: https://api.us-east-1.app.firebolt.io/?database=db_1&engine=user5_engine_1&account_id=1321231&output_format=JSON&advanced_mode=1
QUERY: select 42
 ?column?
---------
       42

Time: 275.639ms
```

## Set and Unset

In interactive mode one can dynamically update extra arguments:
- `set key=value;` to set the argument;
- `unset key;` to unset it.

```
=> select E'qqq';
URL: http://localhost:8123/?database=local_dev_db
QUERY: select E'qqq';
 ?column?
---------
      qqq

Time: 40.745ms

=> set format = Vertical;
=> select E'qqq';
URL: http://localhost:8123/?database=local_dev_db
QUERY: select E'qqq';
Row 1:
──────
?column?: qqq

Time: 38.888ms

=> set cool_mode=disabled;
=> select E'qqq';
URL: http://localhost:8123/?database=local_dev_db&cool_mode=disabled
QUERY: select E'qqq';
Unknown setting cool_mode

Time: 36.802ms

=> unset cool_mode
=> select E'qqq';
URL: http://localhost:8123/?database=local_dev_db
QUERY: select E'qqq';
Row 1:
──────
?column?: qqq

Time: 39.395ms

=> set enable_result_cache=disabled;
=> select E'qqq';
URL: http://localhost:8123/?database=local_dev_db&enable_result_cache=disabled
QUERY: select E'qqq';
Row 1:
──────
qqq: qqq

Time: 41.671ms

=> unset enable_result_cache;
=> select E'qqq';
URL: http://localhost:8123/?database=local_dev_db
QUERY: select E'qqq';
Row 1:
──────
?column?: qqq

Time: 39.453ms

=> 
```

## License

See [LICENSE](LICENSE.md).
