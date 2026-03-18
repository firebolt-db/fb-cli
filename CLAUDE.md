# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`fb-cli` is a command-line client for Firebolt database written in Rust. It supports both single-query execution and an interactive REPL mode with query history and dynamic parameter configuration.

**Note**: This project has been moved to https://github.com/firebolt-db/fb-cli

## Build and Development Commands

### Building
```bash
cargo build --release
```

### Testing
```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Integration tests only
cargo test --test cli
```

### Installation
```bash
cargo install --path . --locked
```

### Formatting
```bash
cargo fmt
```

### Linting
```bash
cargo clippy
```

## Architecture

### Module Structure

- **main.rs**: Entry point; routes to headless (single query / pipe) or interactive TUI mode
- **tui/mod.rs**: Interactive TUI built with ratatui + tui-textarea; event loop, key handling, slash commands, completion, history
- **args.rs**: Command-line argument parsing with gumdrop, URL generation, and default config management
- **context.rs**: Application state container holding Args and ServiceAccountToken
- **query.rs**: HTTP query execution, set/unset parameter handling, and SQL query splitting using Pest parser
- **auth.rs**: OAuth2 service account authentication with token caching
- **utils.rs**: File paths (~/.firebolt/), spinner animation, and time formatting utilities
- **sql.pest**: Pest grammar for parsing SQL queries (handles strings, comments, semicolons)

### Key Design Patterns

**Context Management**: The `Context` struct is passed mutably through the application, allowing runtime updates to Args and URL as users `set` and `unset` parameters in REPL mode.

**Query Splitting**: SQL queries are parsed using a Pest grammar (sql.pest) that correctly handles:
- String literals (single quotes with escape sequences)
- E-strings (PostgreSQL-style escaped strings)
- Raw strings ($$...$$)
- Quoted identifiers (double quotes)
- Line comments (--) and nested block comments (/* /* */ */)
- Semicolons as query terminators (but not within strings/comments)

**Authentication Flow**:
1. Service Account tokens are cached in `~/.firebolt/fb_sa_token`
2. Tokens are valid for 30 minutes and automatically reused if valid
3. JWT can be loaded from `~/.firebolt/jwt` with `--jwt-from-file`
4. Bearer tokens can be passed directly with `--bearer`

**Configuration Persistence**: Defaults are stored in `~/.firebolt/fb_config` as YAML. When `--update-defaults` is used, current arguments (excluding update_defaults itself) are saved and merged with future invocations.

**Dynamic Parameter Updates**: The REPL supports `set key=value` and `unset key` commands to modify query parameters at runtime without restarting the CLI. Server can also update parameters via `firebolt-update-parameters` and `firebolt-remove-parameters` headers.

### HTTP Protocol

- Uses reqwest with HTTP/2 keep-alive (3600s timeout, 60s intervals)
- Sends queries as POST body to constructed URL with query parameters
- Headers: `user-agent`, `Firebolt-Protocol-Version: 2.3`, `authorization` (Bearer token)
- Handles special response headers: `firebolt-update-parameters`, `firebolt-remove-parameters`, `firebolt-update-endpoint`

### Data Type Handling in JSONLines_Compact Format

**Firebolt Serialization Patterns:**

The `JSONLines_Compact` format uses type-specific JSON serialization to preserve precision and avoid limitations of JSON numbers:

| Firebolt Type | JSON Representation | Example |
|---------------|---------------------|---------|
| INT | JSON number | `42` |
| BIGINT | JSON **string** | `"9223372036854775807"` |
| NUMERIC/DECIMAL | JSON **string** | `"1.23"` |
| DOUBLE/REAL | JSON number | `3.14` |
| TEXT | JSON string | `"text"` |
| DATE | JSON string (ISO) | `"2026-02-06"` |
| TIMESTAMP | JSON string | `"2026-02-06 15:35:34+00"` |
| BYTEA | JSON string (hex) | `"\\x48656c6c6f"` |
| GEOGRAPHY | JSON string (WKB hex) | `"0101000020E6..."` |
| ARRAY | JSON array | `[1,2,3]` |
| BOOLEAN | JSON boolean | `true` |
| NULL | JSON `null` | `null` |

**Why certain types are JSON strings:**
- **BIGINT/NUMERIC**: JavaScript/JSON numbers are 64-bit floats with ~53 bits precision. BIGINT (64-bit int) and NUMERIC (38 digits) need strings to avoid precision loss.
- **DATE/TIMESTAMP**: ISO format strings are portable and timezone-aware
- **BYTEA**: Binary data encoded as hex strings (e.g., `\x48656c6c6f` = "Hello")
- **GEOGRAPHY**: Spatial data in WKB (Well-Known Binary) format encoded as hex strings

**Client-Side Rendering:**

The `format_value()` function renders these types for display:
- Numbers displayed via `.to_string()` (e.g., `42`, `3.14`)
- Strings displayed as-is without quotes (BIGINT `"9223372036854775807"` → displays as `9223372036854775807`)
- NULL rendered as SQL-style `"NULL"` string in tables, empty string in CSV
- Arrays/Objects JSON-serialized with truncation at 1000 characters

The `column_type` field from the server is currently unused but available for future type-aware formatting.

**Server-Side Formats:**

Formats like `PSQL`, `TabSeparatedWithNames`, etc. bypass client parsing entirely and are printed raw to stdout.

### URL Construction

URLs are built from:
- Protocol: `http` for localhost, `https` otherwise
- Host: from `--host` or defaults
- Database: from `--database` or `--extra database=...`
- Format: from `--format` or `--extra format=...`
- Extra parameters: from `--extra` (URL-encoded once during normalize_extras)
- Query label: from `--label`
- Advanced mode: automatically added for non-localhost

### Output Format Options with Client Prefix

fb-cli uses a single `--format` option with a prefix notation to distinguish between client-side and server-side rendering:

**Client-Side Rendering** (prefix with `client:`):
- Values: `client:auto`, `client:vertical`, `client:horizontal`
- Behavior: Client requests `JSONLines_Compact` and renders it with formatting
- Supports: Pretty tables, NULL coloring, csvlens viewer integration
- Default in interactive mode: `client:auto`

**Client display modes:**
- `client:auto` - Smart switching: horizontal for narrow tables, vertical for wide tables
- `client:horizontal` - Force horizontal table with column headers
- `client:vertical` - Force vertical two-column layout (column | value)

**Server-Side Rendering** (no prefix):
- Values: `PSQL`, `TabSeparatedWithNames`, `JSON`, `CSV`, `JSONLines_Compact`, etc.
- Behavior: Server renders output in this format, client prints raw output
- Default in non-interactive mode: `PSQL`

**Detection:**
- If `--format` starts with `client:`: Use client-side rendering
- Otherwise: Use server-side rendering

**Default Behavior:**
- Interactive sessions (TTY): `--format client:auto` (client-side pretty tables)
- Non-interactive (piped/redirected): `--format PSQL` (server-side, backwards compatible)
- Can be overridden with explicit `--format` flag

**Changing format at runtime:**
- Command-line: `--format client:auto` or `--format JSON`
- SQL-style: `set format = client:vertical;` or `set format = CSV;`
- Reset: `unset format;` (resets to default based on interactive mode)

**Config persistence:**
- Saved in `~/.firebolt/fb_config`
- Format can be persisted with `--update-defaults`
- Clear which mode: `client:` prefix means client-side, no prefix means server-side

**csvlens Integration:**
- Only works with client-side formats (`client:*`)
- Automatically checks and displays error if server-side format used

### Firebolt Core vs Standard

- **Core mode** (`--core`): Connects to Firebolt Core at `localhost:3473`, database `firebolt`, format `PSQL`, no JWT
- **Standard mode** (default): Connects to `localhost:8123` (or 9123 with JWT), database `local_dev_db`, format `PSQL`

## Testing Considerations

- Unit tests are in-module (`#[cfg(test)] mod tests`)
- Integration tests in `tests/cli.rs` use `CARGO_BIN_EXE_fb` to test the compiled binary
- SQL parsing tests extensively cover edge cases: nested comments, strings with semicolons, escape sequences
- Auth tests are minimal due to external service dependencies

## Important Behavioral Details

- **URL encoding**: Parameters are encoded once during `normalize_extras(encode: true)`. Subsequent calls with `encode: false` prevent double-encoding.
- **TUI multi-line**: Press Shift+Enter or Alt+Enter to insert a newline. Queries must end with semicolon.
- **Ctrl+C in TUI**: Cancels an in-flight query; does not exit
- **Ctrl+D in TUI**: Exits
- **Ctrl+V in TUI**: Opens csvlens viewer for the last result directly
- **Alt+E in TUI**: Opens current query in $EDITOR
- **Spinner**: Shown during query execution for client-side formats only
- **History**: Saved to `~/.firebolt/fb_history` (max 10,000 entries), supports Ctrl+R search
