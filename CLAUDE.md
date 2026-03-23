# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`fb-cli` is a Rust-based CLI tool for interacting with the Firebolt analytical database. It supports both one-off queries and an interactive REPL mode with history, search, and multi-line query support.

Repo upstream: https://github.com/firebolt-db/fb-cli

## Common Commands

### Building and Running
```bash
# Build the project
cargo build

# Build and install locally
cargo install --path . --locked

# Run the CLI (after installation)
fb

# Run directly with cargo
cargo run

# Run with arguments
cargo run -- select 42
```

### Testing
```bash
# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run tests in a specific file/module
cargo test query::tests
```

### Code Quality
```bash
# Format code (uses rustfmt.toml config)
cargo fmt

# Check code without building
cargo check

# Lint with clippy
cargo clippy
```

## Architecture

### Core Modules

- **main.rs**: Entry point; handles REPL loop with rustyline for readline functionality. Binds Ctrl+O to insert newlines in the REPL.

- **args.rs**: Command-line argument parsing using gumdrop. Manages configuration defaults stored in `~/.firebolt/fb_config` (YAML format). The `get_args()` function merges CLI args with saved defaults. URL construction happens in `get_url()`.

- **query.rs**: Query execution logic. Uses Pest parser (defined in sql.pest) to split multi-statement queries while handling SQL comments and string literals. Sends HTTP/2 requests to Firebolt with keep-alive. Handles `set`/`unset` commands for dynamic parameter configuration. Supports async cancellation via Ctrl+C.

- **auth.rs**: OAuth authentication for Service Accounts. Caches tokens in `~/.firebolt/fb_sa_token/` (valid for 30 minutes). Supports both JWT and bearer token authentication.

- **context.rs**: Application state container holding Args, URL, and optional ServiceAccountToken. The URL is rebuilt when parameters change.

- **utils.rs**: Helper functions for file paths (`~/.firebolt/` directory management), terminal spinner, and time formatting.

### SQL Query Parsing

The tool uses Pest parser generator (src/sql.pest) to split multiple SQL queries while correctly handling:
- Single and double-quoted strings with escaped quotes
- E-strings (PostgreSQL-style escaped strings)
- Raw strings ($$-delimited)
- Line comments (--) and nested block comments (/* */)
- Semicolons inside strings/comments (not treated as query terminators)

The parser requires all queries to end with semicolons. Incomplete queries in the REPL are accumulated across lines until a semicolon is encountered.

### Configuration and State

- **Defaults**: Stored in `~/.firebolt/fb_config` (YAML). Updated with `--update-defaults` flag.
- **JWT tokens**: Can be loaded from `~/.firebolt/jwt` with `--jwt-from-file`.
- **Service Account tokens**: Cached in `~/.firebolt/fb_sa_token/` (YAML).
- **REPL history**: Saved to `~/.firebolt/history` (max 10,000 entries).

### Authentication Modes

1. **Local development**: Direct connection (default: localhost:8123 or localhost:9123 with JWT)
2. **Firebolt Core**: Use `-C` flag (connects to localhost:3473, uses PSQL format)
3. **Service Account**: Provide `--sa-id`, `--sa-secret`, and `--oauth-env` (staging/app)
4. **Bearer token**: Use `--bearer` flag for browser-extracted tokens
5. **JWT**: Use `--jwt` or `--jwt-from-file`

### HTTP Protocol

- Uses HTTP/2 with keep-alive (3600s timeout, 60s interval)
- Custom headers: `user-agent` (fdb-cli/version), `Firebolt-Protocol-Version` (2.3)
- Supports dynamic parameter updates via response headers:
  - `firebolt-update-parameters`: Sets query parameters
  - `firebolt-remove-parameters`: Removes query parameters
  - `firebolt-update-endpoint`: Changes host/endpoint with parameters

## Firebolt-Specific Features

### SET/UNSET Commands
In REPL mode, dynamically modify query parameters:
```sql
set format = Vertical;
set engine = my_engine;
unset enable_result_cache;
```

These modify the URL query string. `format` is a special case that maps to `output_format` parameter.

### Output Formats
Controlled by `--format` flag or `set format = ...`:
- PSQL (default for most modes)
- TabSeparatedWithNames
- TabSeparatedWithNamesAndTypes
- JSONLines_Compact
- CSVWithNames
- Vertical

### Query Labels
Add `--label` to tag queries for tracking in Firebolt logs.

## Testing

The codebase has comprehensive unit tests, especially for:
- SQL query splitting with complex edge cases (see query.rs tests)
- Parameter encoding and URL generation (see args.rs tests)
- SET/UNSET command parsing
- String literals and comment handling

When modifying the SQL parser (sql.pest), run `cargo test` to validate against the extensive test suite in query.rs.
