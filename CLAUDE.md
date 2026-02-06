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

- **main.rs**: Entry point, handles REPL mode with rustyline for line editing and history
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

### URL Construction

URLs are built from:
- Protocol: `http` for localhost, `https` otherwise
- Host: from `--host` or defaults
- Database: from `--database` or `--extra database=...`
- Format: from `--format` or `--extra format=...`
- Extra parameters: from `--extra` (URL-encoded once during normalize_extras)
- Query label: from `--label`
- Advanced mode: automatically added for non-localhost

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
- **REPL multi-line**: Press Ctrl+O to insert newline. Queries must end with semicolon.
- **Ctrl+C in REPL**: Cancels current input but doesn't exit
- **Ctrl+D in REPL**: Exits (EOF)
- **Spinner**: Shown during query execution unless `--no-spinner` or `--concise`
- **History**: Saved to `~/.firebolt/fb_history` (max 10,000 entries), supports Ctrl+R search
