# TUI Architecture

fb-cli's interactive REPL is built on [ratatui](https://github.com/ratatui-org/ratatui) with [tui-textarea](https://github.com/rhysd/tui-textarea) for the editor widget and [crossterm](https://github.com/crossterm-rs/crossterm) as the terminal backend.

## Layout

```
┌───────────────────────────────────────────────┐
│  OUTPUT PANE  (scrollable, fills remaining    │
│  space; bottom-anchored: new output grows     │
│  upward from the input area)                  │
│                                               │
├───────────────────────────────────────────────┤
│  INPUT PANE  (tui-textarea, 1–5 lines visible │
│  + DarkGray top/bottom borders)               │
├───────────────────────────────────────────────┤
│  STATUS BAR  (1 line, host | db | version)    │
└───────────────────────────────────────────────┘
```

The layout is computed by `tui/layout.rs::compute_layout()`. It reserves a fixed block of rows at the bottom (`RESERVED_INPUT = 5`) so the output pane doesn't jump when the textarea grows to a second line. A transparent spacer is inserted between the input pane and the status bar to absorb the reserved space. When the textarea grows past 5 lines, it starts pushing the output pane upward.

## Key structs

| Struct | File | Purpose |
|--------|------|---------|
| `TuiApp` | `tui/mod.rs` | Owns all state; drives the event loop |
| `OutputPane` | `tui/output_pane.rs` | Scrollable output history |
| `History` | `tui/history.rs` | File-backed history with cursor navigation |
| `AppLayout` | `tui/layout.rs` | Three `Rect` values from layout computation |
| `TuiMsg` | `tui_msg.rs` | Channel message type between query task and TUI |

## Event loop

`TuiApp::run()` sets up the terminal (raw mode, alternate screen, mouse capture, Kitty keyboard protocol) then delegates to `event_loop()`.

```
loop:
  1. drain_query_output()   — pull pending TuiMsg items off the channel
  2. increment spinner tick (if query is running)
  3. terminal.draw(render)  — paint the frame
  4. event::poll(100ms)     — wait for input; tick the spinner even when idle
  5. handle_key() or scroll mouse event
```

The 100 ms poll timeout drives the spinner animation without consuming CPU.

## Query execution

Queries run in a `tokio::spawn` task, fully asynchronous with respect to the render loop. Communication happens through two primitives:

- **`mpsc::unbounded_channel::<TuiMsg>()`** – output from the query flows back to the TUI. When the task finishes (or is cancelled), it drops `ctx`, which drops the sender, which closes the channel. `drain_query_output()` detects the `Disconnected` variant and clears `is_running`.
- **`CancellationToken`** – Ctrl+C cancels the query by calling `token.cancel()`. The query function polls this token and aborts the HTTP request.

The query task receives a cloned `Context` with `tui_output_tx` and `query_cancel` populated. All output is routed through `Context::emit*()` helpers rather than `println!`, so the same query code works in both headless and TUI modes.

## Running pane

While a query is in flight the input textarea is replaced by a compact "running pane" showing a Braille spinner and elapsed time. The `running_hint` string (e.g. "Showing first N rows — collecting remainder...") is also displayed if present. This hint is intercepted from the `TuiMsg::Line` stream in `drain_query_output()` — lines that start with `"Showing first "` are captured in `running_hint` rather than forwarded to the output pane.

## Key bindings

| Key | Action |
|-----|--------|
| `Enter` | Submit query if SQL is syntactically complete; otherwise insert newline |
| `Shift+Enter` | Always insert newline (requires Kitty keyboard protocol) |
| `Ctrl+D` | Exit |
| `Ctrl+C` | Cancel input (idle) or cancel in-flight query (running) |
| `Ctrl+V` | Open last result in csvlens viewer |
| `Up` / `Ctrl+Up` | History: navigate backward (Up at row 0 triggers history) |
| `Down` / `Ctrl+Down` | History: navigate forward |
| `PageUp` / `PageDown` | Scroll output pane by 10 lines |
| Mouse scroll | Scroll output pane by 8 lines |
| `\view` + Enter | Open csvlens viewer |
| `\refresh` + Enter | Refresh schema cache |
| `\help` + Enter | Show help in output pane |

### Kitty keyboard protocol

`PushKeyboardEnhancementFlags(DISAMBIGUATE_ESCAPE_CODES)` is sent on startup so terminals that support it (kitty, WezTerm, foot, etc.) send distinct codes for `Shift+Enter` vs `Enter`. On terminals that don't support it the flag push is silently ignored; `Shift+Enter` falls through to inserting a newline via the textarea's default handling.

### Query submission heuristic

The TUI calls `try_split_queries()` from `query.rs`, which uses the Pest SQL grammar to split on semicolons while respecting strings, comments, and dollar-quoted blocks. If the text splits into at least one complete query the whole buffer is submitted. If not (e.g. the user is mid-statement), a newline is inserted instead.

## Rendering pipeline

```
TuiApp::render()
 ├── compute_layout(area, input_height)
 ├── output.clamp_scroll(layout.output.height)
 ├── output.render(layout.output, buf)        ← OutputPane
 ├── render_running_pane() OR render_input()  ← tui-textarea / spinner
 └── render_status_bar()
```

Colors used:
- Output pane: prompt `❯` = green+bold, SQL text = yellow, stats = dark gray, errors = red, table headers = cyan+bold, NULL values = dark gray
- Input pane: `❯` prompt = green, border = dark gray
- Status bar: dark gray background, white text, right side shows `Ctrl+C cancel` while running

## History (`tui/history.rs`)

`History` stores entries in memory and appends each new entry to `~/.firebolt/fb_history` (one entry per line). Multi-line entries are stored with embedded newlines escaped as `\\n`.

Navigation state:
- `cursor: Option<usize>` — `None` means not navigating. `Some(i)` means showing `entries[i]`.
- `saved_current: String` — the text that was in the editor when the user first pressed Up, restored when they press Down past the newest entry.

Consecutive duplicate entries are not stored. The in-memory list is capped at 10 000 entries; the file grows unboundedly (trimming is applied on load).

## csvlens integration

`\view` (or Ctrl+V) calls `open_viewer()`, which:
1. `disable_raw_mode()` + `LeaveAlternateScreen` — yields the terminal
2. Spawns `csvlens` as a subprocess via `viewer::open_csvlens_viewer()`
3. `enable_raw_mode()` + `EnterAlternateScreen` — reclaims the terminal
4. Sets `needs_clear = true` so ratatui does a full repaint on the next tick

## Adding new key bindings

Add a match arm in `handle_key()` in `tui/mod.rs`. For bindings that only apply while running, add them in the early return block at the top of `handle_key()`.

## Adding new backslash commands

Add a match arm in `handle_backslash_command()`. Persistent parameter changes (e.g. `set format = ...`) should go through `handle_meta_command()` in `meta_commands.rs` so they work in both TUI and headless mode.
