# Output Pipeline

This document describes how output travels from business logic (query execution, meta-commands) to the visible text in the TUI output pane, and how the output pane renders it.

## Overview

```
query() / meta_commands()
        │
        │  Context::emit*()
        ▼
  mpsc::UnboundedSender<TuiMsg>
        │
        │  TuiApp::drain_query_output()  (100ms tick)
        ▼
  OutputPane::push_*()
        │
        │  OutputPane::render()
        ▼
  ratatui Buffer  →  terminal
```

## TuiMsg (`src/tui_msg.rs`)

The channel carries `TuiMsg`, a two-variant enum:

```rust
pub enum TuiMsg {
    Line(String),                  // plain text (may contain ANSI escape codes)
    StyledLines(Vec<TuiLine>),     // pre-styled table output
}
```

`TuiLine` is a `Vec<TuiSpan>`. `TuiSpan` holds a `String`, a `TuiColor`, and a `bold` flag. This type lives in `tui_msg.rs` and has **no dependency on ratatui** so it can be constructed inside the query task (which also runs in headless mode and must not import TUI types).

The conversion from `TuiSpan` → ratatui `Span<'static>` happens in `output_pane.rs::tui_span_to_ratatui()`.

### Why two variants?

Tables need per-span color (header cyan, NULL dark gray, border plain). Encoding this as ANSI escape codes and then parsing them back is fragile and depends on the rendering library emitting codes unconditionally (comfy-table does not when stdout is not a TTY). `StyledLines` bypasses ANSI entirely by carrying the styling as structured data.

Plain text output (errors, stats, `\help` text, etc.) is sent as `Line(String)`. It may optionally contain ANSI SGR codes from other sources; those are parsed by `parse_ansi_line()` in the output pane.

## Context output helpers (`src/context.rs`)

All output in business-logic code goes through these methods on `Context`:

| Method | Headless | TUI |
|--------|----------|-----|
| `emit(text)` | `println!` | `TuiMsg::Line` |
| `emit_err(text)` | `eprintln!` | `TuiMsg::Line` |
| `emit_newline()` | `eprintln!()` | `TuiMsg::Line("")` |
| `emit_raw(text)` | `print!` (no trailing newline) | splits on `\n`, sends each as `TuiMsg::Line` |
| `emit_styled_lines(lines)` | no-op | `TuiMsg::StyledLines` |

`is_tui()` returns `true` when `tui_output_tx` is `Some`. Query code that needs different behaviour in TUI mode (e.g. calling `emit_table_tui()` instead of `render_table_output()`) uses this.

## drain_query_output (`tui/mod.rs`)

Called once per event-loop tick before rendering. Drains all pending messages non-blockingly:

- `TuiMsg::StyledLines` → `output.push_tui_lines()`
- `TuiMsg::Line` starting with `"Time: "` or `"Scanned: "` → `output.push_stat()`
- `TuiMsg::Line` starting with `"Error: "` or `"^C"` → `output.push_error()`
- `TuiMsg::Line` starting with `"Showing first "` while running → captured as `running_hint`, not forwarded
- Everything else → `output.push_ansi_text()`

Channel `Disconnected` signals that the query task has finished; `is_running` is cleared.

## OutputPane (`tui/output_pane.rs`)

Stores all output as a `Vec<OutputLine>`. Each `OutputLine` holds a `Line<'static>` — a ratatui line of styled spans — that has already been fully resolved. There is no late-stage style application during render; all conversion happens in the `push_*` methods.

### Push methods

| Method | Style applied |
|--------|---------------|
| `push_line(s)` | No style (default terminal color) |
| `push_prompt(s)` | `❯ ` prefix: green+bold; remainder: yellow |
| `push_stat(s)` | Dark gray; splits on `\n` |
| `push_error(s)` | Red; splits on `\n` |
| `push_ansi_text(s)` | Parses ANSI SGR codes; splits on `\n` |
| `push_tui_lines(lines)` | Converts `TuiLine` → ratatui `Line` via `tui_span_to_ratatui()` |

All push methods call `scroll_to_bottom()` (sets `scroll = usize::MAX` as a sentinel), which is resolved to `total_lines - visible_height` in `clamp_scroll()` before rendering.

### Scrolling

`scroll_up(n)` / `scroll_down(n)` adjust `scroll` by `n` lines. `clamp_scroll(height)` is called before each render to keep the offset in bounds and to resolve the `usize::MAX` sentinel.

### Rendering

`render(area, buf)` renders only the visible slice `lines[scroll..scroll+height]`. Content is **bottom-anchored**: empty `Line::raw("")` padding is prepended so that when there are fewer lines than the visible height, the content sticks to the bottom of the pane (the output "grows upward" from the input area, like a normal terminal).

```rust
let padding = height.saturating_sub(content.len());
let visible = repeat_n(Line::raw(""), padding).chain(content).collect();
Widget::render(Paragraph::new(visible), area, buf);
```

### ANSI SGR parser

`parse_ansi_line(text)` walks the raw byte string looking for `ESC [` sequences terminated by `m`. It applies `apply_sgr()` to build a running `Style` and flushes text segments as `Span::styled(text, current_style)`. Unrecognized SGR parameters are silently ignored. This parser handles output from non-table sources (e.g. `\help`, comfy-table in headless mode) that may have been formatted with ANSI codes before being written to the channel.
