# Table Rendering

fb-cli has two rendering paths for query results:

| Path | When used | Output type |
|------|-----------|-------------|
| **Headless** (`render_table*`) | Non-TUI mode | ANSI string written to stdout |
| **TUI** (`render_*_tui_lines`) | TUI mode | `Vec<TuiLine>` sent via `TuiMsg::StyledLines` |

The TUI path produces `TuiLine`/`TuiSpan` values directly, bypassing ANSI entirely. Both paths are in `src/table_renderer.rs`.

## Entry point in query.rs

```rust
if context.is_tui() {
    emit_table_tui(context, columns, rows, terminal_width, max_cell);
} else {
    let rendered = render_table_output(context, columns, rows, terminal_width, max_cell);
    out!(context, "{}", rendered);
}
```

`emit_table_tui()` subtracts 1 from `terminal_width` before passing it to the renderer. This leaves a 1-column right margin so the table's closing border is never clipped by ratatui's render area.

## Display modes

Controlled by `context.args`:

| `args` method | TUI renderer called |
|---------------|---------------------|
| `is_vertical_display()` | `render_vertical_table_to_tui_lines()` |
| `is_horizontal_display()` | `render_horizontal_forced_tui_lines()` |
| auto (default) | `render_table_to_tui_lines()` — decides at runtime |

In auto mode, `decide_col_widths()` returns `Some(widths)` for horizontal or `None` to fall back to vertical.

## Cell value formatting (`fmt_cell`)

Before layout, every cell value is processed by `fmt_cell(val, max_value_length)`:

1. `format_value(val)` converts the `serde_json::Value` to a display string:
   - `Null` → `"NULL"`
   - `String(s)` → `s` (as-is, preserving the value)
   - `Number` / `Bool` → `.to_string()`
   - `Array` / `Object` → JSON-serialized, hard-capped at 1 000 chars with `... (truncated)` suffix

2. All control characters (`\n`, `\r`, `\t`, etc.) are replaced with spaces. This is critical: cells that contain multi-line text (e.g. `query_text` storing SQL with newlines) would otherwise produce spans containing `\n`, which ratatui renders as line breaks, causing misaligned borders.

3. `truncate_to_chars(s, max_value_length)` caps the result at `max_value_length` Unicode scalar values, appending `"..."` if truncated. The limit is `max_cell_length` from args (default: 1 000); for single-column queries it is raised to 5× the normal limit.

## Auto layout: `decide_col_widths`

Accepts the formatted cell strings and the available terminal width. Returns `Some(col_widths)` for horizontal layout or `None` for vertical.

### Available space calculation

```
overhead = 3 * n + 1   (for "│ col₀ │ col₁ │ … │" structure)
available = terminal_width - overhead
```

### Decision sequence

1. **Single-column shortcut** — always horizontal, width = `min(terminal_width - 4, natural_capped)`.

2. **Natural widths** — for each column: `max(header_chars, max_content_chars)`, capped at `terminal_width * 4/5`.
   - If `Σ natural_widths ≤ available`: use natural widths as-is.

3. **Too cramped?** — if `available / n < 10`: return `None` (vertical).

4. **Minimum widths** — per column: `max(10, ⌈header_len/2⌉, ⌈√max_content_len⌉)`.
   - `⌈header_len/2⌉` ensures the header needs at most 2 wrap lines.
   - `⌈√max_content_len⌉` keeps cells roughly square (height ≈ width).
   - If `Σ min_widths > available`: return `None` (vertical).

5. **Space distribution** — start with `min_widths`, repeatedly add 1 to each column that hasn't reached its natural width until `available` is exhausted.

6. **Tall-cell guard** — after distribution, compute how many wrap rows each cell needs: `⌈content_chars / col_width⌉`. If any cell needs more rows than there are columns (`n`), return `None` (vertical). This prevents a single long cell from producing a table taller than it is wide.

## Horizontal rendering (`render_horizontal_tui`)

Uses Unicode box-drawing characters:

```
┌──────────────┬──────────┐   top border
│ account_name │ value    │   header row (cyan + bold)
╞══════════════╪══════════╡   header separator
│ firebolt     │ 42       │   data row
├──────────────┼──────────┤   row separator (only when wrapping occurs)
│ other-acct   │ 0        │
└──────────────┴──────────┘   bottom border
```

Row separators (`├─┼─┤`) are only added between rows when **any** cell in the table wraps to more than one visual line. When all cells fit on one line, no separators are drawn.

### `wrap_cell(s, width) -> Vec<String>`

Splits a string into chunks of exactly `width` Unicode scalar values, padding the last chunk with spaces. Each chunk is suitable for direct use as a ratatui span because:
- All chunks are exactly `width` chars wide (measured by `chars().count()`)
- No control characters exist (sanitized by `fmt_cell`)

### Border construction (`make_tui_border`)

Each column occupies `col_width + 2` fill characters between separator characters, matching the `" col_content "` padding in data rows.

### NULL styling

NULL cells are rendered in dark gray (`TuiColor::DarkGray`) to visually distinguish them from the empty-string value.

## Vertical rendering (`render_vertical_table_to_tui_lines`)

Renders each row as its own mini two-column table:

```
Row 1:                       ← cyan + bold label
┌──────────────┬──────────┐
│ account_name │ firebolt │
│ value        │ 42       │
└──────────────┴──────────┘
                             ← blank line between rows
Row 2:
…
```

Column widths:
- Name column: `max(header_names_len, 30)`, floor 10
- Value column: `terminal_width - name_col - 7` (7 = overhead for two-column table), floor 10

Row separators between fields within a mini-table follow the same rule as horizontal: only drawn when any name or value wraps.

## Forced-horizontal rendering (`render_horizontal_forced_tui_lines`)

Calls `decide_col_widths()` first. If it returns `None` (would normally go vertical), falls back to equal-share widths: `available / n` per column (minimum 1). This ensures a horizontal table is always produced regardless of content width.

## Headless rendering

`render_table()` uses [comfy-table](https://github.com/Numesson/comfy-table) for non-TUI output. comfy-table does **not** emit ANSI codes when stdout is not a TTY, so the TUI path was built from scratch rather than trying to reuse it.

`render_table_vertical()` renders a simple two-column plain-text table using `format!` strings without a dependency on comfy-table.
