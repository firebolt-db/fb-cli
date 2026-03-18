use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// The three panes of the TUI.
pub struct AppLayout {
    pub output: Rect,
    pub input: Rect,
    pub status: Rect,
}

/// Rows always reserved at the bottom for the input area (5 content + 2 borders).
/// The output pane never encroaches into this space, even when the textarea is
/// only one line tall.  When the textarea grows past 5 lines it starts to push
/// the output pane upward.
const RESERVED_INPUT: u16 = 5;

/// Compute a 4-pane vertical layout: output / spacer / input / status.
///
/// `input_height` is the *actual* (tight) height of the input pane including
/// its borders.  A transparent spacer is inserted above the input pane so that
/// the bottom `RESERVED_INPUT` rows are always reserved, keeping the output
/// pane stable while the textarea is small.
pub fn compute_layout(area: Rect, input_height: u16) -> AppLayout {
    const STATUS_HEIGHT: u16 = 1;

    // Hard ceiling: never let the input+spacer eat more than area/3
    let max_input = area.height.saturating_sub(STATUS_HEIGHT + 3);
    let input_h = input_height.clamp(3, max_input.max(3));

    // How many rows are reserved at the bottom (at least RESERVED_INPUT, but
    // clamped so the output pane always keeps at least 3 rows).
    let reserved = RESERVED_INPUT.max(input_h).min(max_input.max(input_h));
    let spacer = reserved.saturating_sub(input_h);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),                // output pane – takes remaining space
            Constraint::Length(input_h),       // input pane (anchored at top of reserved area)
            Constraint::Length(spacer),        // transparent spacer below input
            Constraint::Length(STATUS_HEIGHT), // status bar
        ])
        .split(area);

    AppLayout {
        output: chunks[0],
        input: chunks[1],
        status: chunks[3],
    }
}
