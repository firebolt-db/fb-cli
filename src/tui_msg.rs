/// Messages sent from the query thread to the TUI output pane via the mpsc channel.
pub enum TuiMsg {
    /// A plain text line (may contain ANSI escape codes).
    Line(String),
    /// Pre-styled lines produced by the custom TUI table renderer.
    StyledLines(Vec<TuiLine>),
    /// Streaming progress update: total data rows received so far.
    Progress(u64),
    /// The parsed result of the last successful SELECT query (for csvlens / export).
    ParsedResult(crate::table_renderer::ParsedResult),
}

/// A single rendered line made up of zero or more styled spans.
pub struct TuiLine(pub Vec<TuiSpan>);

/// A styled run of text.
pub struct TuiSpan {
    pub text: String,
    pub color: TuiColor,
    pub bold: bool,
}

impl TuiSpan {
    pub fn plain(text: impl Into<String>) -> Self {
        Self { text: text.into(), color: TuiColor::Default, bold: false }
    }
    pub fn styled(text: impl Into<String>, color: TuiColor, bold: bool) -> Self {
        Self { text: text.into(), color, bold }
    }
}

#[derive(Clone, Copy)]
pub enum TuiColor {
    Default,
    Cyan,
    DarkGray,
}
