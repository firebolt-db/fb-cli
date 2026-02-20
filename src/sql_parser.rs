/// Shared tree-sitter SQL parser infrastructure.
///
/// Used by the completion system (Phase 6) for AST-based context detection.
/// Each component that needs parsing should call `create_parser()` to get its
/// own `Parser` instance — `tree_sitter::Parser` is not `Sync`.

pub fn sql_language() -> tree_sitter::Language {
    devgen_tree_sitter_sql::language()
}

pub fn create_parser() -> tree_sitter::Parser {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&sql_language())
        .expect("tree-sitter SQL grammar should always load");
    parser
}
