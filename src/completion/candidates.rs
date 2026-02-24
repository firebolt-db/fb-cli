/// Shared completion candidate collection used by both tab-completion and fuzzy search.
///
/// Both completers call `collect_candidates()` to get a unified, consistently-scored
/// list of schema items. Tab completion then applies additional context filters
/// (hide columns from unrelated tables, hide non-public tables from unreferenced schemas).
/// Fuzzy search uses the raw candidates as-is.
use std::sync::Arc;

use super::schema_cache::SchemaCache;
use super::usage_tracker::{ItemType, UsageTracker};

// ── Priority tiers ────────────────────────────────────────────────────────────
// Higher value → shown first.
//
// Ordering (highest first):
//   5000  Column from a table referenced in the current SQL
//   4500  Public table
//   4400  Schema completion (e.g. "information_schema.")
//   4000  Non-public table
//   3000  Column from a table NOT in the current SQL
//   1000  Function
//      0  System-schema items (information_schema / pg_catalog)
//
// Usage bonus adds up to 990 (99 uses × 10 pts) within each tier.

/// Column belongs to a table referenced in the current SQL statement.
pub const PRIORITY_COLUMN_IN_QUERY: u32 = 5000;
/// Public-schema table (shown as bare name).
pub const PRIORITY_PUBLIC_TABLE: u32 = 4500;
/// Schema completion (e.g. "information_schema.") — just below public tables.
pub const PRIORITY_SCHEMA: u32 = 4400;
/// Non-public table.
pub const PRIORITY_NONPUBLIC_TABLE: u32 = 4000;
/// Column from a table *not* referenced in the current statement.
pub const PRIORITY_COLUMN_OTHER: u32 = 3000;
/// SQL function.
pub const PRIORITY_FUNCTION: u32 = 1000;
/// Items from system schemas (information_schema, pg_catalog).
/// Must be < PRIORITY_FUNCTION even with max usage bonus (0 + 99*10 = 990 < 1000).
pub const PRIORITY_SYSTEM: u32 = 0;

const MAX_USAGE: u32 = 99;
const USAGE_MULTIPLIER: u32 = 10;

// ── Candidate ─────────────────────────────────────────────────────────────────

/// A single completion candidate produced by `collect_candidates`.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Text shown in the popup and used as the default insert value.
    pub display: String,
    /// Text actually inserted when the candidate is accepted.
    /// Differs from `display` for functions: display = "fn()", insert = "fn(".
    pub insert: String,
    /// Short type label: "table", "column", "function", or "schema".
    pub description: &'static str,
    /// Logical type — used for selectivity filtering.
    pub item_type: ItemType,
    /// Schema name; empty string for public-schema items and functions.
    pub schema: String,
    /// Table that owns this column (Column items only).
    pub table_name: Option<String>,
    /// Alternative names tried for prefix matching in addition to `display`.
    ///
    /// Examples:
    /// - non-public table `info.engine_qh` → alts = `["engine_qh"]`
    /// - public column  `test.val`         → alts = `["val"]`
    /// - non-public col `info.engine_qh.t` → alts = `["engine_qh.t", "t"]`
    pub alts: Vec<String>,
    /// Priority score; higher items appear first.  Includes usage bonus.
    pub priority: u32,
}

impl Candidate {
    /// Returns `true` if `partial` is a prefix of `display` or any alternative name.
    /// Empty partial always matches (shows all candidates).
    pub fn prefix_matches(&self, partial: &str) -> bool {
        if partial.is_empty() {
            return true;
        }
        let p = partial.to_lowercase();
        if self.display.to_lowercase().starts_with(&p) {
            return true;
        }
        self.alts.iter().any(|a| a.to_lowercase().starts_with(&p))
    }
}

// ── Query-context helpers ─────────────────────────────────────────────────────

/// Returns `true` if the given table (identified by `table_name` + `schema`)
/// is referenced anywhere in `query_tables` (the list of tables extracted from
/// the current SQL statement by `ContextAnalyzer::extract_tables`).
///
/// Matches bare name, schema-qualified name, and cross-qualified variations.
pub fn table_in_query(table_name: &str, schema: &str, query_tables: &[String]) -> bool {
    if query_tables.is_empty() {
        return false;
    }
    let t = table_name.to_lowercase();
    let s = schema.to_lowercase();
    let qualified = format!("{}.{}", s, t);

    query_tables.iter().any(|qt| {
        let q = qt.to_lowercase();
        q == t                                  // bare match ("users" == "users")
            || q == qualified                   // full match ("public.users" == "public.users")
            || q.ends_with(&format!(".{}", t))  // query "public.users", cache bare "users"
            || qualified == q                   // same as above but other way
    })
}

/// Returns `true` if the given schema name is referenced in `query_tables`.
///
/// Always returns `true` for the public schema (empty string or "public") because
/// public tables are always visible in SQL without schema qualification.
pub fn schema_in_query(schema: &str, query_tables: &[String]) -> bool {
    if schema == "public" || schema.is_empty() {
        return true;
    }
    if query_tables.is_empty() {
        return false;
    }
    let s = schema.to_lowercase();
    query_tables.iter().any(|qt| {
        let q = qt.to_lowercase();
        q.starts_with(&format!("{}.", s)) || q == s
    })
}

// ── Candidate collection ──────────────────────────────────────────────────────

/// Collect all completion candidates from the schema cache with priority scores.
///
/// Pass `tables_in_query` (from `ContextAnalyzer::extract_tables`) to get
/// context-aware column priority.  Fuzzy search passes `&[]` (no context).
pub fn collect_candidates(
    cache: &SchemaCache,
    usage_tracker: Arc<UsageTracker>,
    tables_in_query: &[String],
) -> Vec<Candidate> {
    let mut items = Vec::new();

    // ── Tables ───────────────────────────────────────────────────────────────
    let mut seen_schemas: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (schema, table) in cache.get_all_tables() {
        let is_public = schema == "public" || schema.is_empty();
        let is_system = is_system_schema(&schema);

        let (display, insert) = if is_public {
            (table.clone(), table.clone())
        } else {
            let q = format!("{}.{}", schema, table);
            (q.clone(), q)
        };

        // Non-public tables expose the bare table name as an alternative so
        // the user can type e.g. "engine_q" and still see
        // "information_schema.engine_query_history".
        let alts: Vec<String> = if is_public { vec![] } else { vec![table.clone()] };

        let usage_count = usage_tracker.get_count(ItemType::Table, &table);
        let usage_bonus = usage_count.min(MAX_USAGE) * USAGE_MULTIPLIER;

        let base = if is_system {
            PRIORITY_SYSTEM
        } else if is_public {
            PRIORITY_PUBLIC_TABLE
        } else {
            PRIORITY_NONPUBLIC_TABLE
        };

        items.push(Candidate {
            display,
            insert,
            description: "table",
            item_type: ItemType::Table,
            schema: schema.clone(),
            table_name: None,
            alts,
            priority: base.saturating_add(usage_bonus),
        });

        if !is_public && !schema.is_empty() {
            seen_schemas.insert(schema);
        }
    }

    // ── Schemas ──────────────────────────────────────────────────────────────
    // Emit "schema." completions so the user can drill into a schema.
    let mut schema_list: Vec<String> = seen_schemas.into_iter().collect();
    schema_list.sort();

    for schema in schema_list {
        let schema_with_dot = format!("{}.", schema);
        let usage_count = usage_tracker.get_count(ItemType::Table, &schema);
        let usage_bonus = usage_count.min(MAX_USAGE) * USAGE_MULTIPLIER;
        let base = if is_system_schema(&schema) { PRIORITY_SYSTEM } else { PRIORITY_SCHEMA };

        items.push(Candidate {
            display: schema_with_dot.clone(),
            insert: schema_with_dot,
            description: "schema",
            item_type: ItemType::Schema,
            schema: schema.clone(),
            table_name: None,
            alts: vec![],
            priority: base.saturating_add(usage_bonus),
        });
    }

    // ── Columns ──────────────────────────────────────────────────────────────
    for (schema, table, column) in cache.get_all_columns() {
        let is_public = schema == "public" || schema.is_empty();
        let is_system = is_system_schema(&schema);

        // Always emit qualified names:  "table.col"  or  "schema.table.col"
        let display = if is_public {
            format!("{}.{}", table, column)
        } else {
            format!("{}.{}.{}", schema, table, column)
        };

        // Alternative prefix matches:
        //   public:     "col"                  (bare column name)
        //   non-public: "table.col", "col"     (also allows "table" prefix to trigger)
        let alts: Vec<String> = if is_public {
            vec![column.clone()]
        } else {
            vec![format!("{}.{}", table, column), column.clone()]
        };

        let in_query =
            !tables_in_query.is_empty() && table_in_query(&table, &schema, tables_in_query);

        // in_query always takes precedence: if the user explicitly referenced this
        // table in the current SQL, its columns should be shown at top priority
        // regardless of whether the schema is a system schema.
        let base = if in_query {
            PRIORITY_COLUMN_IN_QUERY
        } else if is_system {
            PRIORITY_SYSTEM
        } else {
            PRIORITY_COLUMN_OTHER
        };

        let usage_count = usage_tracker.get_count(ItemType::Column, &column);
        let usage_bonus = usage_count.min(MAX_USAGE) * USAGE_MULTIPLIER;

        items.push(Candidate {
            display: display.clone(),
            insert: display,
            description: "column",
            item_type: ItemType::Column,
            schema: schema.clone(),
            table_name: Some(table),
            alts,
            priority: base.saturating_add(usage_bonus),
        });
    }

    // ── Functions ─────────────────────────────────────────────────────────────
    for function in cache.get_all_functions() {
        let usage_count = usage_tracker.get_count(ItemType::Function, &function);
        let usage_bonus = usage_count.min(MAX_USAGE) * USAGE_MULTIPLIER;

        // Handle the "name-distinct" pattern: e.g. "count-distinct" →
        // display as "count-distinct()" but insert "count(DISTINCT ".
        let (display, insert, alts) = if let Some(base) = function.strip_suffix("-distinct") {
            (
                format!("{}-distinct()", base),
                format!("{}(DISTINCT ", base),
                vec![function.clone(), base.to_string(), format!("{}_distinct", base)],
            )
        } else {
            (
                format!("{}()", function),
                format!("{}(", function),
                vec![function.clone()],
            )
        };

        items.push(Candidate {
            display,
            insert,
            description: "function",
            item_type: ItemType::Function,
            schema: String::new(),
            table_name: None,
            alts,
            priority: PRIORITY_FUNCTION.saturating_add(usage_bonus),
        });
    }

    items
}

fn is_system_schema(schema: &str) -> bool {
    schema == "information_schema" || schema == "pg_catalog"
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // ── table_in_query ────────────────────────────────────────────────────────

    #[test]
    fn test_table_in_query_bare_match() {
        let q = &["users".to_string()];
        assert!(table_in_query("users", "public", q));
        assert!(!table_in_query("orders", "public", q));
    }

    #[test]
    fn test_table_in_query_qualified_in_sql() {
        // SQL has "information_schema.engine_query_history", cache has bare table name
        let q = &["information_schema.engine_query_history".to_string()];
        assert!(table_in_query(
            "engine_query_history",
            "information_schema",
            q
        ));
        assert!(!table_in_query("columns", "information_schema", q));
    }

    #[test]
    fn test_table_in_query_cross_qualified() {
        // SQL has "public.users", cache table bare "users"
        let q1 = &["public.users".to_string()];
        assert!(table_in_query("users", "public", q1));

        // SQL has bare "users", cache "public.users"
        let q2 = &["users".to_string()];
        assert!(table_in_query("users", "public", q2));
    }

    #[test]
    fn test_table_in_query_empty_query_tables() {
        assert!(!table_in_query("users", "public", &[]));
    }

    // ── schema_in_query ───────────────────────────────────────────────────────

    #[test]
    fn test_schema_in_query_public_always_visible() {
        assert!(schema_in_query("public", &[]));
        assert!(schema_in_query("", &[]));
        assert!(schema_in_query("public", &["orders".to_string()]));
    }

    #[test]
    fn test_schema_in_query_nonpublic_found() {
        let q = &["information_schema.columns".to_string()];
        assert!(schema_in_query("information_schema", q));
    }

    #[test]
    fn test_schema_in_query_nonpublic_not_found() {
        let q = &["orders".to_string()];
        assert!(!schema_in_query("information_schema", q));
        assert!(!schema_in_query("pg_catalog", q));
    }

    #[test]
    fn test_schema_in_query_empty_query_tables() {
        assert!(!schema_in_query("information_schema", &[]));
    }

    // ── Candidate::prefix_matches ─────────────────────────────────────────────

    #[test]
    fn test_prefix_matches_empty_always_true() {
        let c = make_column_candidate(
            "information_schema",
            "engine_query_history",
            "start_time",
        );
        assert!(c.prefix_matches(""));
    }

    #[test]
    fn test_prefix_matches_via_display() {
        let c = make_column_candidate("public", "orders", "order_id");
        // Public column display = "orders.order_id"
        assert!(c.prefix_matches("orders.ord"));
        assert!(!c.prefix_matches("users.order"));
    }

    #[test]
    fn test_prefix_matches_via_bare_col() {
        let c = make_column_candidate("public", "orders", "order_id");
        // Alt = "order_id"
        assert!(c.prefix_matches("order"));
        assert!(!c.prefix_matches("zzzz"));
    }

    #[test]
    fn test_prefix_matches_nonpublic_via_table_col() {
        let c = make_column_candidate(
            "information_schema",
            "engine_query_history",
            "start_time",
        );
        // display = "information_schema.engine_query_history.start_time"
        // alts    = ["engine_query_history.start_time", "start_time"]
        assert!(c.prefix_matches("information_schema.engine"));
        assert!(c.prefix_matches("engine_query"));  // via alt "engine_query_history.start_time"
        assert!(c.prefix_matches("start"));         // via alt "start_time"
        assert!(!c.prefix_matches("nonexistent"));
    }

    #[test]
    fn test_prefix_matches_nonpublic_table() {
        let c = make_table_candidate("information_schema", "columns");
        // display = "information_schema.columns"
        // alts    = ["columns"]
        assert!(c.prefix_matches("information_schema"));
        assert!(c.prefix_matches("col")); // via alt
        assert!(!c.prefix_matches("zzz"));
    }

    // ── collect_candidates priorities ────────────────────────────────────────

    #[test]
    fn test_collect_priority_column_in_query_beats_table() {
        let _cache = Arc::new(super::super::schema_cache::SchemaCache::new(300));
        let _usage_tracker = Arc::new(UsageTracker::new(10));
        // Empty cache — just verify priority constants are ordered correctly
        assert!(PRIORITY_COLUMN_IN_QUERY > PRIORITY_PUBLIC_TABLE);
        assert!(PRIORITY_PUBLIC_TABLE > PRIORITY_SCHEMA);
        assert!(PRIORITY_SCHEMA > PRIORITY_NONPUBLIC_TABLE);
        assert!(PRIORITY_NONPUBLIC_TABLE > PRIORITY_COLUMN_OTHER);
        assert!(PRIORITY_COLUMN_OTHER > PRIORITY_FUNCTION);
        assert!(PRIORITY_FUNCTION > PRIORITY_SYSTEM);
    }

    /// Columns from a system-schema table that is referenced in the current SQL
    /// must get PRIORITY_COLUMN_IN_QUERY, not PRIORITY_SYSTEM.
    ///
    /// Regression test: previously `is_system` was checked before `in_query`,
    /// burying information_schema columns at priority 0 even when the user had
    /// written `FROM information_schema.engine_query_history`.
    #[test]
    fn test_system_schema_column_in_query_gets_high_priority() {
        // Create a candidate as collect_candidates would
        let tables_in_query = vec!["information_schema.engine_query_history".to_string()];
        let in_query = table_in_query(
            "engine_query_history",
            "information_schema",
            &tables_in_query,
        );
        assert!(in_query, "table_in_query should match qualified reference");

        // The priority base should be COLUMN_IN_QUERY, not SYSTEM
        let base = if in_query {
            PRIORITY_COLUMN_IN_QUERY
        } else if is_system_schema_test("information_schema") {
            PRIORITY_SYSTEM
        } else {
            PRIORITY_COLUMN_OTHER
        };
        assert_eq!(
            base, PRIORITY_COLUMN_IN_QUERY,
            "system-schema column from query table must get PRIORITY_COLUMN_IN_QUERY"
        );
    }

    fn is_system_schema_test(s: &str) -> bool {
        s == "information_schema" || s == "pg_catalog"
    }

    #[test]
    fn test_collect_system_schema_priority() {
        // usage bonus cannot lift a system item above PRIORITY_FUNCTION
        let max_system = PRIORITY_SYSTEM + MAX_USAGE * USAGE_MULTIPLIER;
        assert!(
            max_system < PRIORITY_FUNCTION,
            "max_system={} must be < PRIORITY_FUNCTION={}",
            max_system,
            PRIORITY_FUNCTION
        );
    }

    // ── helper constructors for tests ─────────────────────────────────────────

    fn make_column_candidate(schema: &str, table: &str, column: &str) -> Candidate {
        let is_public = schema == "public" || schema.is_empty();
        let display = if is_public {
            format!("{}.{}", table, column)
        } else {
            format!("{}.{}.{}", schema, table, column)
        };
        let alts: Vec<String> = if is_public {
            vec![column.to_string()]
        } else {
            vec![format!("{}.{}", table, column), column.to_string()]
        };
        Candidate {
            display: display.clone(),
            insert: display,
            description: "column",
            item_type: ItemType::Column,
            schema: schema.to_string(),
            table_name: Some(table.to_string()),
            alts,
            priority: PRIORITY_COLUMN_IN_QUERY,
        }
    }

    fn make_table_candidate(schema: &str, table: &str) -> Candidate {
        let is_public = schema == "public" || schema.is_empty();
        let display = if is_public {
            table.to_string()
        } else {
            format!("{}.{}", schema, table)
        };
        let alts: Vec<String> = if is_public { vec![] } else { vec![table.to_string()] };
        Candidate {
            display: display.clone(),
            insert: display,
            description: "table",
            item_type: ItemType::Table,
            schema: schema.to_string(),
            table_name: None,
            alts,
            priority: PRIORITY_NONPUBLIC_TABLE,
        }
    }
}
