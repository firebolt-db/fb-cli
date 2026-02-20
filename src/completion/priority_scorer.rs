use super::context_analyzer::ContextAnalyzer;
use super::usage_tracker::{ItemType, UsageTracker};
use std::sync::Arc;

/// Calculates priority scores for auto-completion suggestions
pub struct PriorityScorer {
    usage_tracker: Arc<UsageTracker>,
}

/// A suggestion with its calculated priority score
#[derive(Debug, Clone)]
pub struct ScoredSuggestion {
    pub name: String,
    pub item_type: ItemType,
    pub score: u32,
    /// For column items: the table the column belongs to (short name, no schema prefix).
    pub table_name: Option<String>,
}

/// Priority classes determine the base score before usage bonuses
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PriorityClass {
    /// Columns from tables mentioned in current query - highest priority
    ColumnFromQueryTable = 5000,

    /// Tables - high priority when no table context
    Table = 4000,

    /// Qualified columns from tables NOT in query
    QualifiedColumnOtherTable = 3000,

    /// Unqualified columns from tables NOT in query
    UnqualifiedColumnOtherTable = 2000,

    /// Functions - lower priority than columns
    Function = 1000,

    /// System schema items - lowest priority
    SystemSchema = 0,
}

impl PriorityScorer {
    pub fn new(usage_tracker: Arc<UsageTracker>) -> Self {
        Self { usage_tracker }
    }

    /// Calculate priority score for a suggestion
    ///
    /// # Arguments
    /// * `name` - The suggestion name (e.g., "users", "user_id", "users.user_id")
    /// * `item_type` - Whether this is a table or column
    /// * `tables_in_statement` - Tables mentioned in the current SQL statement
    /// * `column_table` - For columns, the table they belong to (if known)
    ///
    /// # Returns
    /// Priority score (higher = more relevant)
    pub fn score(
        &self,
        name: &str,
        item_type: ItemType,
        tables_in_statement: &[String],
        column_table: Option<&str>,
    ) -> u32 {
        // Get usage count
        let usage_count = self.usage_tracker.get_count(item_type, name);

        // Calculate base score from priority class
        let base_score = self.calculate_priority_class(
            name,
            item_type,
            tables_in_statement,
            column_table,
        ) as u32;

        // Add usage bonus (max 99 uses = 990 points, ensures higher class always wins)
        let usage_bonus = usage_count.min(99) * 10;

        base_score + usage_bonus
    }

    /// Determine the priority class for an item
    fn calculate_priority_class(
        &self,
        name: &str,
        item_type: ItemType,
        tables_in_statement: &[String],
        column_table: Option<&str>,
    ) -> PriorityClass {
        // System schemas always get lowest priority
        if ContextAnalyzer::is_system_schema(name) {
            return PriorityClass::SystemSchema;
        }

        match item_type {
            ItemType::Table => PriorityClass::Table,

            ItemType::Column => {
                // Check if column belongs to a table in the statement
                if let Some(table) = column_table {
                    // Normalize table name for comparison
                    let normalized_table = table.to_lowercase();

                    // Check if this table is mentioned in the statement
                    let table_in_statement = tables_in_statement.iter().any(|t| {
                        let normalized_t = t.to_lowercase();
                        normalized_t == normalized_table
                            || normalized_t.ends_with(&format!(".{}", normalized_table))
                            || normalized_table.ends_with(&format!(".{}", normalized_t))
                    });

                    if table_in_statement {
                        // Column from table in query - highest priority
                        return PriorityClass::ColumnFromQueryTable;
                    }
                }

                // Column from other table - check if qualified
                let is_qualified = name.contains('.');

                if is_qualified {
                    PriorityClass::QualifiedColumnOtherTable
                } else {
                    PriorityClass::UnqualifiedColumnOtherTable
                }
            }

            ItemType::Function => PriorityClass::Function,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_scorer() -> PriorityScorer {
        let tracker = Arc::new(UsageTracker::new(10));
        PriorityScorer::new(tracker)
    }

    #[test]
    fn test_tables_prioritized_when_no_context() {
        let scorer = create_test_scorer();

        let table_score = scorer.score("users", ItemType::Table, &[], None);
        let column_score = scorer.score("user_id", ItemType::Column, &[], None);

        assert!(table_score > column_score, "Tables should be prioritized over columns when no context");
        assert!(table_score >= 4000, "Table should be in Table priority class");
        assert!(column_score < 3000, "Column should be in lower priority class");
    }

    #[test]
    fn test_columns_prioritized_when_table_in_statement() {
        let scorer = create_test_scorer();

        let column_score = scorer.score(
            "user_id",
            ItemType::Column,
            &["users".to_string()],
            Some("users"),
        );

        assert!(column_score >= 5000, "Column from query table should be highest priority");
    }

    #[test]
    fn test_system_schema_deprioritized() {
        let scorer = create_test_scorer();

        let user_table_score = scorer.score("users", ItemType::Table, &[], None);
        let system_table_score = scorer.score("information_schema.tables", ItemType::Table, &[], None);

        assert!(user_table_score > system_table_score, "User tables should be prioritized over system schemas");
        assert!(system_table_score < 1000, "System schema should be in lowest priority class");
    }

    #[test]
    fn test_qualified_vs_unqualified_columns() {
        let scorer = create_test_scorer();

        let qualified_score = scorer.score(
            "orders.order_id",
            ItemType::Column,
            &["users".to_string()], // Different table in statement
            Some("orders"),
        );

        let unqualified_score = scorer.score(
            "order_id",
            ItemType::Column,
            &["users".to_string()],
            Some("orders"),
        );

        assert!(qualified_score > unqualified_score, "Qualified columns should rank above unqualified");
        assert!(qualified_score >= 3000 && qualified_score < 4000, "Qualified column should be in correct class");
        assert!(unqualified_score >= 2000 && unqualified_score < 3000, "Unqualified column should be in correct class");
    }

    #[test]
    fn test_usage_bonus_within_class() {
        let tracker = Arc::new(UsageTracker::new(10));

        // Simulate usage
        for _ in 0..50 {
            tracker.track_query("SELECT * FROM users");
        }
        for _ in 0..10 {
            tracker.track_query("SELECT * FROM orders");
        }

        let scorer = PriorityScorer::new(tracker);

        let users_score = scorer.score("users", ItemType::Table, &[], None);
        let orders_score = scorer.score("orders", ItemType::Table, &[], None);

        assert!(users_score > orders_score, "More frequently used item should score higher");

        // Both should be in same priority class (4000-4999)
        assert!(users_score >= 4000 && users_score < 5000);
        assert!(orders_score >= 4000 && orders_score < 5000);
    }

    #[test]
    fn test_priority_class_beats_usage_count() {
        let tracker = Arc::new(UsageTracker::new(10));

        // Give column very high usage
        for _ in 0..99 {
            tracker.track_query("SELECT order_id FROM orders");
        }

        let scorer = PriorityScorer::new(tracker.clone());

        // Column with high usage from other table
        let column_score = scorer.score(
            "order_id",
            ItemType::Column,
            &["users".to_string()], // Different table
            Some("orders"),
        );

        // Table with no usage
        let table_score = scorer.score("users", ItemType::Table, &[], None);

        // Table should still win because higher priority class
        assert!(table_score > column_score, "Higher priority class should beat usage count");
    }

    #[test]
    fn test_schema_qualified_table_matching() {
        let scorer = create_test_scorer();

        // Test that schema-qualified names are handled correctly
        let score1 = scorer.score(
            "user_id",
            ItemType::Column,
            &["public.users".to_string()],
            Some("users"),
        );

        let score2 = scorer.score(
            "user_id",
            ItemType::Column,
            &["users".to_string()],
            Some("public.users"),
        );

        // Both should recognize the table match
        assert!(score1 >= 5000, "Should match schema.table to table");
        assert!(score2 >= 5000, "Should match table to schema.table");
    }
}
