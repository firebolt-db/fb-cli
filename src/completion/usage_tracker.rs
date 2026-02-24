use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemType {
    Table,
    Column,
    Function,
    Schema,
}

/// Tracks usage frequency of tables, columns, and functions to enable intelligent prioritization
pub struct UsageTracker {
    // Item name -> usage count
    table_counts: Arc<RwLock<HashMap<String, u32>>>,
    column_counts: Arc<RwLock<HashMap<String, u32>>>,
    function_counts: Arc<RwLock<HashMap<String, u32>>>,

    // Recent queries (ring buffer of last N)
    recent_queries: Arc<RwLock<Vec<String>>>,
    max_recent: usize,
}

impl UsageTracker {
    /// Create a new UsageTracker with a maximum number of recent queries to track
    pub fn new(max_recent: usize) -> Self {
        Self {
            table_counts: Arc::new(RwLock::new(HashMap::new())),
            column_counts: Arc::new(RwLock::new(HashMap::new())),
            function_counts: Arc::new(RwLock::new(HashMap::new())),
            recent_queries: Arc::new(RwLock::new(Vec::new())),
            max_recent,
        }
    }

    /// Track a query execution by extracting table and column names
    pub fn track_query(&self, query: &str) {
        // 1. Add to recent queries (ring buffer)
        {
            let mut recent = self.recent_queries.write().unwrap();
            recent.push(query.to_string());
            if recent.len() > self.max_recent {
                recent.remove(0);
            }
        }

        // 2. Extract table and column names
        let tables = Self::extract_table_names(query);
        let columns = Self::extract_column_names(query);

        // 3. Increment usage counts
        {
            let mut table_counts = self.table_counts.write().unwrap();
            for table in tables {
                *table_counts.entry(table).or_insert(0) += 1;
            }
        }

        {
            let mut column_counts = self.column_counts.write().unwrap();
            for column in columns {
                *column_counts.entry(column).or_insert(0) += 1;
            }
        }
    }

    /// Get usage count for an item
    pub fn get_count(&self, item_type: ItemType, name: &str) -> u32 {
        match item_type {
            ItemType::Table => {
                let counts = self.table_counts.read().unwrap();
                counts.get(name).copied().unwrap_or(0)
            }
            ItemType::Column => {
                let counts = self.column_counts.read().unwrap();
                counts.get(name).copied().unwrap_or(0)
            }
            ItemType::Function => {
                let counts = self.function_counts.read().unwrap();
                counts.get(name).copied().unwrap_or(0)
            }
            ItemType::Schema => 0,
        }
    }

    #[allow(dead_code)]
    /// Check if an item name appears in any recent query
    pub fn was_used_recently(&self, name: &str) -> bool {
        let recent = self.recent_queries.read().unwrap();
        recent.iter().any(|q| q.contains(name))
    }

    /// Extract table names from SQL query using simple pattern matching
    fn extract_table_names(query: &str) -> Vec<String> {
        let mut tables = Vec::new();
        let query_upper = query.to_uppercase();

        // Pattern: FROM table_name
        if let Some(from_pos) = query_upper.find("FROM") {
            let after_from = &query[from_pos + 4..];
            if let Some(word) = Self::extract_first_word(after_from) {
                tables.push(word);
            }
        }

        // Pattern: JOIN table_name
        for (idx, _) in query_upper.match_indices("JOIN") {
            let after_join = &query[idx + 4..];
            if let Some(word) = Self::extract_first_word(after_join) {
                tables.push(word);
            }
        }

        // Pattern: UPDATE table_name
        if let Some(update_pos) = query_upper.find("UPDATE") {
            let after_update = &query[update_pos + 6..];
            if let Some(word) = Self::extract_first_word(after_update) {
                tables.push(word);
            }
        }

        // Pattern: INTO table_name
        if let Some(into_pos) = query_upper.find("INTO") {
            let after_into = &query[into_pos + 4..];
            if let Some(word) = Self::extract_first_word(after_into) {
                tables.push(word);
            }
        }

        tables
    }

    /// Extract column names from SQL query using simple pattern matching
    fn extract_column_names(query: &str) -> Vec<String> {
        let mut columns = Vec::new();
        let query_upper = query.to_uppercase();

        // Pattern: SELECT columns FROM
        if let Some(select_pos) = query_upper.find("SELECT") {
            if let Some(from_pos) = query_upper.find("FROM") {
                // Only extract if FROM comes after SELECT
                if from_pos > select_pos + 6 {
                    let between = &query[select_pos + 6..from_pos];
                    columns.extend(Self::extract_column_list(between));
                }
            }
        }

        // Pattern: WHERE column = value
        for (idx, _) in query_upper.match_indices("WHERE") {
            let after_where = &query[idx + 5..];
            if let Some(word) = Self::extract_first_word(after_where) {
                columns.push(word);
            }
        }

        // Pattern: ORDER BY column
        for (idx, _) in query_upper.match_indices("ORDER BY") {
            let after_order = &query[idx + 8..];
            if let Some(word) = Self::extract_first_word(after_order) {
                columns.push(word);
            }
        }

        // Pattern: GROUP BY column
        for (idx, _) in query_upper.match_indices("GROUP BY") {
            let after_group = &query[idx + 8..];
            if let Some(word) = Self::extract_first_word(after_group) {
                columns.push(word);
            }
        }

        columns
    }

    /// Extract first identifier from text (handles schema.table notation)
    fn extract_first_word(text: &str) -> Option<String> {
        let trimmed = text.trim();
        let mut word = String::new();

        for ch in trimmed.chars() {
            if ch.is_alphanumeric() || ch == '_' || ch == '.' {
                word.push(ch);
            } else if !word.is_empty() {
                break;
            }
        }

        if word.is_empty() {
            None
        } else {
            Some(word)
        }
    }

    /// Extract comma-separated column list
    fn extract_column_list(text: &str) -> Vec<String> {
        let mut columns = Vec::new();

        for part in text.split(',') {
            let trimmed = part.trim();

            // Skip * and common SQL functions/keywords
            if trimmed == "*" || trimmed.is_empty() {
                continue;
            }

            // Extract just the column name (before AS, spaces, etc.)
            if let Some(word) = Self::extract_first_word(trimmed) {
                // Remove table prefix if present (e.g., "users.id" -> "id")
                let column_name = if let Some(dot_pos) = word.rfind('.') {
                    word[dot_pos + 1..].to_string()
                } else {
                    word
                };

                // Filter out SQL keywords
                let upper = column_name.to_uppercase();
                if !["AS", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "EXISTS"].contains(&upper.as_str()) {
                    columns.push(column_name);
                }
            }
        }

        columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_query_increments_table_counts() {
        let tracker = UsageTracker::new(10);
        tracker.track_query("SELECT * FROM users");
        assert_eq!(tracker.get_count(ItemType::Table, "users"), 1);

        tracker.track_query("SELECT * FROM users");
        assert_eq!(tracker.get_count(ItemType::Table, "users"), 2);
    }

    #[test]
    fn test_track_query_increments_column_counts() {
        let tracker = UsageTracker::new(10);
        tracker.track_query("SELECT user_id, username FROM users");
        assert_eq!(tracker.get_count(ItemType::Column, "user_id"), 1);
        assert_eq!(tracker.get_count(ItemType::Column, "username"), 1);
    }

    #[test]
    fn test_recent_queries_ring_buffer() {
        let tracker = UsageTracker::new(2);
        tracker.track_query("query1");
        tracker.track_query("query2");
        tracker.track_query("query3"); // Should evict query1

        assert!(!tracker.was_used_recently("query1"));
        assert!(tracker.was_used_recently("query2"));
        assert!(tracker.was_used_recently("query3"));
    }

    #[test]
    fn test_extract_table_names_from_query() {
        let tables = UsageTracker::extract_table_names(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        );
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_table_names_with_schema() {
        let tables = UsageTracker::extract_table_names("SELECT * FROM public.users");
        assert_eq!(tables, vec!["public.users"]);
    }

    #[test]
    fn test_extract_column_names() {
        let columns = UsageTracker::extract_column_names(
            "SELECT user_id, username FROM users WHERE active = true"
        );
        assert!(columns.contains(&"user_id".to_string()));
        assert!(columns.contains(&"username".to_string()));
        assert!(columns.contains(&"active".to_string()));
    }

    #[test]
    fn test_was_used_recently() {
        let tracker = UsageTracker::new(10);
        tracker.track_query("SELECT * FROM users");
        assert!(tracker.was_used_recently("users"));
        assert!(!tracker.was_used_recently("orders"));
    }

    #[test]
    fn test_extract_column_names_with_keywords_out_of_order() {
        // Regression test for panic when FROM appears before SELECT
        let columns = UsageTracker::extract_column_names("from test select val");
        // Should not panic, and should not extract columns since FROM comes before SELECT
        assert_eq!(columns.len(), 0);
    }

    #[test]
    fn test_extract_column_names_edge_cases() {
        // Test with only SELECT, no FROM
        let columns = UsageTracker::extract_column_names("SELECT user_id");
        assert_eq!(columns.len(), 0); // No FROM, so no columns extracted

        // Test with FROM immediately after SELECT (no space for columns)
        let columns = UsageTracker::extract_column_names("SELECTFROM users");
        assert_eq!(columns.len(), 0);
    }
}
