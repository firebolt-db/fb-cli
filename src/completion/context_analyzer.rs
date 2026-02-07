/// Analyzes SQL context to extract table names and identify system schemas
pub struct ContextAnalyzer;

impl ContextAnalyzer {
    /// Extract table names from the current SQL statement
    /// Looks for patterns like "FROM table", "JOIN table", "UPDATE table"
    pub fn extract_tables(sql: &str) -> Vec<String> {
        let mut tables = Vec::new();
        let sql_upper = sql.to_uppercase();

        // Pattern: FROM table_name
        if let Some(from_pos) = sql_upper.find("FROM") {
            let after_from = &sql[from_pos + 4..];
            if let Some(word) = Self::extract_first_identifier(after_from) {
                tables.push(word);
            }
        }

        // Pattern: JOIN table_name
        for (idx, _) in sql_upper.match_indices("JOIN") {
            let after_join = &sql[idx + 4..];
            if let Some(word) = Self::extract_first_identifier(after_join) {
                tables.push(word);
            }
        }

        // Pattern: UPDATE table_name
        if let Some(update_pos) = sql_upper.find("UPDATE") {
            let after_update = &sql[update_pos + 6..];
            if let Some(word) = Self::extract_first_identifier(after_update) {
                tables.push(word);
            }
        }

        // Pattern: INTO table_name (for INSERT statements)
        if let Some(into_pos) = sql_upper.find("INTO") {
            let after_into = &sql[into_pos + 4..];
            if let Some(word) = Self::extract_first_identifier(after_into) {
                tables.push(word);
            }
        }

        tables
    }

    /// Check if a qualified name belongs to a system schema
    pub fn is_system_schema(qualified_name: &str) -> bool {
        qualified_name.starts_with("information_schema.")
            || qualified_name.starts_with("pg_catalog.")
            || qualified_name == "information_schema"
            || qualified_name == "pg_catalog"
    }

    /// Extract the first SQL identifier from text
    /// Handles schema-qualified names (e.g., "public.users")
    fn extract_first_identifier(text: &str) -> Option<String> {
        let trimmed = text.trim();
        let mut identifier = String::new();

        for ch in trimmed.chars() {
            if ch.is_alphanumeric() || ch == '_' || ch == '.' {
                identifier.push(ch);
            } else if !identifier.is_empty() {
                // Stop at first non-identifier character
                break;
            }
        }

        if identifier.is_empty() {
            None
        } else {
            Some(identifier)
        }
    }

    /// Extract the table name from a qualified column reference
    /// E.g., "users.user_id" -> Some("users")
    pub fn extract_table_from_qualified_column(qualified_column: &str) -> Option<String> {
        if let Some(dot_pos) = qualified_column.rfind('.') {
            Some(qualified_column[..dot_pos].to_string())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_tables_from_simple_query() {
        let tables = ContextAnalyzer::extract_tables("SELECT * FROM users");
        assert_eq!(tables, vec!["users"]);
    }

    #[test]
    fn test_extract_tables_from_join() {
        let tables = ContextAnalyzer::extract_tables(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        );
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_tables_with_schema() {
        let tables = ContextAnalyzer::extract_tables("SELECT * FROM public.users");
        assert_eq!(tables, vec!["public.users"]);
    }

    #[test]
    fn test_extract_tables_from_update() {
        let tables = ContextAnalyzer::extract_tables("UPDATE users SET active = true");
        assert_eq!(tables, vec!["users"]);
    }

    #[test]
    fn test_extract_tables_from_insert() {
        let tables = ContextAnalyzer::extract_tables("INSERT INTO users (name) VALUES ('Alice')");
        assert_eq!(tables, vec!["users"]);
    }

    #[test]
    fn test_is_system_schema() {
        assert!(ContextAnalyzer::is_system_schema("information_schema.tables"));
        assert!(ContextAnalyzer::is_system_schema("pg_catalog.pg_class"));
        assert!(ContextAnalyzer::is_system_schema("information_schema"));
        assert!(ContextAnalyzer::is_system_schema("pg_catalog"));

        assert!(!ContextAnalyzer::is_system_schema("public.users"));
        assert!(!ContextAnalyzer::is_system_schema("users"));
        assert!(!ContextAnalyzer::is_system_schema("my_schema.table"));
    }

    #[test]
    fn test_extract_table_from_qualified_column() {
        assert_eq!(
            ContextAnalyzer::extract_table_from_qualified_column("users.user_id"),
            Some("users".to_string())
        );
        assert_eq!(
            ContextAnalyzer::extract_table_from_qualified_column("public.users.user_id"),
            Some("public.users".to_string())
        );
        assert_eq!(
            ContextAnalyzer::extract_table_from_qualified_column("user_id"),
            None
        );
    }

    #[test]
    fn test_extract_first_identifier() {
        assert_eq!(
            ContextAnalyzer::extract_first_identifier("  users  "),
            Some("users".to_string())
        );
        assert_eq!(
            ContextAnalyzer::extract_first_identifier("public.users WHERE"),
            Some("public.users".to_string())
        );
        assert_eq!(
            ContextAnalyzer::extract_first_identifier("users,"),
            Some("users".to_string())
        );
    }
}
