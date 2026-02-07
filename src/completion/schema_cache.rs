use crate::context::Context;
use crate::query::query_silent;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct TableMetadata {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnMetadata>,
}

#[derive(Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
}

pub struct SchemaCache {
    tables: Arc<RwLock<HashMap<String, TableMetadata>>>,
    functions: Arc<RwLock<HashSet<String>>>,
    keywords: HashSet<String>,
    last_refresh: Arc<RwLock<Option<Instant>>>,
    ttl: Duration,
    refreshing: Arc<RwLock<bool>>,
}

impl SchemaCache {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            functions: Arc::new(RwLock::new(HashSet::new())),
            keywords: Self::load_keywords(),
            last_refresh: Arc::new(RwLock::new(None)),
            ttl: Duration::from_secs(ttl_seconds),
            refreshing: Arc::new(RwLock::new(false)),
        }
    }

    fn load_keywords() -> HashSet<String> {
        let keywords = vec![
            "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
            "ON", "GROUP", "ORDER", "BY", "HAVING", "LIMIT", "OFFSET", "UNION", "INTERSECT",
            "EXCEPT", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "DROP",
            "ALTER", "TABLE", "DATABASE", "INDEX", "VIEW", "AS", "DISTINCT", "ALL", "AND",
            "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "CASE", "WHEN",
            "THEN", "ELSE", "END", "WITH", "RECURSIVE", "ASC", "DESC", "COUNT", "SUM", "AVG",
            "MIN", "MAX", "CAST", "SUBSTRING", "TRIM", "UPPER", "LOWER", "COALESCE", "NULLIF",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "AUTO_INCREMENT",
            "EXPLAIN", "DESCRIBE", "SHOW", "USE", "GRANT", "REVOKE", "COMMIT", "ROLLBACK",
            "SAVEPOINT", "TRANSACTION", "BEGIN", "START", "TRUNCATE", "RENAME", "ADD", "MODIFY",
            "COLUMN", "CONSTRAINT", "CASCADE", "RESTRICT",
        ];

        keywords.into_iter().map(String::from).collect()
    }

    /// Checks if cache is stale based on TTL
    pub fn is_stale(&self) -> bool {
        let last_refresh = self.last_refresh.read().unwrap();
        match *last_refresh {
            Some(instant) => instant.elapsed() > self.ttl,
            None => true, // Never refreshed
        }
    }

    /// Returns true if a refresh is currently in progress
    pub fn is_refreshing(&self) -> bool {
        *self.refreshing.read().unwrap()
    }

    /// Marks refresh as started
    fn start_refresh(&self) {
        *self.refreshing.write().unwrap() = true;
    }

    /// Marks refresh as complete and updates timestamp
    fn complete_refresh(&self) {
        *self.refreshing.write().unwrap() = false;
        *self.last_refresh.write().unwrap() = Some(Instant::now());
    }

    /// Get all keywords matching prefix
    pub fn get_keywords(&self, prefix: &str) -> Vec<String> {
        let prefix_lower = prefix.to_lowercase();
        self.keywords
            .iter()
            .filter(|k| k.to_lowercase().starts_with(&prefix_lower))
            .cloned()
            .collect()
    }

    /// Get all table names matching prefix
    pub fn get_tables(&self, prefix: &str) -> Vec<String> {
        let prefix_lower = prefix.to_lowercase();
        let tables = self.tables.read().unwrap();
        tables
            .values()
            .map(|t| {
                if t.schema_name == "public" || t.schema_name.is_empty() {
                    t.table_name.clone()
                } else {
                    format!("{}.{}", t.schema_name, t.table_name)
                }
            })
            .filter(|name| name.to_lowercase().starts_with(&prefix_lower))
            .collect()
    }

    /// Get all column names matching prefix
    pub fn get_columns(&self, prefix: &str) -> Vec<String> {
        let prefix_lower = prefix.to_lowercase();
        let tables = self.tables.read().unwrap();
        let mut columns = HashSet::new();
        for table in tables.values() {
            for column in &table.columns {
                columns.insert(column.name.clone());
            }
        }
        columns
            .into_iter()
            .filter(|name| name.to_lowercase().starts_with(&prefix_lower))
            .collect()
    }

    /// Get all unique schema names matching prefix
    pub fn get_schemas(&self, prefix: &str) -> Vec<String> {
        let prefix_lower = prefix.to_lowercase();
        let tables = self.tables.read().unwrap();

        let mut schemas = std::collections::HashSet::new();
        for table in tables.values() {
            if !table.schema_name.is_empty() {
                schemas.insert(table.schema_name.clone());
            }
        }

        schemas
            .into_iter()
            .filter(|schema| schema.to_lowercase().starts_with(&prefix_lower))
            .collect()
    }

    /// Get all tables from a specific schema, optionally filtered by table name prefix
    pub fn get_tables_in_schema(&self, schema: &str, table_prefix: &str) -> Vec<String> {
        let schema_lower = schema.to_lowercase();
        let prefix_lower = table_prefix.to_lowercase();
        let tables = self.tables.read().unwrap();

        let mut result: Vec<String> = tables
            .values()
            .filter(|t| t.schema_name.to_lowercase() == schema_lower)
            .filter(|t| {
                if prefix_lower.is_empty() {
                    true
                } else {
                    t.table_name.to_lowercase().starts_with(&prefix_lower)
                }
            })
            .map(|t| t.table_name.clone())
            .collect();

        result.sort();
        result
    }

    /// Get all table names with their schemas matching prefix
    /// Returns Vec<(schema, table)>
    pub fn get_tables_with_schema(&self, prefix: &str) -> Vec<(String, String)> {
        let prefix_lower = prefix.to_lowercase();
        let tables = self.tables.read().unwrap();

        let mut result: Vec<(String, String)> = tables
            .values()
            .filter_map(|t| {
                let short_name = t.table_name.to_lowercase();
                let qualified_name = format!("{}.{}", t.schema_name, t.table_name).to_lowercase();

                // Match either short name or qualified name
                if short_name.starts_with(&prefix_lower) || qualified_name.starts_with(&prefix_lower) {
                    Some((t.schema_name.clone(), t.table_name.clone()))
                } else {
                    None
                }
            })
            .collect();

        // Sort by table name for consistent ordering
        result.sort_by(|a, b| a.1.cmp(&b.1));
        result
    }

    /// Get all column names with their table names matching prefix
    /// Returns Vec<(Option<table>, column)>
    pub fn get_columns_with_table(&self, prefix: &str) -> Vec<(Option<String>, String)> {
        let prefix_lower = prefix.to_lowercase();
        let tables = self.tables.read().unwrap();

        let mut result: Vec<(Option<String>, String)> = Vec::new();
        let mut seen_columns = HashSet::new();

        for table in tables.values() {
            for column in &table.columns {
                let column_name = column.name.to_lowercase();
                let qualified_name = format!("{}.{}", table.table_name, column.name).to_lowercase();

                // Check if matches prefix (either column name or qualified name)
                if column_name.starts_with(&prefix_lower) || qualified_name.starts_with(&prefix_lower) {
                    // Track unique columns to avoid duplicates
                    let key = format!("{}:{}", table.table_name, column.name);
                    if !seen_columns.contains(&key) {
                        result.push((Some(table.table_name.clone()), column.name.clone()));
                        seen_columns.insert(key);
                    }
                }
            }
        }

        // Sort by column name for consistent ordering
        result.sort_by(|a, b| a.1.cmp(&b.1));
        result
    }

    /// Get the table for a given column name
    /// Returns the first table that contains this column
    pub fn get_table_for_column(&self, column_name: &str) -> Option<String> {
        let tables = self.tables.read().unwrap();
        let column_lower = column_name.to_lowercase();

        for table in tables.values() {
            for column in &table.columns {
                if column.name.to_lowercase() == column_lower {
                    // Return qualified table name
                    return Some(if table.schema_name == "public" || table.schema_name.is_empty() {
                        table.table_name.clone()
                    } else {
                        format!("{}.{}", table.schema_name, table.table_name)
                    });
                }
            }
        }

        None
    }

    /// Synchronous method to get completions from cache
    pub fn get_completions(
        &self,
        context: super::context_detector::CompletionContext,
        prefix: &str,
    ) -> Vec<String> {
        use super::context_detector::CompletionContext;

        let prefix_lower = prefix.to_lowercase();

        match context {
            CompletionContext::Keyword => {
                // Return keywords
                self.keywords
                    .iter()
                    .filter(|k| k.to_lowercase().starts_with(&prefix_lower))
                    .cloned()
                    .collect()
            }
            CompletionContext::TableName => {
                // Return table names
                let tables = self.tables.read().unwrap();
                tables
                    .values()
                    .map(|t| {
                        if t.schema_name == "public" || t.schema_name.is_empty() {
                            t.table_name.clone()
                        } else {
                            format!("{}.{}", t.schema_name, t.table_name)
                        }
                    })
                    .filter(|name| name.to_lowercase().starts_with(&prefix_lower))
                    .collect()
            }
            CompletionContext::ColumnName => {
                // Return column names from all tables
                let tables = self.tables.read().unwrap();
                let mut columns = HashSet::new();
                for table in tables.values() {
                    for column in &table.columns {
                        columns.insert(column.name.clone());
                    }
                }
                columns
                    .into_iter()
                    .filter(|name| name.to_lowercase().starts_with(&prefix_lower))
                    .collect()
            }
            CompletionContext::FunctionName => {
                // Return function names
                let functions = self.functions.read().unwrap();
                functions
                    .iter()
                    .filter(|f| f.to_lowercase().starts_with(&prefix_lower))
                    .cloned()
                    .collect()
            }
            CompletionContext::SchemaQualified => {
                // Handle schema.table completion
                if let Some(dot_pos) = prefix.rfind('.') {
                    let schema = &prefix[..dot_pos];
                    let table_prefix = &prefix[dot_pos + 1..];
                    let table_prefix_lower = table_prefix.to_lowercase();

                    let tables = self.tables.read().unwrap();
                    tables
                        .values()
                        .filter(|t| t.schema_name == schema)
                        .map(|t| format!("{}.{}", t.schema_name, t.table_name))
                        .filter(|name| {
                            name.split('.').nth(1).unwrap_or("")
                                .to_lowercase()
                                .starts_with(&table_prefix_lower)
                        })
                        .collect()
                } else {
                    Vec::new()
                }
            }
            CompletionContext::Nothing => Vec::new(),
        }
    }

    /// Async method to refresh schema from database
    pub async fn refresh(&self, context: &mut Context) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check if already refreshing
        if self.is_refreshing() {
            return Ok(());
        }

        self.start_refresh();

        let result = self.do_refresh(context).await;

        if result.is_ok() {
            self.complete_refresh();
        } else {
            // Still mark as complete to avoid blocking future attempts
            *self.refreshing.write().unwrap() = false;
        }

        result
    }

    async fn do_refresh(&self, context: &mut Context) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Query tables (including system schemas - they'll be deprioritized by the scorer)
        let tables_query = "SELECT table_schema, table_name \
                           FROM information_schema.tables \
                           ORDER BY table_schema, table_name";

        let tables_result = query_silent(context, tables_query).await;

        // Query columns (including system schemas - they'll be deprioritized by the scorer)
        let columns_query = "SELECT table_schema, table_name, column_name, data_type \
                            FROM information_schema.columns \
                            ORDER BY table_schema, table_name, ordinal_position";

        let columns_result = query_silent(context, columns_query).await;

        // Query functions
        let functions_query = "SELECT routine_name \
                              FROM information_schema.routines \
                              WHERE routine_schema NOT IN ('information_schema', 'pg_catalog') \
                              ORDER BY routine_name";

        let functions_result = query_silent(context, functions_query).await;

        // Parse and populate cache
        let mut new_tables = HashMap::new();

        // Parse tables
        match tables_result {
            Ok(tables_output) => {
                if let Some(table_list) = Self::parse_tables(&tables_output) {
                    for (schema, table) in table_list {
                        let key = format!("{}.{}", schema, table);
                        new_tables.insert(
                            key,
                            TableMetadata {
                                schema_name: schema,
                                table_name: table,
                                columns: Vec::new(),
                            },
                        );
                    }
                } else {
                    eprintln!("Warning: Failed to parse tables from schema query");
                    eprintln!("Output was: {}", &tables_output[..tables_output.len().min(500)]);
                }
            }
            Err(e) => {
                eprintln!("Warning: Tables query failed: {}", e);
            }
        }

        // Parse columns and add to tables
        match columns_result {
            Ok(columns_output) => {
                if let Some(column_list) = Self::parse_columns(&columns_output) {
                    for (schema, table, column, data_type) in column_list {
                        let key = format!("{}.{}", schema, table);
                        if let Some(table_meta) = new_tables.get_mut(&key) {
                            table_meta.columns.push(ColumnMetadata {
                                name: column,
                                data_type,
                            });
                        }
                    }
                } else {
                    eprintln!("Warning: Failed to parse columns from schema query");
                }
            }
            Err(e) => {
                eprintln!("Warning: Columns query failed: {}", e);
            }
        }

        // Update tables cache
        let num_tables = new_tables.len();
        let num_columns: usize = new_tables.values().map(|t| t.columns.len()).sum();
        *self.tables.write().unwrap() = new_tables;

        // Parse functions
        let mut num_functions = 0;
        match functions_result {
            Ok(functions_output) => {
                if let Some(function_list) = Self::parse_functions(&functions_output) {
                    num_functions = function_list.len();
                    *self.functions.write().unwrap() = function_list.into_iter().collect();
                } else {
                    eprintln!("Warning: Failed to parse functions from schema query");
                }
            }
            Err(e) => {
                eprintln!("Warning: Functions query failed: {}", e);
            }
        }

        Ok(())
    }

    fn parse_tables(output: &str) -> Option<Vec<(String, String)>> {
        let mut result = Vec::new();

        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse message-based format
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Check for DATA message type
                if let Some(msg_type) = json.get("message_type").and_then(|v| v.as_str()) {
                    if msg_type == "DATA" {
                        // Extract data array: [["public","test"],["public","test_view"]]
                        if let Some(data) = json.get("data").and_then(|v| v.as_array()) {
                            for row in data {
                                if let Some(row_array) = row.as_array() {
                                    if row_array.len() >= 2 {
                                        let schema = row_array[0].as_str().unwrap_or("").to_string();
                                        let table = row_array[1].as_str().unwrap_or("").to_string();
                                        if !schema.is_empty() && !table.is_empty() {
                                            result.push((schema, table));
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Try old JSONLines_Compact format for backwards compatibility
                    if let (Some(schema), Some(table)) = (
                        json.get("table_schema").and_then(|v| v.as_str()),
                        json.get("table_name").and_then(|v| v.as_str()),
                    ) {
                        result.push((schema.to_string(), table.to_string()));
                    }
                }
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn parse_columns(output: &str) -> Option<Vec<(String, String, String, String)>> {
        let mut result = Vec::new();

        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse message-based format
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Check for DATA message type
                if let Some(msg_type) = json.get("message_type").and_then(|v| v.as_str()) {
                    if msg_type == "DATA" {
                        // Extract data array: [["public","test","id","integer"],...]
                        if let Some(data) = json.get("data").and_then(|v| v.as_array()) {
                            for row in data {
                                if let Some(row_array) = row.as_array() {
                                    if row_array.len() >= 4 {
                                        let schema = row_array[0].as_str().unwrap_or("").to_string();
                                        let table = row_array[1].as_str().unwrap_or("").to_string();
                                        let column = row_array[2].as_str().unwrap_or("").to_string();
                                        let data_type = row_array[3].as_str().unwrap_or("").to_string();
                                        if !schema.is_empty() && !table.is_empty() && !column.is_empty() {
                                            result.push((schema, table, column, data_type));
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Try old JSONLines_Compact format for backwards compatibility
                    if let (Some(schema), Some(table), Some(column), Some(data_type)) = (
                        json.get("table_schema").and_then(|v| v.as_str()),
                        json.get("table_name").and_then(|v| v.as_str()),
                        json.get("column_name").and_then(|v| v.as_str()),
                        json.get("data_type").and_then(|v| v.as_str()),
                    ) {
                        result.push((
                            schema.to_string(),
                            table.to_string(),
                            column.to_string(),
                            data_type.to_string(),
                        ));
                    }
                }
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn parse_functions(output: &str) -> Option<Vec<String>> {
        let mut result = Vec::new();

        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse message-based format
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Check for DATA message type
                if let Some(msg_type) = json.get("message_type").and_then(|v| v.as_str()) {
                    if msg_type == "DATA" {
                        // Extract data array: [["function1"],["function2"],...]
                        if let Some(data) = json.get("data").and_then(|v| v.as_array()) {
                            for row in data {
                                if let Some(row_array) = row.as_array() {
                                    if !row_array.is_empty() {
                                        if let Some(routine_name) = row_array[0].as_str() {
                                            if !routine_name.is_empty() {
                                                result.push(routine_name.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Try old JSONLines_Compact format for backwards compatibility
                    if let Some(routine_name) = json.get("routine_name").and_then(|v| v.as_str()) {
                        result.push(routine_name.to_string());
                    }
                }
            }
        }

        // Always return Some - empty result is valid (no functions defined)
        // Only return None if output is completely empty/unparseable
        if output.trim().is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tables() {
        // Test new message-based format
        let output = r#"{"message_type":"START","result_columns":[{"name":"table_schema","type":"text"},{"name":"table_name","type":"text"}]}
{"message_type":"DATA","data":[["public","users"],["public","orders"]]}
{"message_type":"FINISH_SUCCESSFULLY","statistics":{}}"#;
        let tables = SchemaCache::parse_tables(output).unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0], ("public".to_string(), "users".to_string()));
        assert_eq!(tables[1], ("public".to_string(), "orders".to_string()));

        // Test old JSONLines_Compact format for backwards compatibility
        let output_old = r#"{"table_schema":"public","table_name":"legacy_table"}"#;
        let tables_old = SchemaCache::parse_tables(output_old).unwrap();
        assert_eq!(tables_old.len(), 1);
        assert_eq!(tables_old[0], ("public".to_string(), "legacy_table".to_string()));
    }

    #[test]
    fn test_parse_columns() {
        // Test new message-based format
        let output = r#"{"message_type":"START","result_columns":[]}
{"message_type":"DATA","data":[["public","users","id","INTEGER"],["public","users","name","TEXT"]]}
{"message_type":"FINISH_SUCCESSFULLY","statistics":{}}"#;
        let columns = SchemaCache::parse_columns(output).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(
            columns[0],
            (
                "public".to_string(),
                "users".to_string(),
                "id".to_string(),
                "INTEGER".to_string()
            )
        );
    }

    #[test]
    fn test_keywords_loaded() {
        let cache = SchemaCache::new(300);
        assert!(cache.keywords.contains("SELECT"));
        assert!(cache.keywords.contains("FROM"));
        assert!(cache.keywords.contains("WHERE"));
    }

    #[test]
    fn test_is_stale() {
        let cache = SchemaCache::new(1); // 1 second TTL
        assert!(cache.is_stale()); // Never refreshed

        cache.complete_refresh();
        assert!(!cache.is_stale());

        std::thread::sleep(Duration::from_millis(1100));
        assert!(cache.is_stale());
    }

    #[test]
    fn test_get_keywords() {
        let cache = SchemaCache::new(300);
        let keywords = cache.get_keywords("SEL");
        assert!(keywords.contains(&"SELECT".to_string()));
    }

    #[test]
    fn test_empty_cache() {
        let cache = SchemaCache::new(300);
        // Cache should have keywords even when empty
        assert!(cache.get_keywords("").len() > 0);
        // But no tables or columns yet
        assert_eq!(cache.get_tables("").len(), 0);
        assert_eq!(cache.get_columns("").len(), 0);
    }
}
