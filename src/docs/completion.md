# SQL Auto-Completion

Auto-completion is implemented as a pure in-memory system: no external process, no LSP. It uses a schema cache populated at session start and a priority scorer to surface the most relevant suggestion first.

## Components

| File | Struct | Role |
|------|--------|------|
| `completion/mod.rs` | `SqlCompleter` | Orchestrates completion; called by the REPL |
| `completion/schema_cache.rs` | `SchemaCache` | Caches DB metadata; refreshed async |
| `completion/priority_scorer.rs` | `PriorityScorer` | Scores suggestions by class + usage |
| `completion/usage_tracker.rs` | `UsageTracker` | Counts how often each item has been used |
| `completion/context_analyzer.rs` | `ContextAnalyzer` | Extracts table names from the current query |
| `completion/context_detector.rs` | `ContextDetector` | Classifies the cursor position (FROM, SELECT, etc.) |

## Request flow

```
tui-textarea Tab keypress
        │
        ▼
SqlCompleter::complete_at(line, cursor_byte)
        │
        ├── ContextDetector::detect()        → CompletionContext
        ├── ContextAnalyzer::extract_tables() → Vec<String> (tables in query)
        ├── SchemaCache::get_*()             → candidate lists
        └── PriorityScorer::score()          → ranked Vec<String>
```

The completer returns `(word_start_byte, Vec<String>)`. The TUI replaces `line[word_start..]` up to the cursor with the selected candidate.

## SchemaCache (`completion/schema_cache.rs`)

Thread-safe (`Arc<RwLock<...>>`) cache of:
- Tables: `HashMap<String, TableMetadata>` keyed by `"schema.table"`
- Columns: per table, a `Vec<ColumnMetadata>`
- Functions: `Vec<String>`
- SQL keywords: a hardcoded list of ~60 SQL keywords

### Refresh

`refresh()` is an async method that executes three queries against the connected database:

```sql
SELECT table_schema, table_name FROM information_schema.tables;
SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns;
SELECT routine_name FROM information_schema.routines WHERE routine_type != 'OPERATOR';
```

Responses are parsed from `JSONLines_Compact` format. The refresh is debounced: a `refreshing` atomic flag prevents concurrent refreshes.

On session start, `SchemaCache::refresh()` is called in a background `tokio::spawn` task so startup is non-blocking.

**TTL**: configurable; defaults to 5 minutes. After the TTL expires, `is_stale()` returns true and `\refresh` or the next session start triggers a new fetch. Use `\refresh` in the REPL to force an immediate refresh.

### Lookup methods

| Method | Returns |
|--------|---------|
| `get_tables(prefix)` | Table names matching prefix |
| `get_tables_in_schema(schema, prefix)` | Tables in a specific schema |
| `get_tables_with_schema(prefix)` | `(schema, table)` tuples |
| `get_columns(table, prefix)` | Column names for a table |
| `get_columns_with_table(prefix)` | `(table, column)` tuples |
| `get_schemas(prefix)` | Schema names |
| `get_functions(prefix)` | Function names |
| `get_keywords(prefix)` | SQL keywords |

All methods take a `prefix: &str` and return only items that start with that prefix (case-insensitive).

## SqlCompleter (`completion/mod.rs`)

`complete_at(line, cursor_byte)` is the single entry point:

1. Extract the partial word before the cursor.
2. Detect completion context (FROM, SELECT, WHERE, etc.) using `ContextDetector`.
3. If the partial word contains `.`, handle schema-qualified or table-qualified completion:
   - `schema.` → complete table names in that schema
   - `table.` → complete column names in that table
4. Otherwise, generate candidates from all applicable categories (schemas, tables, columns from in-scope tables, functions, keywords).
5. Score every candidate with `PriorityScorer::score()`.
6. Sort descending by score, deduplicate, return.

### Disabling completion

`SqlCompleter::set_enabled(false)` makes `complete_at()` return an empty list immediately. Controlled at runtime with `set completion = off;`.

## PriorityScorer (`completion/priority_scorer.rs`)

Assigns a numeric score to each candidate. Higher is better. Score = **priority class** + **usage bonus**.

| Priority class | Score | Applied to |
|----------------|-------|------------|
| 5 | 5 000 | Columns from tables already in the current query (context columns) |
| 4 | 4 000 | Schemas |
| 3 | 3 000 | Tables |
| 2 | 2 000 | Functions |
| 1 | 1 000 | Keywords |
| 0 | 0 | System schemas / system tables |

Usage bonus: each item used in the last 10 queries adds `100 × frequency` to its score (capped so it can't surpass the next priority class).

System objects (`information_schema`, `pg_catalog`, `pg_*` tables) are always assigned priority class 0, keeping them at the bottom of any suggestion list.

## UsageTracker (`completion/usage_tracker.rs`)

Tracks how many times each table or column has appeared in recently-executed queries. Stored as a sliding window of the last N queries (default: 10). Usage counts decay naturally as old queries fall out of the window.

`track_query(sql)` is called after each successful query execution (in `query.rs`). It extracts identifiers from the SQL string and increments their counts.

`get_usage(item) -> u32` returns the count, used by `PriorityScorer`.

## ContextAnalyzer and ContextDetector

`ContextAnalyzer::extract_tables(sql)` scans the SQL text for table references (FROM clause, JOIN clauses) using regex patterns. Returns a `Vec<String>` of table names visible in the current statement. These are passed to `PriorityScorer` to identify context columns (priority class 5).

`ContextDetector::detect(sql, cursor_pos)` determines which SQL clause the cursor sits in. The result (`CompletionContext`) influences which candidate categories are included:

| Context | Categories offered |
|---------|--------------------|
| `FromClause` | Tables, schemas |
| `SelectClause` | Columns (context-aware), functions, keywords |
| `WhereClause` | Columns, functions, keywords |
| `JoinClause` | Tables, schemas |
| `Unknown` | All categories |

## Adding new completion sources

1. Add a `get_*()` method to `SchemaCache` returning candidates for the new category.
2. Add a priority class constant to `PriorityScorer` if the category needs a distinct ranking tier.
3. Call the new method from `SqlCompleter::complete_at()` and pass the results through `PriorityScorer::score()`.
4. If the source requires new schema queries, add them to `SchemaCache::refresh()`.
