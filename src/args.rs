use gumdrop::Options;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::IsTerminal;

use crate::utils::{config_path, init_root_path};

// For String.or extension
pub trait Or: Sized {
    fn or(self, other: Self) -> Self;
}

impl Or for String {
    fn or(self, other: String) -> String {
        if self.is_empty() {
            other
        } else {
            self
        }
    }
}

// Default value functions for serde
fn default_min_col_width() -> usize {
    15
}

fn default_max_cell_length() -> usize {
    1000
}


#[derive(Clone, Debug, Options, Deserialize, Serialize)]
pub struct Args {
    #[options(help = "Run a single command and exit")]
    #[serde(skip_serializing, skip_deserializing)]
    pub command: String,

    #[options(short = "C", help = "Preset of settings to connect to Firebolt Core")]
    #[serde(skip_serializing, skip_deserializing)]
    pub core: bool,

    #[options(help = "Hostname (and port) to connect to", meta = "HOSTNAME")]
    #[serde(default)]
    pub host: String,

    #[options(help = "Database name to use")]
    #[serde(skip_serializing, skip_deserializing)]
    pub database: String,

    #[options(help = "Output format (client:auto, client:vertical, client:horizontal, PSQL, JSON, CSV, ...)")]
    #[serde(default)]
    pub format: String,

    #[options(help = "Extra settings in the form --extra <name>=<value>")]
    #[serde(default)]
    pub extra: Vec<String>,

    #[options(help = "Query label for tracking or identification")]
    #[serde(skip_serializing, skip_deserializing)]
    pub label: String,

    #[options(help = "JWT for authentication")]
    #[serde(skip_serializing, skip_deserializing)]
    pub jwt: String,

    #[options(no_short, help = "Service Account ID for OAuth authentication")]
    #[serde(skip_serializing, skip_deserializing)]
    pub sa_id: String,

    #[options(no_short, help = "Service Account Secret for OAuth authentication")]
    #[serde(skip_serializing, skip_deserializing)]
    pub sa_secret: String,

    #[options(no_short, help = "Load JWT from file (~/.firebolt/jwt)")]
    #[serde(default)]
    pub jwt_from_file: bool,


    #[options(
        no_short,
        help = "OAuth environment to use (e.g., 'app' or 'staging'). Used for Service Account authentication",
        default = "staging"
    )]
    #[serde(skip_serializing, skip_deserializing)]
    pub oauth_env: String,

    #[options(help = "Enable extra verbose output")]
    #[serde(default)]
    pub verbose: bool,

    #[options(no_short, help = "Suppress time statistics in output")]
    #[serde(default)]
    pub concise: bool,

    #[options(no_short, help = "Hide URLs that may contain PII in query parameters")]
    #[serde(default)]
    pub hide_pii: bool,

    #[options(no_short, help = "Disable the spinner in CLI output")]
    #[serde(default)]
    pub no_spinner: bool,

    #[options(no_short, help = "Minimum characters per column before switching to vertical mode", default = "15")]
    #[serde(default = "default_min_col_width")]
    pub min_col_width: usize,

    #[options(no_short, help = "Maximum cell content length before truncation", default = "1000")]
    #[serde(default = "default_max_cell_length")]
    pub max_cell_length: usize,


    #[options(no_short, help = "Update default configuration values")]
    #[serde(skip_serializing, skip_deserializing)]
    pub update_defaults: bool,

    #[options(help = "Print version")]
    #[serde(default)]
    pub version: bool,

    #[options(no_short, help = "Show help message and exit")]
    #[serde(skip_serializing, skip_deserializing)]
    pub help: bool,

    #[options(free, help = "Query command(s) to execute. If not specified, starts the REPL")]
    #[serde(skip_serializing, skip_deserializing)]
    pub query: Vec<String>,
}

impl Args {
    pub fn should_render_table(&self) -> bool {
        // Client rendering when format starts with "client:"
        self.format.starts_with("client:")
    }

    /// Extract display mode from client: prefix
    /// "client:auto" → "auto", "client:vertical" → "vertical", "PSQL" → ""
    pub fn get_display_mode(&self) -> &str {
        if self.format.starts_with("client:") {
            &self.format[7..]  // Skip "client:" prefix
        } else {
            ""
        }
    }

    pub fn is_vertical_display(&self) -> bool {
        self.get_display_mode().eq_ignore_ascii_case("vertical")
    }

    pub fn is_horizontal_display(&self) -> bool {
        self.get_display_mode().eq_ignore_ascii_case("horizontal")
    }

    pub fn is_auto_display(&self) -> bool {
        self.get_display_mode().eq_ignore_ascii_case("auto")
    }
}

pub fn normalize_extras(extras: Vec<String>, encode: bool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut x: BTreeMap<String, String> = BTreeMap::new();

    for e in &extras {
        let kv: Vec<&str> = e.split('=').collect();
        if kv.len() < 2 {
            return Err(format!("Cannot parse '{}': expected key=value format", e).into());
        }

        let key = kv[0].to_string();
        let value = kv[1..].join("=").trim().to_string();
        let value = if value.starts_with('\'') && value.ends_with('\'') || value.starts_with('"') && value.ends_with('"') {
            value[1..value.len() - 1].to_string()
        } else {
            value
        };

        let value = if encode { urlencoding::encode(&value).into_owned() } else { value };

        x.insert(key, value);
    }

    let mut new_extras: Vec<String> = vec![];
    for (key, value) in &x {
        new_extras.push(format!("{key}={value}"))
    }

    Ok(new_extras)
}

// Apply defaults and possibly update them.
#[allow(dead_code)]
pub fn get_args() -> Result<Args, Box<dyn std::error::Error>> {
    let config_path = config_path()?;

    let defaults: Args = if config_path.exists() {
        serde_yaml::from_str(&fs::read_to_string(&config_path)?)?
    } else {
        serde_yaml::from_str("")?
    };

    let mut args = Args::parse_args_default_or_exit();

    args.extra = normalize_extras(args.extra, true)?;

    args.jwt_from_file = args.jwt_from_file || defaults.jwt_from_file;
    if args.jwt_from_file {
        let jwt_path = init_root_path()?.join("jwt");
        if args.verbose {
            eprintln!("Loading JWT from {:?}", &jwt_path);
        }
        match fs::read_to_string(&jwt_path) {
            Ok(jwt) => args.jwt = String::from(jwt.trim()),
            Err(error) => eprintln!("Failed to read jwt from {:?}: {}", &jwt_path, error.to_string()),
        }
    }

    let default_host = if !args.jwt.is_empty() {
        String::from("localhost:9123")
    } else {
        String::from("localhost:8123")
    };

    if args.update_defaults {
        args.host = args.host.or(default_host);
        if args.core {
            args.format = args.format.or(String::from("PSQL"));
        } else {
            args.format = args.format.or(String::from("TabSeparatedWithNamesAndTypes"));
        }

        fs::write(&config_path, serde_yaml::to_string(&args)?)?;
        return Ok(args);
    }

    args.verbose = args.verbose || defaults.verbose;
    args.concise = args.concise || defaults.concise;
    args.hide_pii = args.hide_pii || defaults.hide_pii;

    // Use defaults for numeric settings if not specified
    if args.min_col_width == default_min_col_width() {
        args.min_col_width = defaults.min_col_width;
    }
    if args.max_cell_length == default_max_cell_length() {
        args.max_cell_length = defaults.max_cell_length;
    }

    args.database = args
        .database
        .or(args.core.then(|| String::from("firebolt")).unwrap_or(defaults.database))
        .or(String::from("local_dev_db"));

    // Detect if running in interactive mode
    let is_interactive = std::io::stdout().is_terminal() && std::io::stdin().is_terminal();

    if args.core {
        args.host = args.host.or(String::from("localhost:3473"));
        args.jwt = String::from("");
        args.format = args.format.or(String::from("PSQL"));
    } else {
        // Apply smart defaults based on mode if format is not already set
        let default_format = if args.format.is_empty() && defaults.format.is_empty() {
            if is_interactive {
                // Interactive mode: default to client-side rendering with auto display
                String::from("client:auto")
            } else {
                // Non-interactive mode: default to server-side rendering with PSQL
                String::from("PSQL")
            }
        } else {
            String::new()
        };

        args.format = args.format.or(defaults.format).or(default_format);
        args.host = args.host.or(defaults.host).or(default_host);
    }

    if !args.extra.is_empty() {
        let mut extras = normalize_extras(defaults.extra, true)?;
        extras.append(&mut args.extra);
        args.extra = normalize_extras(extras, false)?;
    }

    // Warn if user specified a client format name without the "client:" prefix
    if args.format.eq_ignore_ascii_case("auto")
        || args.format.eq_ignore_ascii_case("vertical")
        || args.format.eq_ignore_ascii_case("horizontal") {
        eprintln!("Warning: Format '{}' is not supported by the server.", args.format);
        eprintln!("Did you mean '--format client:{}'?", args.format.to_lowercase());
        eprintln!("Client-side formats require the 'client:' prefix (e.g., client:auto, client:vertical, client:horizontal)");
        eprintln!();
    }

    Ok(args)
}

// Create URL from Args
pub fn get_url(args: &Args) -> String {
    let query_label = if !args.label.is_empty() && !args.extra.iter().any(|e| e.starts_with("query_label=")) {
        format!("&query_label={}", args.label)
    } else {
        String::new()
    };

    let database = if !args.database.is_empty() && !args.extra.iter().any(|e| e.starts_with("database=")) {
        format!("&database={}", args.database)
    } else {
        String::new()
    };

    let extra = if args.extra.is_empty() {
        String::new()
    } else {
        format!("&{}", args.extra.join("&"))
    };

    let is_localhost = args.host.starts_with("localhost");
    let protocol = if is_localhost { "http" } else { "https" };
    let output_format = if !args.format.is_empty() && !args.extra.iter().any(|e| e.starts_with("format=")) {
        if args.format.starts_with("client:") {
            // Client-side rendering: always use JSONLines_Compact
            format!("&output_format=JSONLines_Compact")
        } else {
            // Server-side rendering: use format as-is
            format!("&output_format={}", &args.format)
        }
    } else {
        String::new()
    };
    let advanced_mode = if is_localhost { "" } else { "&advanced_mode=1" };

    format!(
        "{protocol}://{host}/?{database}{query_label}{extra}{output_format}{advanced_mode}",
        host = args.host
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_extras() {
        let extras = vec!["param1=value1".to_string(), "param2=value with spaces".to_string()];

        // Test without encoding
        let result = normalize_extras(extras.clone(), false).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"param1=value1".to_string()));
        assert!(result.contains(&"param2=value with spaces".to_string()));

        // Test with encoding
        let result = normalize_extras(extras, true).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"param1=value1".to_string()));
        assert!(result.contains(&"param2=value%20with%20spaces".to_string()));
    }

    #[test]
    fn test_url_generation() {
        let mut args = Args::parse_args_default_or_exit();
        args.host = "localhost:8123".to_string();
        args.database = "test_db".to_string();
        args.core = true;
        args.format = "PSQL".to_string();

        let url = get_url(&args);

        assert!(url.starts_with("http://localhost:8123"));
        assert!(url.contains("database=test_db"));
        assert!(url.contains("output_format=PSQL"));
    }

    #[test]
    fn test_params_encoded_only_once() {
        // Test that parameters with special characters are encoded correctly,
        // but already encoded parameters aren't double-encoded

        // Case 1: Regular parameter with special chars
        let extras = vec!["param=value with spaces".to_string()];
        let result = normalize_extras(extras, true).unwrap();
        assert_eq!(result[0], "param=value%20with%20spaces");

        // Case 2: Already encoded parameter
        let extras = vec!["param=already%20encoded".to_string()];
        let result1 = normalize_extras(extras, true).unwrap();
        let result2 = normalize_extras(result1.clone(), true).unwrap();

        // The parameter should be encoded only once
        assert_eq!(result1[0], "param=already%2520encoded"); // %20 became %2520
        assert_eq!(result2[0], "param=already%252520encoded"); // Double-encoding changes %2520 to %252520

        // Case 3: Parameters with various special characters
        let extras = vec![
            "param1=value+with+plus".to_string(),
            "param2=value/with/slash".to_string(),
            "param3=value?with&query".to_string(),
        ];

        let result = normalize_extras(extras, true).unwrap();
        assert!(result.contains(&"param1=value%2Bwith%2Bplus".to_string())); // + becomes %2B
        assert!(result.contains(&"param2=value%2Fwith%2Fslash".to_string())); // / becomes %2F
        assert!(result.contains(&"param3=value%3Fwith%26query".to_string())); // ? becomes %3F, & becomes %26

        // Verify the encoding in URL
        let mut args = Args::parse_args_default_or_exit();
        args.host = "localhost:8123".to_string();
        args.extra = normalize_extras(vec!["param=value with spaces".to_string()], true).unwrap();

        let url = get_url(&args);
        assert!(url.contains("param=value%20with%20spaces"));
        assert!(!url.contains("param=value%2520with%2520spaces")); // No double encoding
    }

    #[test]
    fn test_params_with_quotes() {
        let extras = vec!["param1='value with spaces'".to_string(), "param2=\"value with spaces\"".to_string()];
        let result = normalize_extras(extras, true).unwrap();
        assert_eq!(result[0], "param1=value%20with%20spaces");
        assert_eq!(result[1], "param2=value%20with%20spaces");
    }

    #[test]
    fn test_params_with_spaces() {
        let extras = vec![
            "param1=   value with spaces  ".to_string(),
            "param2=  \"value with spaces\"  ".to_string(),
            "param3=\"  value with spaces \"".to_string(),
        ];
        let result = normalize_extras(extras, true).unwrap();
        assert_eq!(result[0], "param1=value%20with%20spaces");
        assert_eq!(result[1], "param2=value%20with%20spaces");
        assert_eq!(result[2], "param3=%20%20value%20with%20spaces%20");
    }

    #[test]
    fn test_should_render_table_with_client_prefix() {
        let mut args = Args::parse_args_default_or_exit();

        // Server-side format: should not render
        args.format = String::from("PSQL");
        assert!(!args.should_render_table());

        args.format = String::from("JSON");
        assert!(!args.should_render_table());

        // Client-side format: should render
        args.format = String::from("client:auto");
        assert!(args.should_render_table());

        args.format = String::from("client:vertical");
        assert!(args.should_render_table());

        args.format = String::from("client:horizontal");
        assert!(args.should_render_table());
    }

    #[test]
    fn test_get_display_mode() {
        let mut args = Args::parse_args_default_or_exit();

        // Client formats
        args.format = String::from("client:auto");
        assert_eq!(args.get_display_mode(), "auto");

        args.format = String::from("client:vertical");
        assert_eq!(args.get_display_mode(), "vertical");

        args.format = String::from("client:horizontal");
        assert_eq!(args.get_display_mode(), "horizontal");

        // Server formats
        args.format = String::from("PSQL");
        assert_eq!(args.get_display_mode(), "");

        args.format = String::from("JSON");
        assert_eq!(args.get_display_mode(), "");
    }

    #[test]
    fn test_display_mode_helpers() {
        let mut args = Args::parse_args_default_or_exit();

        args.format = String::from("client:auto");
        assert!(args.is_auto_display());
        assert!(!args.is_vertical_display());
        assert!(!args.is_horizontal_display());

        args.format = String::from("client:vertical");
        assert!(!args.is_auto_display());
        assert!(args.is_vertical_display());
        assert!(!args.is_horizontal_display());

        args.format = String::from("client:horizontal");
        assert!(!args.is_auto_display());
        assert!(!args.is_vertical_display());
        assert!(args.is_horizontal_display());

        args.format = String::from("PSQL");
        assert!(!args.is_auto_display());
        assert!(!args.is_vertical_display());
        assert!(!args.is_horizontal_display());
    }

    #[test]
    fn test_format_without_client_prefix() {
        // Test that formats "auto", "vertical", "horizontal" without "client:" prefix
        // are recognized (they will trigger a warning at runtime, but are valid format strings)
        let mut args = Args::parse_args_default_or_exit();

        args.format = String::from("auto");
        assert!(!args.should_render_table()); // Should NOT render because no "client:" prefix
        assert_eq!(args.get_display_mode(), ""); // Empty because no prefix

        args.format = String::from("vertical");
        assert!(!args.should_render_table());
        assert_eq!(args.get_display_mode(), "");

        args.format = String::from("horizontal");
        assert!(!args.should_render_table());
        assert_eq!(args.get_display_mode(), "");
    }
}
