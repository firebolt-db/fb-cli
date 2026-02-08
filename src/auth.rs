use serde::Deserialize;
use std::fs;
use std::time::SystemTime;
use tokio::task;
use tokio_util::sync::CancellationToken;

use crate::context::{Context, SavedCredentials, ServiceAccountToken};
use crate::utils::{credentials_path, format_remaining_time, sa_token_path, spin};
use std::io::{self, Write};

/// Helper to create an authenticated context from saved credentials
async fn create_context_from_credentials(
    host: String,
    database: String,
    format: String,
    no_spinner: bool,
) -> Result<Context, Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Err("No saved credentials found. Run 'fb auth' first.".into());
    }

    let saved_creds: SavedCredentials = serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    let temp_args = crate::args::Args {
        command: String::new(),
        core: false,
        host,
        database,
        format,
        extra: vec![],
        label: String::new(),
        jwt: String::new(),
        sa_id: saved_creds.sa_id,
        sa_secret: saved_creds.sa_secret,
        account_name: saved_creds.account_name,
        jwt_from_file: false,
        oauth_env: saved_creds.oauth_env,
        verbose: false,
        concise: true,
        hide_pii: false,
        no_spinner,
        update_defaults: false,
        version: false,
        help: false,
        query: vec![],
    };

    let mut context = Context::new(temp_args);
    context.update_url();
    authenticate_service_account(&mut context).await?;

    Ok(context)
}

pub async fn authenticate_service_account(context: &mut Context) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(sa_token) = &context.sa_token {
        let valid_until = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(sa_token.until);
        if sa_token.sa_id == context.args.sa_id &&
           sa_token.sa_secret == context.args.sa_secret &&
           sa_token.oauth_env == context.args.oauth_env &&
           valid_until > SystemTime::now() {
            return Ok(());
        }
    }

    let args = &mut context.args;
    if args.sa_id.is_empty() {
        return Err("Missing Service Account ID (--sa-id)".into());
    }

    if args.sa_secret.is_empty() {
        return Err(format!("Missing Service Account Secret (--sa-secret)").into());
    }

    if args.oauth_env != "staging" && args.oauth_env != "app" {
        return Err(format!("OAuth Env = {:?}, which is not \"staging\" or \"app\"", args.oauth_env).into());
    }

    let auth_url = format!("https://id.{}.firebolt.io/oauth/token", args.oauth_env);
    if args.verbose {
        eprintln!("Getting auth token for SA ID {:?} from {:?}...", args.sa_id, auth_url);
    }

    let sa_token_path = sa_token_path()?;
    if sa_token_path.exists() {
        if let Some(sa_token) = serde_yaml::from_str::<Option<ServiceAccountToken>>(&fs::read_to_string(&sa_token_path)?)? {
            let valid_until = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(sa_token.until);
            if sa_token.sa_id == args.sa_id &&
               sa_token.sa_secret == args.sa_secret &&
               sa_token.oauth_env == args.oauth_env &&
               valid_until > SystemTime::now() {
                if args.verbose {
                    eprintln!(
                        "Using cached SA token from {:?}, valid for {:}",
                        sa_token_path,
                        format_remaining_time(valid_until, "more".into())?
                    );
                }

                args.jwt.clear();
                context.sa_token = Some(sa_token.clone());

                return Ok(());
            }
        }
    }

    let mut params = std::collections::HashMap::new();
    params.insert("grant_type", "client_credentials");
    params.insert("audience", "https://api.firebolt.io");
    params.insert("client_id", &args.sa_id);
    params.insert("client_secret", &args.sa_secret);
    let async_req = reqwest::Client::new()
        .post(auth_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&params);

    if args.verbose {
        eprintln!("OAuth request: {:?}", async_req);
    }

    let valid_until: SystemTime = SystemTime::now() + std::time::Duration::new(1800, 0);
    let async_resp = async_req.send();

    let token = CancellationToken::new();
    let maybe_spin = if args.no_spinner || args.concise {
        None
    } else {
        let token_clone = token.clone();
        Some(task::spawn(async {
            spin(token_clone).await;
        }))
    };

    let response = async_resp.await;
    token.cancel();
    if let Some(spin) = maybe_spin {
        spin.await?;
    }

    match response {
        Ok(resp) => {
            #[derive(Deserialize)]
            struct AuthResponse<'a> {
                access_token: Option<&'a str>,
            }
            let resp_text = resp.text().await?;
            if args.verbose {
                eprintln!("OAuth response: {:?}", resp_text);
            }

            let response: AuthResponse = serde_json::from_str(&resp_text)?;
            if !response.access_token.is_some() {
                return Err(format!("Failed to authenticate: '{:}'", resp_text).into());
            }

            let sa_token = ServiceAccountToken {
                sa_id: args.sa_id.clone(),
                sa_secret: args.sa_secret.clone(),
                token: response.access_token.unwrap().to_string(),
                until: valid_until.duration_since(SystemTime::UNIX_EPOCH)?.as_secs(),
                oauth_env: args.oauth_env.clone(),
            };

            args.jwt.clear();
            context.sa_token = Some(sa_token.clone());

            fs::write(&sa_token_path, serde_yaml::to_string(&sa_token)?)?;
            if args.verbose {
                eprintln!(
                    "SA token saved to {:?}, valid for {:}",
                    sa_token_path,
                    format_remaining_time(valid_until, "".into())?
                );
            }
        }
        Err(error) => {
            if context.args.verbose {
                return Err(format!("Failed to authenticate: {:?}", error).into());
            }

            return Err(format!("Failed to authenticate: {}", error.to_string()).into());
        }
    };

    return Ok(());
}

pub async fn maybe_authenticate(context: &mut Context) -> Result<(), Box<dyn std::error::Error>> {
    let args = &mut context.args;
    if !args.sa_id.is_empty() || !args.sa_secret.is_empty() {
        return authenticate_service_account(context).await;
    }

    return Ok(());
}

/// Discover system engine URL from account name using the gateway API
async fn discover_system_engine_url(
    account_name: &str,
    access_token: &str,
    api_endpoint: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let gateway_url = format!(
        "https://{}/web/v3/account/{}/engineUrl",
        api_endpoint, account_name
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&gateway_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await?;
        return Err(format!(
            "Failed to discover system engine URL (status {}): {}",
            status, text
        )
        .into());
    }

    #[derive(Deserialize)]
    struct EngineUrlResponse {
        #[serde(rename = "engineUrl")]
        engine_url: String,
    }

    let response_data: EngineUrlResponse = response.json().await?;
    Ok(response_data.engine_url)
}

/// Interactive prompt for authentication setup
pub async fn interactive_auth_setup() -> Result<(), Box<dyn std::error::Error>> {
    println!("Welcome to Firebolt CLI authentication setup!\n");

    print!("Enter Service Account ID: ");
    io::stdout().flush()?;
    let mut sa_id = String::new();
    io::stdin().read_line(&mut sa_id)?;
    let sa_id = sa_id.trim().to_string();

    // Use dialoguer for better password input with visual feedback
    use console::Term;
    let term = Term::stderr();

    eprint!("Enter Service Account Secret: ");
    io::stderr().flush()?;

    let mut sa_secret = String::new();
    loop {
        if let Ok(key) = term.read_key() {
            match key {
                console::Key::Enter => {
                    eprintln!();
                    break;
                }
                console::Key::Backspace => {
                    if !sa_secret.is_empty() {
                        sa_secret.pop();
                        eprint!("\x08 \x08");
                        io::stderr().flush()?;
                    }
                }
                console::Key::Char(c) if !c.is_control() => {
                    sa_secret.push(c);
                    eprint!("*");
                    io::stderr().flush()?;
                }
                _ => {}
            }
        }
    }
    let sa_secret = sa_secret.trim().to_string();

    print!("Enter account name (e.g., my_account): ");
    io::stdout().flush()?;
    let mut account_name = String::new();
    io::stdin().read_line(&mut account_name)?;
    let account_name = account_name.trim().to_string();

    // Always use "app" environment (production)
    let oauth_env = "app";
    let api_endpoint = "api.app.firebolt.io";

    // Step 1: Authenticate to get access token
    println!("\nAuthenticating...");

    // Create minimal args for authentication
    let temp_args = crate::args::Args {
        command: String::new(),
        core: false,
        host: api_endpoint.to_string(), // Temporarily use API endpoint
        database: String::new(),
        format: String::new(),
        extra: vec![],
        label: String::new(),
        jwt: String::new(),
        sa_id: sa_id.clone(),
        sa_secret: sa_secret.clone(),
        account_name: account_name.clone(),
        jwt_from_file: false,
        oauth_env: oauth_env.to_string(),
        verbose: false,
        concise: false,
        hide_pii: false,
        no_spinner: true,
        update_defaults: false,
        version: false,
        help: false,
        query: vec![],
    };

    let mut temp_context = crate::context::Context::new(temp_args);
    authenticate_service_account(&mut temp_context).await?;

    let access_token = if let Some(token) = &temp_context.sa_token {
        token.token.clone()
    } else {
        return Err("Failed to obtain access token".into());
    };

    println!("✓ Authentication successful!");

    // Step 2: Discover system engine URL
    println!("Discovering system engine endpoint...");
    let system_engine_url = discover_system_engine_url(&account_name, &access_token, api_endpoint).await?;
    println!("✓ System engine URL: {}", system_engine_url);

    // Update context with system engine URL
    temp_context.args.host = system_engine_url.clone();
    temp_context.update_url();

    // Step 3: Optional database and engine configuration
    println!("\n(Optional) Configure defaults:");

    print!("Default database name [press Enter to skip]: ");
    io::stdout().flush()?;
    let mut database_input = String::new();
    io::stdin().read_line(&mut database_input)?;
    let database_name = database_input.trim();

    let final_database = if !database_name.is_empty() {
        temp_context.args.database = database_name.to_string();
        temp_context.update_url();
        Some(database_name.to_string())
    } else {
        None
    };

    print!("Default engine name [press Enter to skip]: ");
    io::stdout().flush()?;
    let mut engine_input = String::new();
    io::stdin().read_line(&mut engine_input)?;
    let engine_name = engine_input.trim();

    let final_host = if !engine_name.is_empty() {
        println!("Resolving engine '{}' endpoint...", engine_name);

        // Run USE ENGINE to get the engine endpoint via Firebolt-Update-Endpoint header
        let use_engine_query = format!("USE ENGINE {}", engine_name);
        match crate::query::query(&mut temp_context, use_engine_query).await {
            Ok(_) => {
                println!("✓ Engine '{}' configured: {}", engine_name, temp_context.args.host);
                Some(temp_context.args.host.clone())
            }
            Err(e) => {
                eprintln!("⚠ Warning: Failed to resolve engine '{}': {}", engine_name, e);
                eprintln!("  Continuing with system engine endpoint.");
                Some(system_engine_url)
            }
        }
    } else {
        Some(system_engine_url)
    };

    let saved_creds = SavedCredentials {
        sa_id,
        sa_secret,
        oauth_env: oauth_env.to_string(),
        account_name,
        host: final_host,
        database: final_database,
    };

    let creds_path = credentials_path()?;
    fs::write(&creds_path, serde_yaml::to_string(&saved_creds)?)?;

    println!("\nCredentials saved to {:?}", creds_path);
    println!("\n✓ Setup complete! You can now run queries:");
    if saved_creds.database.is_some() {
        println!("  fb \"select 42\"");
        println!("  fb \"select * from my_table\"");
    } else {
        println!("  fb -d <database> \"select 42\"");
        println!("  fb -d <database> \"select * from my_table\"");
    }

    Ok(())
}

/// Helper function to execute a query and return the response text (doesn't print to stdout)
async fn execute_query_internal(
    context: &mut Context,
    query_text: String,
) -> Result<String, Box<dyn std::error::Error>> {
    use crate::{FIREBOLT_PROTOCOL_VERSION, USER_AGENT};

    let mut request = reqwest::Client::builder()
        .http2_keep_alive_timeout(std::time::Duration::from_secs(3600))
        .http2_keep_alive_interval(Some(std::time::Duration::from_secs(60)))
        .http2_keep_alive_while_idle(false)
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .build()?
        .post(context.url.clone())
        .header("user-agent", USER_AGENT)
        .header("Firebolt-Protocol-Version", FIREBOLT_PROTOCOL_VERSION)
        .body(query_text);

    if let Some(sa_token) = &context.sa_token {
        request = request.header("authorization", format!("Bearer {}", sa_token.token));
    }

    let response = request.send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await?;
        return Err(format!("Query failed (status {}): {}", status, text).into());
    }

    Ok(response.text().await?)
}

/// Set default database in saved credentials (with validation)
pub async fn set_default_database(database_name: String) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    let saved_creds: SavedCredentials = serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    // Get system engine host for querying information_schema
    let system_engine_host = if let Some(host) = &saved_creds.host {
        if let Some(pos) = host.find("?engine=") {
            host[..pos].to_string()
        } else {
            host.clone()
        }
    } else {
        return Err("No host configured. Run 'fb auth' to set up credentials.".into());
    };

    let mut temp_context = create_context_from_credentials(
        system_engine_host,
        String::new(),
        String::from("TabSeparatedWithNames"),
        true,
    )
    .await?;

    // Query information_schema.catalogs to check if database exists
    println!("Validating database '{}'...", database_name);

    let check_query = format!(
        "SELECT catalog_name FROM information_schema.catalogs WHERE catalog_name = '{}'",
        database_name.replace("'", "''") // Escape single quotes
    );

    match execute_query_internal(&mut temp_context, check_query).await {
        Ok(response) => {
            // Check if response contains the database name (simple check - response will have header + data row if exists)
            if response.contains(&database_name) {
                // Database exists! Save it to credentials
                let mut updated_creds = saved_creds;
                updated_creds.database = Some(database_name.clone());

                fs::write(&creds_path, serde_yaml::to_string(&updated_creds)?)?;
                println!("✓ Default database set to: {}", database_name);
                Ok(())
            } else {
                eprintln!("Database '{}' does not exist.", database_name);
                eprintln!("Run 'fb show databases' to see available databases.");
                Err("Database validation failed".into())
            }
        }
        Err(e) => {
            Err(format!("Failed to validate database: {}", e).into())
        }
    }
}

/// Set default engine in saved credentials by running USE ENGINE and capturing endpoint
pub async fn set_default_engine(engine_name: String) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Err("No saved credentials found. Run 'fb auth' first.".into());
    }

    let mut saved_creds: SavedCredentials = serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    // Need to get the system engine URL first
    let system_engine_host = if let Some(host) = &saved_creds.host {
        // If the host contains ?engine=, strip it to get system engine
        if let Some(pos) = host.find("?engine=") {
            host[..pos].to_string()
        } else {
            host.clone()
        }
    } else {
        // No host saved, need to discover it
        println!("Discovering system engine endpoint...");

        let api_endpoint = "api.app.firebolt.io";
        let temp_context = create_context_from_credentials(
            api_endpoint.to_string(),
            String::new(),
            String::new(),
            true,
        )
        .await?;

        let access_token = if let Some(token) = &temp_context.sa_token {
            token.token.clone()
        } else {
            return Err("Failed to obtain access token".into());
        };

        let system_url =
            discover_system_engine_url(&saved_creds.account_name, &access_token, api_endpoint)
                .await?;
        println!("✓ System engine: {}", system_url);
        system_url
    };

    // Create context with system engine to run USE ENGINE
    let mut temp_context = create_context_from_credentials(
        system_engine_host.clone(),
        saved_creds.database.clone().unwrap_or_default(),
        String::new(),
        true,
    )
    .await?;

    // First, validate that the engine exists by querying information_schema.engines
    println!("Validating engine '{}'...", engine_name);
    let check_query = format!(
        "SELECT engine_name FROM information_schema.engines WHERE engine_name = '{}'",
        engine_name.replace("'", "''") // Escape single quotes
    );

    match execute_query_internal(&mut temp_context, check_query).await {
        Ok(response) => {
            // Check if response contains the engine name
            if !response.contains(&engine_name) {
                eprintln!("Engine '{}' does not exist.", engine_name);
                eprintln!("Run 'fb show engines' to see available engines.");
                return Err("Engine validation failed".into());
            }
        }
        Err(e) => {
            return Err(format!("Failed to validate engine: {}", e).into());
        }
    }

    // Engine exists! Now run USE ENGINE to get the endpoint
    println!("Resolving engine '{}' endpoint...", engine_name);
    let use_engine_query = format!("USE ENGINE {}", engine_name);
    crate::query::query(&mut temp_context, use_engine_query).await?;

    // The query function will have updated temp_context.args.host with the engine endpoint
    saved_creds.host = Some(temp_context.args.host.clone());

    fs::write(&creds_path, serde_yaml::to_string(&saved_creds)?)?;
    println!("✓ Default engine set to: {} ({})", engine_name, temp_context.args.host);

    Ok(())
}

/// Display current authentication status
pub fn show_auth_status() -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        println!("No saved credentials found.");
        println!("Run 'fb auth' to set up authentication.");
        return Ok(());
    }

    let saved_creds: SavedCredentials = serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    println!("Authenticated as: Service Account");
    println!("  ID: {}", saved_creds.sa_id);
    println!("  Account: {}", saved_creds.account_name);
    println!("  Environment: {}", saved_creds.oauth_env);

    // Check if token is cached and valid
    if let Ok(sa_token_path) = sa_token_path() {
        if sa_token_path.exists() {
            if let Ok(content) = fs::read_to_string(&sa_token_path) {
                if let Ok(Some(token)) =
                    serde_yaml::from_str::<Option<ServiceAccountToken>>(&content)
                {
                    let valid_until = SystemTime::UNIX_EPOCH
                        + std::time::Duration::from_secs(token.until);
                    if valid_until > SystemTime::now() {
                        println!(
                            "  Token valid for: {}",
                            format_remaining_time(valid_until, "".into())?
                        );
                    } else {
                        println!("  Token: expired");
                    }
                }
            }
        }
    }

    if let Some(host) = &saved_creds.host {
        println!("  System engine: {}", host);
    }
    if let Some(database) = &saved_creds.database {
        println!("  Default database: {}", database);
    }

    Ok(())
}

/// Clear saved credentials
pub fn clear_auth() -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if creds_path.exists() {
        fs::remove_file(&creds_path)?;
        println!("Credentials cleared from {:?}", creds_path);
    } else {
        println!("No saved credentials to clear.");
    }
    Ok(())
}

/// Load saved credentials into Args
pub fn load_saved_credentials(
    args: &mut crate::args::Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Ok(()); // No saved credentials
    }

    let saved_creds: SavedCredentials = serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    // Only apply if not explicitly provided on command line
    if args.sa_id.is_empty() {
        args.sa_id = saved_creds.sa_id;
    }
    if args.sa_secret.is_empty() {
        args.sa_secret = saved_creds.sa_secret;
    }
    // Always use the saved oauth_env
    args.oauth_env = saved_creds.oauth_env;

    // Apply default host/database if saved
    if args.host.is_empty() {
        if let Some(host) = saved_creds.host {
            args.host = host;
        }
    }
    if args.database.is_empty() {
        if let Some(database) = saved_creds.database {
            args.database = database;
        }
    }

    if args.verbose {
        eprintln!("Loaded credentials from {:?}", creds_path);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Add tests for authentication functionality when possible
}
