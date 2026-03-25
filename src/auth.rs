use base64::{engine::general_purpose, Engine as _};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::time::SystemTime;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::task;
use tokio_util::sync::CancellationToken;

use crate::context::{AuthMethod, CachedToken, Context, SavedCredentials};
use crate::utils::{credentials_path, format_remaining_time, secrets_path, spin};
use std::io::{self, Write};

const KEYRING_SERVICE: &str = "fb-cli";
/// TODO: Replace with the real Firebolt browser OAuth client ID before shipping.
const BROWSER_CLIENT_ID: &str = "REPLACE_WITH_CLIENT_ID";

// ─── Cached token JSON (stored in keyring / secrets file) ────────────────────

#[derive(Serialize, Deserialize)]
struct CachedTokenJson {
    token: String,
    until: u64,
}

// ─── OIDC discovery ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct OidcConfig {
    authorization_endpoint: String,
    token_endpoint: String,
}

async fn discover_oidc_config(oauth_env: &str) -> Result<OidcConfig, Box<dyn std::error::Error>> {
    let url = format!(
        "https://id.{}.firebolt.io/.well-known/openid-configuration",
        oauth_env
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("OIDC discovery failed ({}): {}", resp.status(), resp.text().await?).into());
    }
    Ok(resp.json::<OidcConfig>().await?)
}

// ─── Keyring / file-based secret storage ─────────────────────────────────────

fn secrets_file_read() -> HashMap<String, String> {
    if let Ok(path) = secrets_path() {
        if let Ok(content) = fs::read_to_string(&path) {
            if let Ok(map) = serde_yaml::from_str::<HashMap<String, String>>(&content) {
                return map;
            }
        }
    }
    HashMap::new()
}

fn store_secret_in_file(key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut map = secrets_file_read();
    map.insert(key.to_string(), value.to_string());
    fs::write(secrets_path()?, serde_yaml::to_string(&map)?)?;
    Ok(())
}

fn load_secret_from_file(key: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    Ok(secrets_file_read().get(key).cloned())
}

fn delete_secret_from_file(key: &str) {
    let mut map = secrets_file_read();
    map.remove(key);
    if let Ok(path) = secrets_path() {
        let _ = fs::write(path, serde_yaml::to_string(&map).unwrap_or_default());
    }
}

fn keyring_store(key: &str, value: &str, no_keyring: bool) -> Result<(), Box<dyn std::error::Error>> {
    if no_keyring {
        return store_secret_in_file(key, value);
    }
    keyring::Entry::new(KEYRING_SERVICE, key)?.set_password(value)?;
    Ok(())
}

fn keyring_load(key: &str, no_keyring: bool) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if no_keyring {
        return load_secret_from_file(key);
    }
    match keyring::Entry::new(KEYRING_SERVICE, key)?.get_password() {
        Ok(val) => Ok(Some(val)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn keyring_delete(key: &str, no_keyring: bool) {
    if no_keyring {
        delete_secret_from_file(key);
        return;
    }
    if let Ok(entry) = keyring::Entry::new(KEYRING_SERVICE, key) {
        let _ = entry.delete_credential();
    }
}

// ─── PKCE browser flow helpers ────────────────────────────────────────────────

async fn wait_for_callback(
    listener: tokio::net::TcpListener,
) -> Result<String, Box<dyn std::error::Error>> {
    let result = tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let (stream, _) = listener.accept().await?;
        let (reader_half, mut writer_half) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader_half);

        let mut first_line = String::new();
        reader.read_line(&mut first_line).await?;

        // Drain request headers
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line).await?;
            if n == 0 || line == "\r\n" || line == "\n" {
                break;
            }
        }

        let success_html = concat!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n",
            "<html><body><h1>Authentication successful!</h1>",
            "<p>You can close this window.</p></body></html>"
        );
        writer_half.write_all(success_html.as_bytes()).await?;
        let _ = writer_half.shutdown().await;

        // Parse "GET /callback?code=XXX HTTP/1.1"
        let code: Result<String, Box<dyn std::error::Error>> = first_line
            .split_whitespace()
            .nth(1)
            .and_then(|path| path.split('?').nth(1))
            .and_then(|query| {
                query
                    .split('&')
                    .find(|p| p.starts_with("code="))
                    .map(|p| p[5..].to_string())
            })
            .map(|c| urlencoding::decode(&c).map(|s| s.into_owned()).unwrap_or(c))
            .ok_or_else(|| "No authorization code in callback".into());

        code
    })
    .await;

    match result {
        Ok(inner) => inner,
        Err(_) => Err("Authentication timed out (5 minutes)".into()),
    }
}

async fn authenticate_browser(
    context: &mut Context,
    oidc_config: &OidcConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let no_keyring = context.args.no_keyring;

    // Generate PKCE code_verifier (64 URL-safe chars)
    let code_verifier: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(64)
        .map(char::from)
        .collect();

    // code_challenge = BASE64URL_NOPAD(SHA256(verifier))
    let hash = Sha256::digest(code_verifier.as_bytes());
    let code_challenge = general_purpose::URL_SAFE_NO_PAD.encode(hash);

    // Bind local callback server on a random port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://localhost:{}/callback", port);

    let auth_url = format!(
        "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&code_challenge={}&code_challenge_method=S256&audience=https%3A%2F%2Fapi.firebolt.io",
        oidc_config.authorization_endpoint,
        urlencoding::encode(BROWSER_CLIENT_ID),
        urlencoding::encode(&redirect_uri),
        urlencoding::encode("offline_access openid"),
        urlencoding::encode(&code_challenge),
    );

    println!("Opening browser for authentication...");
    println!("If the browser does not open, visit:\n  {}", auth_url);
    let _ = open::that(&auth_url);

    println!("Waiting for authentication callback...");
    let code = wait_for_callback(listener).await?;

    // Exchange authorization code for tokens
    let mut params = HashMap::new();
    params.insert("grant_type", "authorization_code");
    params.insert("client_id", BROWSER_CLIENT_ID);
    params.insert("code", code.as_str());
    params.insert("redirect_uri", redirect_uri.as_str());
    params.insert("code_verifier", code_verifier.as_str());

    let client = reqwest::Client::new();
    let resp = client
        .post(&oidc_config.token_endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&params)
        .send()
        .await?;

    if !resp.status().is_success() {
        let text = resp.text().await?;
        return Err(format!("Token exchange failed: {}", text).into());
    }

    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
        refresh_token: Option<String>,
        expires_in: Option<u64>,
    }

    let token_resp: TokenResponse = resp.json().await?;
    let expires_in = token_resp.expires_in.unwrap_or(1800);
    let until = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs()
        + expires_in;

    let cached = CachedTokenJson { token: token_resp.access_token.clone(), until };
    keyring_store("access_token", &serde_json::to_string(&cached)?, no_keyring)?;

    if let Some(refresh) = &token_resp.refresh_token {
        keyring_store("refresh_token", refresh, no_keyring)?;
    }

    context.auth_token = Some(CachedToken { token: token_resp.access_token, until });
    Ok(())
}

async fn refresh_browser_token(
    context: &mut Context,
    oidc_config: &OidcConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let no_keyring = context.args.no_keyring;

    let refresh_token = keyring_load("refresh_token", no_keyring)?
        .ok_or("No refresh token found. Please run 'fb auth' again.")?;

    let mut params = HashMap::new();
    params.insert("grant_type", "refresh_token");
    params.insert("client_id", BROWSER_CLIENT_ID);
    params.insert("refresh_token", refresh_token.as_str());

    let client = reqwest::Client::new();
    let resp = client
        .post(&oidc_config.token_endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&params)
        .send()
        .await?;

    if !resp.status().is_success() {
        let text = resp.text().await?;
        return Err(format!("Token refresh failed: {}", text).into());
    }

    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
        refresh_token: Option<String>,
        expires_in: Option<u64>,
    }

    let token_resp: TokenResponse = resp.json().await?;
    let expires_in = token_resp.expires_in.unwrap_or(1800);
    let until = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs()
        + expires_in;

    let cached = CachedTokenJson { token: token_resp.access_token.clone(), until };
    keyring_store("access_token", &serde_json::to_string(&cached)?, no_keyring)?;

    if let Some(new_refresh) = &token_resp.refresh_token {
        keyring_store("refresh_token", new_refresh, no_keyring)?;
    }

    context.auth_token = Some(CachedToken { token: token_resp.access_token, until });
    Ok(())
}

async fn authenticate_browser_from_keyring(
    context: &mut Context,
    oauth_env: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let no_keyring = context.args.no_keyring;

    // Try cached access token first
    if let Some(token_json) = keyring_load("access_token", no_keyring)? {
        if let Ok(cached) = serde_json::from_str::<CachedTokenJson>(&token_json) {
            let valid_until =
                SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(cached.until);
            // Use with 60-second buffer
            if valid_until > SystemTime::now() + std::time::Duration::from_secs(60) {
                context.auth_token =
                    Some(CachedToken { token: cached.token, until: cached.until });
                return Ok(());
            }
        }
    }

    // Token expired or missing — refresh
    let oidc = discover_oidc_config(oauth_env).await?;
    refresh_browser_token(context, &oidc).await
}

// ─── Service Account flow ─────────────────────────────────────────────────────

pub async fn authenticate_service_account(
    context: &mut Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let no_keyring = context.args.no_keyring;

    // Check in-memory token
    if let Some(token) = &context.auth_token {
        let valid_until =
            SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(token.until);
        if valid_until > SystemTime::now() + std::time::Duration::from_secs(60) {
            return Ok(());
        }
    }

    let args = &context.args;
    if args.sa_id.is_empty() {
        return Err("Missing Service Account ID (--sa-id)".into());
    }
    if args.sa_secret.is_empty() {
        return Err("Missing Service Account Secret (--sa-secret)".into());
    }
    if args.oauth_env != "staging" && args.oauth_env != "app" {
        return Err(
            format!("OAuth Env = {:?}, which is not \"staging\" or \"app\"", args.oauth_env)
                .into(),
        );
    }

    let sa_id = args.sa_id.clone();

    // Check keyring cache
    if let Some(token_json) = keyring_load("access_token", no_keyring)? {
        if let Ok(cached) = serde_json::from_str::<CachedTokenJson>(&token_json) {
            let valid_until =
                SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(cached.until);
            if valid_until > SystemTime::now() + std::time::Duration::from_secs(60) {
                if args.verbose {
                    eprintln!(
                        "Using cached access token from keyring, valid for {}",
                        format_remaining_time(valid_until, "more".into())?
                    );
                }
                context.auth_token =
                    Some(CachedToken { token: cached.token, until: cached.until });
                context.args.jwt.clear();
                return Ok(());
            }
        }
    }

    let auth_url = format!(
        "https://id.{}.firebolt.io/oauth/token",
        context.args.oauth_env
    );
    if context.args.verbose {
        eprintln!(
            "Getting auth token for SA ID {:?} from {:?}...",
            sa_id, auth_url
        );
    }

    let mut params = HashMap::new();
    params.insert("grant_type", "client_credentials");
    params.insert("audience", "https://api.firebolt.io");
    params.insert("client_id", context.args.sa_id.as_str());
    params.insert("client_secret", context.args.sa_secret.as_str());

    let async_req = reqwest::Client::new()
        .post(&auth_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&params);

    let valid_until = SystemTime::now() + std::time::Duration::new(1800, 0);
    let async_resp = async_req.send();

    let token = CancellationToken::new();
    let maybe_spin = if context.args.no_spinner || context.args.concise {
        None
    } else {
        let token_clone = token.clone();
        Some(task::spawn(async { spin(token_clone).await }))
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
            if context.args.verbose {
                eprintln!("OAuth response: {:?}", resp_text);
            }

            let response: AuthResponse = serde_json::from_str(&resp_text)?;
            if response.access_token.is_none() {
                return Err(format!("Failed to authenticate: '{}'", resp_text).into());
            }

            let token_str = response.access_token.unwrap().to_string();
            let until = valid_until
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs();

            // Cache in keyring
            let cached_json = CachedTokenJson { token: token_str.clone(), until };
            keyring_store("access_token", &serde_json::to_string(&cached_json)?, no_keyring)?;

            if context.args.verbose {
                eprintln!(
                    "SA token cached in keyring, valid for {}",
                    format_remaining_time(valid_until, "".into())?
                );
            }

            context.args.jwt.clear();
            context.auth_token = Some(CachedToken { token: token_str, until });
        }
        Err(error) => {
            if context.args.verbose {
                return Err(format!("Failed to authenticate: {:?}", error).into());
            }
            return Err(format!("Failed to authenticate: {}", error).into());
        }
    }

    Ok(())
}

// ─── Maybe authenticate (dispatches based on saved auth method) ───────────────

pub async fn maybe_authenticate(
    context: &mut Context,
) -> Result<(), Box<dyn std::error::Error>> {
    // Explicit SA credentials on CLI always take priority
    if !context.args.sa_id.is_empty() {
        return authenticate_service_account(context).await;
    }

    // Check saved credentials for auth method
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Ok(());
    }

    let saved_creds: SavedCredentials =
        match serde_yaml::from_str(&fs::read_to_string(&creds_path)?) {
            Ok(c) => c,
            Err(_) => return Ok(()), // Old format or unreadable — skip
        };

    match &saved_creds.auth_method {
        AuthMethod::ServiceAccount { .. } => {
            if !context.args.sa_id.is_empty() {
                authenticate_service_account(context).await?;
            }
        }
        AuthMethod::Browser => {
            authenticate_browser_from_keyring(context, &saved_creds.oauth_env).await?;
        }
    }

    Ok(())
}

// ─── System engine discovery ──────────────────────────────────────────────────

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

    let data: EngineUrlResponse = response.json().await?;
    Ok(data.engine_url)
}

// ─── Create context from saved credentials (used internally) ─────────────────

pub async fn create_context_from_credentials(
    host: String,
    database: String,
    format: String,
    no_spinner: bool,
    no_keyring: bool,
) -> Result<Context, Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Err("No saved credentials found. Run 'fb auth' first.".into());
    }

    let saved_creds: SavedCredentials =
        serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    let mut sa_id = String::new();
    let mut sa_secret = String::new();

    if let AuthMethod::ServiceAccount { sa_id: id } = &saved_creds.auth_method {
        sa_id = id.clone();
        if let Some(secret) = keyring_load("sa_secret", no_keyring)? {
            sa_secret = secret;
        }
    }

    let temp_args = crate::args::Args {
        command: String::new(),
        core: false,
        host,
        database,
        format,
        extra: vec![],
        label: String::new(),
        jwt: String::new(),
        sa_id,
        sa_secret,
        account_name: saved_creds.account_name.clone(),
        jwt_from_file: false,
        oauth_env: saved_creds.oauth_env.clone(),
        verbose: false,
        concise: true,
        hide_pii: false,
        no_spinner,
        no_keyring,
        update_defaults: false,
        version: false,
        help: false,
        query: vec![],
    };

    let mut context = Context::new(temp_args);
    context.saved_creds = Some(saved_creds);
    maybe_authenticate(&mut context).await?;

    Ok(context)
}

// ─── Interactive setup ────────────────────────────────────────────────────────

pub async fn interactive_auth_setup(
    no_keyring: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Welcome to Firebolt CLI authentication setup!\n");
    println!("How would you like to authenticate?");
    println!("  1) Browser login (recommended)");
    println!("  2) Service Account (client_id / client_secret)");
    println!();
    print!("Enter choice [1]: ");
    io::stdout().flush()?;

    let mut choice = String::new();
    io::stdin().read_line(&mut choice)?;
    let choice = choice.trim().to_string();

    let oauth_env = "app";
    let api_endpoint = "api.app.firebolt.io";

    if choice == "2" {
        setup_service_account(no_keyring, oauth_env, api_endpoint).await
    } else {
        setup_browser(no_keyring, oauth_env, api_endpoint).await
    }
}

async fn setup_browser(
    no_keyring: bool,
    oauth_env: &str,
    api_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    print!("Enter account name (e.g., my_account): ");
    io::stdout().flush()?;
    let mut account_name = String::new();
    io::stdin().read_line(&mut account_name)?;
    let account_name = account_name.trim().to_string();

    let oidc = discover_oidc_config(oauth_env).await?;

    let temp_args = crate::args::Args {
        command: String::new(),
        core: false,
        host: api_endpoint.to_string(),
        database: String::new(),
        format: String::new(),
        extra: vec![],
        label: String::new(),
        jwt: String::new(),
        sa_id: String::new(),
        sa_secret: String::new(),
        account_name: account_name.clone(),
        jwt_from_file: false,
        oauth_env: oauth_env.to_string(),
        verbose: false,
        concise: false,
        hide_pii: false,
        no_spinner: true,
        no_keyring,
        update_defaults: false,
        version: false,
        help: false,
        query: vec![],
    };

    let mut temp_context = Context::new(temp_args);
    authenticate_browser(&mut temp_context, &oidc).await?;

    println!("✓ Authentication successful!");

    let access_token = temp_context
        .auth_token
        .as_ref()
        .map(|t| t.token.clone())
        .ok_or("Failed to obtain access token")?;

    println!("Discovering system engine endpoint...");
    let system_engine_url =
        discover_system_engine_url(&account_name, &access_token, api_endpoint).await?;
    println!("✓ System engine URL: {}", system_engine_url);

    temp_context.args.host = system_engine_url.clone();
    temp_context.update_url();

    let (final_host, final_database) =
        configure_defaults(&mut temp_context, system_engine_url).await;

    let saved_creds = SavedCredentials {
        auth_method: AuthMethod::Browser,
        oauth_env: oauth_env.to_string(),
        account_name,
        host: final_host,
        database: final_database.clone(),
    };

    let creds_path = credentials_path()?;
    fs::write(&creds_path, serde_yaml::to_string(&saved_creds)?)?;

    println!("\nCredentials saved to {:?}", creds_path);
    println!("\n✓ Setup complete! You can now run queries:");
    if final_database.is_some() {
        println!("  fb \"select 42\"");
    } else {
        println!("  fb -d <database> \"select 42\"");
    }

    Ok(())
}

async fn setup_service_account(
    no_keyring: bool,
    oauth_env: &str,
    api_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    print!("Enter Service Account ID: ");
    io::stdout().flush()?;
    let mut sa_id = String::new();
    io::stdin().read_line(&mut sa_id)?;
    let sa_id = sa_id.trim().to_string();

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

    println!("\nAuthenticating...");

    let temp_args = crate::args::Args {
        command: String::new(),
        core: false,
        host: api_endpoint.to_string(),
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
        no_keyring,
        update_defaults: false,
        version: false,
        help: false,
        query: vec![],
    };

    let mut temp_context = Context::new(temp_args);
    authenticate_service_account(&mut temp_context).await?;

    let access_token = temp_context
        .auth_token
        .as_ref()
        .map(|t| t.token.clone())
        .ok_or("Failed to obtain access token")?;

    println!("✓ Authentication successful!");

    // Store SA secret in keyring
    keyring_store("sa_secret", &sa_secret, no_keyring)?;

    println!("Discovering system engine endpoint...");
    let system_engine_url =
        discover_system_engine_url(&account_name, &access_token, api_endpoint).await?;
    println!("✓ System engine URL: {}", system_engine_url);

    temp_context.args.host = system_engine_url.clone();
    temp_context.update_url();

    let (final_host, final_database) =
        configure_defaults(&mut temp_context, system_engine_url).await;

    let saved_creds = SavedCredentials {
        auth_method: AuthMethod::ServiceAccount { sa_id: sa_id.clone() },
        oauth_env: oauth_env.to_string(),
        account_name,
        host: final_host,
        database: final_database.clone(),
    };

    let creds_path = credentials_path()?;
    fs::write(&creds_path, serde_yaml::to_string(&saved_creds)?)?;

    println!("\nCredentials saved to {:?}", creds_path);
    println!("SA secret stored in {}", if no_keyring { "~/.firebolt/fb_secrets" } else { "OS keychain" });
    println!("\n✓ Setup complete! You can now run queries:");
    if final_database.is_some() {
        println!("  fb \"select 42\"");
    } else {
        println!("  fb -d <database> \"select 42\"");
    }

    Ok(())
}

/// Shared helper: prompt for optional database/engine and return final (host, database).
async fn configure_defaults(
    temp_context: &mut Context,
    system_engine_url: String,
) -> (Option<String>, Option<String>) {
    println!("\n(Optional) Configure defaults:");

    print!("Default database name [press Enter to skip]: ");
    let _ = io::stdout().flush();
    let mut database_input = String::new();
    let _ = io::stdin().read_line(&mut database_input);
    let database_name = database_input.trim().to_string();

    let final_database = if !database_name.is_empty() {
        println!("Validating database '{}'...", database_name);
        let check_query = format!(
            "SELECT catalog_name FROM information_schema.catalogs WHERE catalog_name = '{}'",
            database_name.replace('\'', "''")
        );
        match execute_query_internal(temp_context, check_query).await {
            Ok(response) if response.contains(&database_name) => {
                println!("✓ Database '{}' validated", database_name);
                temp_context.args.database = database_name.clone();
                temp_context.update_url();
                Some(database_name)
            }
            _ => {
                eprintln!("⚠ Warning: could not validate database '{}'. Skipping.", database_name);
                None
            }
        }
    } else {
        None
    };

    print!("Default engine name [press Enter to skip]: ");
    let _ = io::stdout().flush();
    let mut engine_input = String::new();
    let _ = io::stdin().read_line(&mut engine_input);
    let engine_name = engine_input.trim().to_string();

    let final_host = if !engine_name.is_empty() {
        println!("Validating engine '{}'...", engine_name);
        let check_query = format!(
            "SELECT engine_name FROM information_schema.engines WHERE engine_name = '{}'",
            engine_name.replace('\'', "''")
        );
        match execute_query_internal(temp_context, check_query).await {
            Ok(response) if response.contains(&engine_name) => {
                println!("Resolving engine '{}' endpoint...", engine_name);
                let use_query = format!("USE ENGINE {}", engine_name);
                match crate::query::query(temp_context, use_query).await {
                    Ok(_) => {
                        println!("✓ Engine '{}' configured: {}", engine_name, temp_context.args.host);
                        Some(temp_context.args.host.clone())
                    }
                    Err(e) => {
                        eprintln!("⚠ Warning: failed to resolve engine endpoint: {}. Using system engine.", e);
                        Some(system_engine_url)
                    }
                }
            }
            _ => {
                eprintln!("⚠ Warning: could not validate engine '{}'. Using system engine.", engine_name);
                Some(system_engine_url)
            }
        }
    } else {
        Some(system_engine_url)
    };

    (final_host, final_database)
}

// ─── Execute query internally (no stdout, returns response text) ──────────────

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

    if let Some(token) = context.access_token() {
        request = request.header("authorization", format!("Bearer {}", token));
    } else if !context.args.jwt.is_empty() {
        request = request.header("authorization", format!("Bearer {}", context.args.jwt));
    }

    let response = request.send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await?;
        return Err(format!("Query failed (status {}): {}", status, text).into());
    }

    Ok(response.text().await?)
}

// ─── Set default database / engine ───────────────────────────────────────────

pub async fn set_default_database(
    database_name: String,
    no_keyring: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    let saved_creds: SavedCredentials =
        serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    let system_engine_host = if let Some(host) = &saved_creds.host {
        if let Some(pos) = host.find("?engine=") { host[..pos].to_string() } else { host.clone() }
    } else {
        return Err("No host configured. Run 'fb auth' to set up credentials.".into());
    };

    let mut temp_context = create_context_from_credentials(
        system_engine_host,
        String::new(),
        String::from("TabSeparatedWithNames"),
        true,
        no_keyring,
    )
    .await?;

    println!("Validating database '{}'...", database_name);
    let check_query = format!(
        "SELECT catalog_name FROM information_schema.catalogs WHERE catalog_name = '{}'",
        database_name.replace('\'', "''")
    );

    match execute_query_internal(&mut temp_context, check_query).await {
        Ok(response) if response.contains(&database_name) => {
            let mut updated_creds = saved_creds;
            updated_creds.database = Some(database_name.clone());
            fs::write(&creds_path, serde_yaml::to_string(&updated_creds)?)?;
            println!("✓ Default database set to: {}", database_name);
            Ok(())
        }
        _ => {
            eprintln!("Database '{}' does not exist.", database_name);
            eprintln!("Run 'fb show databases' to see available databases.");
            Err("Database validation failed".into())
        }
    }
}

pub async fn set_default_engine(
    engine_name: String,
    no_keyring: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Err("No saved credentials found. Run 'fb auth' first.".into());
    }

    let mut saved_creds: SavedCredentials =
        serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    let system_engine_host = if let Some(host) = &saved_creds.host {
        if let Some(pos) = host.find("?engine=") { host[..pos].to_string() } else { host.clone() }
    } else {
        return Err("No host configured. Run 'fb auth' first.".into());
    };

    let mut temp_context = create_context_from_credentials(
        system_engine_host,
        saved_creds.database.clone().unwrap_or_default(),
        String::new(),
        true,
        no_keyring,
    )
    .await?;

    println!("Validating engine '{}'...", engine_name);
    let check_query = format!(
        "SELECT engine_name FROM information_schema.engines WHERE engine_name = '{}'",
        engine_name.replace('\'', "''")
    );

    match execute_query_internal(&mut temp_context, check_query).await {
        Ok(response) if response.contains(&engine_name) => {}
        _ => {
            eprintln!("Engine '{}' does not exist.", engine_name);
            eprintln!("Run 'fb show engines' to see available engines.");
            return Err("Engine validation failed".into());
        }
    }

    println!("Resolving engine '{}' endpoint...", engine_name);
    let use_engine_query = format!("USE ENGINE {}", engine_name);
    crate::query::query(&mut temp_context, use_engine_query).await?;

    saved_creds.host = Some(temp_context.args.host.clone());
    fs::write(&creds_path, serde_yaml::to_string(&saved_creds)?)?;
    println!("✓ Default engine set to: {} ({})", engine_name, temp_context.args.host);

    Ok(())
}

// ─── Auth status, clear, token ───────────────────────────────────────────────

pub fn show_auth_status(no_keyring: bool) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        println!("No saved credentials found.");
        println!("Run 'fb auth' to set up authentication.");
        return Ok(());
    }

    let saved_creds: SavedCredentials =
        serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    match &saved_creds.auth_method {
        AuthMethod::ServiceAccount { sa_id } => {
            println!("Authenticated as: Service Account");
            println!("  ID: {}", sa_id);
        }
        AuthMethod::Browser => {
            println!("Authenticated as: Browser login");
        }
    }
    println!("  Account: {}", saved_creds.account_name);
    println!("  Environment: {}", saved_creds.oauth_env);

    // Show cached token expiry
    if let Ok(Some(token_json)) = keyring_load("access_token", no_keyring) {
        if let Ok(cached) = serde_json::from_str::<CachedTokenJson>(&token_json) {
            let valid_until =
                SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(cached.until);
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

    if let Some(host) = &saved_creds.host {
        println!("  Endpoint: {}", host);
    }
    if let Some(database) = &saved_creds.database {
        println!("  Default database: {}", database);
    }

    Ok(())
}

pub fn clear_auth(no_keyring: bool) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if creds_path.exists() {
        fs::remove_file(&creds_path)?;
        println!("Credentials cleared from {:?}", creds_path);
    } else {
        println!("No saved credentials to clear.");
    }

    // Clear keyring / secrets file
    keyring_delete("sa_secret", no_keyring);
    keyring_delete("access_token", no_keyring);
    keyring_delete("refresh_token", no_keyring);

    Ok(())
}

pub async fn print_access_token(
    no_keyring: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        eprintln!("No saved credentials found. Run 'fb auth' first.");
        std::process::exit(1);
    }

    let saved_creds: SavedCredentials =
        serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    // Build a minimal context to use the auth machinery
    let host = saved_creds.host.clone().unwrap_or_default();
    let context = create_context_from_credentials(
        host,
        String::new(),
        String::new(),
        true,
        no_keyring,
    )
    .await
    .map_err(|e| {
        eprintln!("Failed to authenticate: {}", e);
        std::process::exit(1);
    })
    .unwrap();

    if let Some(token) = context.access_token() {
        print!("{}", token);
        return Ok(());
    }

    // For browser mode, the token may need to come from keyring directly
    if let Some(token_json) = keyring_load("access_token", no_keyring)? {
        if let Ok(cached) = serde_json::from_str::<CachedTokenJson>(&token_json) {
            print!("{}", cached.token);
            return Ok(());
        }
    }

    eprintln!("No access token available. Run 'fb auth' first.");
    std::process::exit(1);
}

// ─── Load saved credentials into Args ────────────────────────────────────────

pub fn load_saved_credentials(
    args: &mut crate::args::Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Ok(());
    }

    let saved_creds: SavedCredentials =
        match serde_yaml::from_str(&fs::read_to_string(&creds_path)?) {
            Ok(c) => c,
            Err(_) => return Ok(()), // Old format — skip, don't crash
        };

    // Apply saved host/database if not overridden
    if args.host.is_empty() {
        if let Some(host) = &saved_creds.host {
            args.host = host.clone();
        }
    }
    if args.database.is_empty() {
        if let Some(database) = &saved_creds.database {
            args.database = database.clone();
        }
    }

    args.oauth_env = saved_creds.oauth_env.clone();
    args.account_name = saved_creds.account_name.clone();

    match &saved_creds.auth_method {
        AuthMethod::ServiceAccount { sa_id } => {
            if args.sa_id.is_empty() {
                args.sa_id = sa_id.clone();
            }
            // Load SA secret from keyring if not provided on CLI
            if args.sa_secret.is_empty() {
                if let Ok(Some(secret)) = keyring_load("sa_secret", args.no_keyring) {
                    args.sa_secret = secret;
                }
            }
        }
        AuthMethod::Browser => {
            // Nothing extra needed in args for browser mode
        }
    }

    if args.verbose {
        eprintln!("Loaded credentials from {:?}", creds_path);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Integration tests for authentication require a running Firebolt instance.
}
