use crate::auth::authenticate_service_account;
use crate::context::{Context, SavedCredentials};
use crate::utils::credentials_path;
use std::fs;

/// Load saved credentials and create a context for internal queries
async fn create_query_context(
    database: Option<String>,
    format: Option<String>,
) -> Result<Context, Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Err("No saved credentials found. Run 'fb auth' first.".into());
    }

    let saved_creds: SavedCredentials = serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    // Get system engine host
    let system_engine_host = if let Some(host) = &saved_creds.host {
        if let Some(pos) = host.find("?engine=") {
            host[..pos].to_string()
        } else {
            host.clone()
        }
    } else {
        return Err("No host configured. Run 'fb auth' to set up credentials.".into());
    };

    let temp_args = crate::args::Args {
        command: String::new(),
        core: false,
        host: system_engine_host,
        database: database.unwrap_or_default(),
        format: format.unwrap_or_else(|| String::from("PSQL")),
        extra: vec![],
        label: String::new(),
        jwt: String::new(),
        sa_id: saved_creds.sa_id.clone(),
        sa_secret: saved_creds.sa_secret.clone(),
        account_name: saved_creds.account_name.clone(),
        jwt_from_file: false,
        oauth_env: saved_creds.oauth_env.clone(),
        verbose: false,
        concise: false,
        hide_pii: false,
        no_spinner: false,
        update_defaults: false,
        version: false,
        help: false,
        query: vec![],
    };

    let mut context = Context::new(temp_args);
    context.update_url();

    // Authenticate
    authenticate_service_account(&mut context).await?;

    Ok(context)
}

/// Show available databases
pub async fn show_databases() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = create_query_context(None, None).await?;

    println!("Available databases:\n");
    let query = "SELECT catalog_name FROM information_schema.catalogs ORDER BY catalog_name";
    crate::query::query(&mut context, query.to_string()).await?;

    Ok(())
}

/// Show available engines
pub async fn show_engines() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = create_query_context(None, None).await?;

    println!("Available engines:\n");
    let query = "SELECT engine_name, status FROM information_schema.engines ORDER BY status, engine_name";
    crate::query::query(&mut context, query.to_string()).await?;

    Ok(())
}
