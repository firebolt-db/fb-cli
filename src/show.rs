use crate::utils::credentials_path;
use std::fs;

/// Load saved credentials and create a context for internal queries
async fn create_query_context(
    database: Option<String>,
    format: Option<String>,
) -> Result<crate::context::Context, Box<dyn std::error::Error>> {
    let creds_path = credentials_path()?;
    if !creds_path.exists() {
        return Err("No saved credentials found. Run 'fb auth' first.".into());
    }

    let saved_creds: crate::context::SavedCredentials =
        serde_yaml::from_str(&fs::read_to_string(&creds_path)?)?;

    // Use system engine host (strip any ?engine= query param)
    let system_engine_host = if let Some(host) = &saved_creds.host {
        if let Some(pos) = host.find("?engine=") {
            host[..pos].to_string()
        } else {
            host.clone()
        }
    } else {
        return Err("No host configured. Run 'fb auth' to set up credentials.".into());
    };

    crate::auth::create_context_from_credentials(
        system_engine_host,
        database.unwrap_or_default(),
        format.unwrap_or_else(|| String::from("PSQL")),
        false,
        false,
    )
    .await
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
