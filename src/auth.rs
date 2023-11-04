use serde::Deserialize;
use std::fs;
use std::time::SystemTime;
use tokio::task;
use tokio_util::sync::CancellationToken;

use crate::context::{Context, ServiceAccountToken};
use crate::utils::{format_remaining_time, sa_token_path, spin};

pub async fn authenticate_service_account(context: &mut Context) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(sa_token) = &context.sa_token {
        let valid_until = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(sa_token.until);
        if sa_token.sa_id == context.args.sa_id && sa_token.sa_secret == context.args.sa_secret && valid_until > SystemTime::now() {
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
            if sa_token.sa_id == args.sa_id && sa_token.sa_secret == args.sa_secret && valid_until > SystemTime::now() {
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
        Err(error) => return Err(format!("Failed to authenticate: {:?}", error).into()),
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

#[cfg(test)]
mod tests {
    // Add tests for authentication functionality when possible
}
