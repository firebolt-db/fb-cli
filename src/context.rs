use crate::args::{get_url, Args};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "auth_method")]
pub enum AuthMethod {
    #[serde(rename = "service_account")]
    ServiceAccount { sa_id: String },
    #[serde(rename = "browser")]
    Browser,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedCredentials {
    pub auth_method: AuthMethod,
    pub oauth_env: String,
    pub account_name: String,
    pub host: Option<String>,
    pub database: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CachedToken {
    pub token: String,
    pub until: u64, // expiry as unix epoch seconds
}

pub struct Context {
    pub args: Args,
    pub url: String,
    pub auth_token: Option<CachedToken>,
    pub saved_creds: Option<SavedCredentials>,
    pub prompt1: Option<String>,
    pub prompt2: Option<String>,
    pub prompt3: Option<String>,
}

impl Context {
    pub fn new(args: Args) -> Self {
        let url = get_url(&args);
        Self { args, url, auth_token: None, saved_creds: None, prompt1: None, prompt2: None, prompt3: None }
    }

    pub fn update_url(&mut self) {
        self.url = get_url(&self.args);
    }

    pub fn access_token(&self) -> Option<&str> {
        self.auth_token.as_ref().map(|t| t.token.as_str())
    }

    pub fn set_prompt1(&mut self, prompt: String) {
        self.prompt1 = Some(prompt);
    }

    pub fn set_prompt2(&mut self, prompt: String) {
        self.prompt2 = Some(prompt);
    }

    pub fn set_prompt3(&mut self, prompt: String) {
        self.prompt3 = Some(prompt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let mut args = crate::args::get_args().unwrap();
        args.host = "localhost:8123".to_string();
        args.database = "test_db".to_string();

        let context = Context::new(args);

        assert!(context.url.contains("localhost:8123"));
        assert!(context.url.contains("database=test_db"));
        assert!(context.auth_token.is_none());
    }
}
