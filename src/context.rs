use crate::args::{get_url, Args};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServiceAccountToken {
    pub sa_id: String,
    pub sa_secret: String,
    pub token: String,
    pub until: u64,
}

pub struct Context {
    pub args: Args,
    pub url: String,
    pub sa_token: Option<ServiceAccountToken>,
}

impl Context {
    pub fn new(args: Args) -> Self {
        let url = get_url(&args);
        Self { args, url, sa_token: None }
    }

    pub fn update_url(&mut self) {
        self.url = get_url(&self.args);
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
        assert!(context.sa_token.is_none());
    }
}
