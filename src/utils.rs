use std::fs;
use std::io::stdout;
use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::select;
use tokio_util::sync::CancellationToken;

// Init root path for config and history.
pub fn init_root_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(home_dir) = dirs::home_dir() {
        let home_dir = home_dir.join(".firebolt");
        fs::create_dir_all(&home_dir)?;
        Ok(home_dir)
    } else {
        Err(Box::from("Failed to get home directory"))
    }
}

// Get config path on disk.
pub fn config_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(init_root_path()?.join("fb_config"))
}

// Get history path on disk.
pub fn history_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(init_root_path()?.join("fb_history"))
}

// Get sa_token path on disk.
pub fn sa_token_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(init_root_path()?.join("fb_sa_token"))
}

// Format remaining time for token validity
pub fn format_remaining_time(time: SystemTime, maybe_more: String) -> Result<String, Box<dyn std::error::Error>> {
    let remaining = time.duration_since(SystemTime::now())?.as_secs();
    if remaining > 60 {
        return Ok(format!("{:?} {:} minutes and {:?} seconds", remaining / 60, maybe_more, remaining % 60).to_string());
    }

    return Ok(format!("{:?} {:?} seconds", remaining, maybe_more).to_string());
}

// Draw spinner until cancelled.
pub async fn spin(token: CancellationToken) {
    let spins = ['─', '\\', '|', '/'];
    let mut it = 0;
    print!("{}", spins[it]);
    it += 1;
    let _ = stdout().flush();
    loop {
        select! {
            _ = token.cancelled() => {
                print!("\x08 \x08");
                let _ = stdout().flush();
                return;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {
                print!("\x08{}", spins[it]);
                it = (it + 1) % spins.len();
                let _ = stdout().flush();
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_functions() {
        let root = init_root_path().unwrap();
        assert!(root.ends_with(".firebolt"));

        let config = config_path().unwrap();
        assert!(config.ends_with("fb_config"));

        let history = history_path().unwrap();
        assert!(history.ends_with("fb_history"));

        let sa_token = sa_token_path().unwrap();
        assert!(sa_token.ends_with("fb_sa_token"));
    }
}
