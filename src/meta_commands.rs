use crate::context::Context;
use regex::Regex;
use once_cell::sync::Lazy;

// Handle meta-commands (backslash commands)
pub fn handle_meta_command(context: &mut Context, command: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // Handle \set PROMPT1 command
    if let Some(prompt) = parse_set_prompt1(command) {
        context.set_prompt1(prompt);
        return Ok(true);
    }

    // Handle \set PROMPT2 command
    if let Some(prompt) = parse_set_prompt2(command) {
        context.set_prompt2(prompt);
        return Ok(true);
    }

    // Handle \set PROMPT3 command
    if let Some(prompt) = parse_set_prompt3(command) {
        context.set_prompt3(prompt);
        return Ok(true);
    }

    // Handle \unset PROMPT1 command
    if parse_unset_prompt1(command) {
        context.prompt1 = None;
        return Ok(true);
    }

    // Handle \unset PROMPT2 command
    if parse_unset_prompt2(command) {
        context.prompt2 = None;
        return Ok(true);
    }

    // Handle \unset PROMPT3 command
    if parse_unset_prompt3(command) {
        context.prompt3 = None;
        return Ok(true);
    }

    Ok(false)
}

// Parse \set PROMPT1 'value' command
fn parse_set_prompt1(command: &str) -> Option<String> {
    static SET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\set\s+PROMPT1\s+(?:'([^']*)'|"([^"]*)"|(\S+))\s*$"#).unwrap()
    });

    if let Some(captures) = SET_PROMPT_RE.captures(command) {
        // Check which capture group matched
        if let Some(prompt) = captures.get(1) {
            return Some(prompt.as_str().to_string());
        } else if let Some(prompt) = captures.get(2) {
            return Some(prompt.as_str().to_string());
        } else if let Some(prompt) = captures.get(3) {
            return Some(prompt.as_str().to_string());
        }
    }

    None
}

// Parse \set PROMPT2 'value' command
fn parse_set_prompt2(command: &str) -> Option<String> {
    static SET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\set\s+PROMPT2\s+(?:'([^']*)'|"([^"]*)"|(\S+))\s*$"#).unwrap()
    });

    if let Some(captures) = SET_PROMPT_RE.captures(command) {
        // Check which capture group matched
        if let Some(prompt) = captures.get(1) {
            return Some(prompt.as_str().to_string());
        } else if let Some(prompt) = captures.get(2) {
            return Some(prompt.as_str().to_string());
        } else if let Some(prompt) = captures.get(3) {
            return Some(prompt.as_str().to_string());
        }
    }

    None
}

// Parse \set PROMPT3 'value' command
fn parse_set_prompt3(command: &str) -> Option<String> {
    static SET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\set\s+PROMPT3\s+(?:'([^']*)'|"([^"]*)"|(\S+))\s*$"#).unwrap()
    });

    if let Some(captures) = SET_PROMPT_RE.captures(command) {
        // Check which capture group matched
        if let Some(prompt) = captures.get(1) {
            return Some(prompt.as_str().to_string());
        } else if let Some(prompt) = captures.get(2) {
            return Some(prompt.as_str().to_string());
        } else if let Some(prompt) = captures.get(3) {
            return Some(prompt.as_str().to_string());
        }
    }

    None
}

// Parse \unset PROMPT1 command
fn parse_unset_prompt1(command: &str) -> bool {
    static UNSET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\unset\s+PROMPT1\s*$"#).unwrap()
    });

    UNSET_PROMPT_RE.is_match(command)
}

// Parse \unset PROMPT2 command
fn parse_unset_prompt2(command: &str) -> bool {
    static UNSET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\unset\s+PROMPT2\s*$"#).unwrap()
    });

    UNSET_PROMPT_RE.is_match(command)
}

// Parse \unset PROMPT3 command
fn parse_unset_prompt3(command: &str) -> bool {
    static UNSET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\unset\s+PROMPT3\s*$"#).unwrap()
    });

    UNSET_PROMPT_RE.is_match(command)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::get_args;

    #[test]
    fn test_set_prompt1_single_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT1 'custom_prompt> '"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt1, Some("custom_prompt> ".to_string()));
    }

    #[test]
    fn test_set_prompt1_double_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT1 "custom_prompt> ""#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt1, Some("custom_prompt> ".to_string()));
    }

    #[test]
    fn test_set_prompt1_no_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT1 custom_prompt>"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt1, Some("custom_prompt>".to_string()));
    }

    #[test]
    fn test_set_prompt2_single_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT2 'custom_prompt> '"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt2, Some("custom_prompt> ".to_string()));
    }

    #[test]
    fn test_set_prompt2_double_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT2 "custom_prompt> ""#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt2, Some("custom_prompt> ".to_string()));
    }

    #[test]
    fn test_set_prompt2_no_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT2 custom_prompt>"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt2, Some("custom_prompt>".to_string()));
    }

    #[test]
    fn test_set_prompt3_single_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT3 'custom_prompt> '"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt3, Some("custom_prompt> ".to_string()));
    }

    #[test]
    fn test_set_prompt3_double_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT3 "custom_prompt> ""#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt3, Some("custom_prompt> ".to_string()));
    }

    #[test]
    fn test_set_prompt3_no_quotes() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        let command = r#"\set PROMPT3 custom_prompt>"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt3, Some("custom_prompt>".to_string()));
    }

    #[test]
    fn test_unset_prompt1() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        // First set a prompt
        context.set_prompt1("test> ".to_string());
        assert_eq!(context.prompt1, Some("test> ".to_string()));
        
        // Then unset it
        let command = r#"\unset PROMPT1"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt1, None);
    }

    #[test]
    fn test_unset_prompt2() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        // First set a prompt
        context.set_prompt2("test> ".to_string());
        assert_eq!(context.prompt2, Some("test> ".to_string()));
        
        // Then unset it
        let command = r#"\unset PROMPT2"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt2, None);
    }

    #[test]
    fn test_unset_prompt3() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        // First set a prompt
        context.set_prompt3("test> ".to_string());
        assert_eq!(context.prompt3, Some("test> ".to_string()));
        
        // Then unset it
        let command = r#"\unset PROMPT3"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt3, None);
    }

    #[test]
    fn test_invalid_commands() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        // Invalid commands should return false
        let command = r#"\invalid command"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(!result);
        
        let command = r#"\set INVALID value"#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_whitespace_handling() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        // Test with various whitespace
        let command = r#"  \set  PROMPT1  'test>'  "#;
        let result = handle_meta_command(&mut context, command).unwrap();
        assert!(result);
        assert_eq!(context.prompt1, Some("test>".to_string()));
    }

    #[test]
    fn test_prompt_independence() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        // Set all three prompts to different values
        let command1 = r#"\set PROMPT1 'prompt1> '"#;
        let command2 = r#"\set PROMPT2 'prompt2> '"#;
        let command3 = r#"\set PROMPT3 'prompt3> '"#;
        
        handle_meta_command(&mut context, command1).unwrap();
        handle_meta_command(&mut context, command2).unwrap();
        handle_meta_command(&mut context, command3).unwrap();
        
        // Verify all prompts are set independently
        assert_eq!(context.prompt1, Some("prompt1> ".to_string()));
        assert_eq!(context.prompt2, Some("prompt2> ".to_string()));
        assert_eq!(context.prompt3, Some("prompt3> ".to_string()));
        
        // Unset only PROMPT2
        let unset_command = r#"\unset PROMPT2"#;
        handle_meta_command(&mut context, unset_command).unwrap();
        
        // Verify only PROMPT2 was unset
        assert_eq!(context.prompt1, Some("prompt1> ".to_string()));
        assert_eq!(context.prompt2, None);
        assert_eq!(context.prompt3, Some("prompt3> ".to_string()));
    }
}
