use crate::context::Context;
use regex::Regex;
use once_cell::sync::Lazy;

// Handle meta-commands (backslash commands)
pub fn handle_meta_command(context: &mut Context, command: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // Handle \set PROMPT1 command
    if let Some(prompt) = parse_set_prompt(command, "PROMPT1") {
        context.set_prompt1(prompt);
        return Ok(true);
    }

    // Handle \set PROMPT2 command
    if let Some(prompt) = parse_set_prompt(command, "PROMPT2") {
        context.set_prompt2(prompt);
        return Ok(true);
    }

    // Handle \set PROMPT3 command
    if let Some(prompt) = parse_set_prompt(command, "PROMPT3") {
        context.set_prompt3(prompt);
        return Ok(true);
    }

    // Handle \unset PROMPT1 command
    if parse_unset_prompt(command, "PROMPT1") {
        context.prompt1 = None;
        return Ok(true);
    }

    // Handle \unset PROMPT2 command
    if parse_unset_prompt(command, "PROMPT2") {
        context.prompt2 = None;
        return Ok(true);
    }

    // Handle \unset PROMPT3 command
    if parse_unset_prompt(command, "PROMPT3") {
        context.prompt3 = None;
        return Ok(true);
    }

    Ok(false)
}

// Generic function to parse \set PROMPT command
fn parse_set_prompt(command: &str, prompt_type: &str) -> Option<String> {
    static SET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\set\s+(\w+)\s+(?:'([^']*)'|"([^"]*)"|(\S+))\s*$"#).unwrap()
    });

    if let Some(captures) = SET_PROMPT_RE.captures(command) {
        // Check if the prompt type matches
        if let Some(cmd_prompt_type) = captures.get(1) {
            if cmd_prompt_type.as_str().eq_ignore_ascii_case(prompt_type) {
                // Check which capture group matched for the value
                if let Some(prompt) = captures.get(2) {
                    return Some(prompt.as_str().to_string());
                } else if let Some(prompt) = captures.get(3) {
                    return Some(prompt.as_str().to_string());
                } else if let Some(prompt) = captures.get(4) {
                    return Some(prompt.as_str().to_string());
                }
            }
        }
    }

    None
}

// Generic function to parse \unset PROMPT command
fn parse_unset_prompt(command: &str, prompt_type: &str) -> bool {
    static UNSET_PROMPT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)^\s*\\unset\s+(\w+)\s*$"#).unwrap()
    });

    if let Some(captures) = UNSET_PROMPT_RE.captures(command) {
        if let Some(cmd_prompt_type) = captures.get(1) {
            return cmd_prompt_type.as_str().eq_ignore_ascii_case(prompt_type);
        }
    }

    false
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

    #[test]
    fn test_case_insensitive_prompt_types() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);
        
        // Test case insensitive prompt type matching
        let command1 = r#"\set prompt1 'test1> '"#;
        let command2 = r#"\set Prompt2 'test2> '"#;
        let command3 = r#"\set PROMPT3 'test3> '"#;
        
        handle_meta_command(&mut context, command1).unwrap();
        handle_meta_command(&mut context, command2).unwrap();
        handle_meta_command(&mut context, command3).unwrap();
        
        // Verify all prompts are set correctly regardless of case
        assert_eq!(context.prompt1, Some("test1> ".to_string()));
        assert_eq!(context.prompt2, Some("test2> ".to_string()));
        assert_eq!(context.prompt3, Some("test3> ".to_string()));
    }
}
