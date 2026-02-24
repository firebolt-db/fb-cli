/// Finds the start position of the word at the given position
pub fn find_word_start(line: &str, pos: usize) -> usize {
    let bytes = line.as_bytes();
    let mut start = pos;

    while start > 0 {
        let prev = start - 1;
        let ch = bytes[prev] as char;

        // Word characters: alphanumeric, underscore, or dot (for schema.table)
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            start = prev;
        } else {
            break;
        }
    }

    start
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_word_start() {
        assert_eq!(find_word_start("SELECT * FROM users", 19), 14);
        assert_eq!(find_word_start("SELECT col", 10), 7);
        assert_eq!(find_word_start("public.users", 12), 0);
        assert_eq!(find_word_start("SELECT * FROM public.us", 23), 14);
    }
}
