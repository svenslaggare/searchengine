use std::collections::HashSet;
use std::iter::FromIterator;

use regex::Regex;

pub type Token = String;
pub type Tokens = Vec<Token>;

pub struct Tokenizer {
    separator_characters: HashSet<char>,
    ignore_characters: HashSet<char>
}

impl Tokenizer {
    pub fn new() -> Tokenizer {
        Tokenizer {
            // Chars that leads to new tokens
            separator_characters: HashSet::from_iter(vec![
                ' ',
                '\t',
                '\n',
                '.',
                ',',
                ':',
                ';',
                '?',
                '-',
                '–',
                '/',
                '#',
                '&',
                '\\',
            ].into_iter()),
            // Chars that are ignored entirely
            ignore_characters: HashSet::from_iter(vec![
                '(',
                ')',
                '[',
                ']',
                '{',
                '}',
                '"',
                '\'',
                '^',
                '®',
                'ʹ',
            ].into_iter())
        }
    }

    pub fn tokenize(&self, content: &str) -> Tokens {
        let mut tokens = Vec::new();

        let mut token = String::new();

        let mut add_token = |token: String| {
            if token.is_empty() {
                return;
            }

            tokens.push(token.to_lowercase());
        };

        for current in content.chars() {
            if self.separator_characters.contains(&current) {
                let mut new_token = String::new();
                std::mem::swap(&mut new_token, &mut token);
                add_token(new_token);
            } else {
                if !self.ignore_characters.contains(&current) {
                    token.push(current);
                }
            }
        }

        add_token(token);
        tokens
    }
}

#[test]
fn test_tokenize1() {
    let extractor = Tokenizer::new();
    let tokens = extractor.tokenize("Testing this thing");
    assert_eq!(vec!["testing", "this", "thing"], tokens);
}

#[test]
fn test_tokenize2() {
    let extractor = Tokenizer::new();
    let tokens = extractor.tokenize("Testing this thing.");
    assert_eq!(vec!["testing", "this", "thing"], tokens);
}

#[test]
fn test_tokenize3() {
    let extractor = Tokenizer::new();
    let tokens = extractor.tokenize("Does (this) work?");
    assert_eq!(vec!["does", "this", "work"], tokens);
}