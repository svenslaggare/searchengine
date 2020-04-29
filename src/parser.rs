use crate::content_extractor::ContentExtractor;
use crate::tokenizer::{Tokenizer, Token, Tokens};

pub struct ParsedContent {
    pub title: Option<String>,
    pub tokens: Tokens
}

impl ParsedContent {
    pub fn new(title: Option<String>, tokens: Tokens) -> ParsedContent {
        ParsedContent {
            title,
            tokens
        }
    }
}

pub struct Parser {
    content_extractor: ContentExtractor,
    tokenizer: Tokenizer
}

impl Parser {
    pub fn new() -> Parser {
        Parser {
            content_extractor: ContentExtractor::new(),
            tokenizer: Tokenizer::new()
        }
    }

    pub fn parse(&self, content: &str) -> ParsedContent {
        let extracted_content = self.content_extractor.extract(content);
        ParsedContent::new(
            extracted_content.title,
            self.tokenizer.tokenize(&extracted_content.body)
        )
    }
}

#[test]
fn test_parse1() {
    let parser = Parser::new();
    let content = parser.parse("<html><title>Testing!</title><div><b>Haha</b> <a href=\"wololo\">Wololo</a> Again.</div></html>");
    assert_eq!(content.tokens, vec!["haha", "wololo", "again"]);
    assert_eq!(content.title.unwrap(), "Testing!");
}

#[test]
fn test_parse_full() {
    let parser = Parser::new();

    let text_input = std::fs::read_to_string("testdata/test_full_page2.txt").unwrap();
    let content = parser.parse(&text_input);
    let tokens_expected = std::fs::read_to_string("testdata/test_full_page2_tokens.txt").unwrap();
    assert_eq!(content.title.unwrap(), "Air &amp; space/smithsonian - Wikipedia");
    assert_eq!(content.tokens.join(" "), tokens_expected);
}