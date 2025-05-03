use std::collections::HashSet;

pub type Document = HashSet<String>;

pub fn tokenize(document: &str) -> Document {
    document
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .collect()
}

pub fn documents() -> impl Iterator<Item = Document> {
    include_str!("./all_the_henries.txt").lines().map(tokenize)
}
