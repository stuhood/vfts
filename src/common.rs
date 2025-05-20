use std::collections::HashSet;

pub type Document = (u64, HashSet<String>);

pub fn tokenize(document: &str) -> HashSet<String> {
    document
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .collect()
}

pub fn documents(doc_count: usize) -> impl Iterator<Item = Document> {
    include_str!("./all_the_henries.txt")
        .lines()
        .cycle()
        .take(doc_count)
        .map(tokenize)
        .enumerate()
        .map(|(id, document)| (id.try_into().unwrap(), document))
}
