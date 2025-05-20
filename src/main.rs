mod common;
mod tantivy;
mod vortex;
mod vortex_list_expr;

use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(subcommand)]
    Index(Index),
    #[command(subcommand)]
    Search(Search),
    #[command(subcommand)]
    SearchMany(SearchMany),
}

#[derive(Debug, Subcommand)]
enum Index {
    Tantivy {
        path: PathBuf,
        documents: usize,
    },
    Vortex {
        path: PathBuf,
        documents: usize,
        buckets: u16,
    },
}

#[derive(Debug, Subcommand)]
enum Search {
    Tantivy { path: PathBuf, query: String },
    Vortex { path: PathBuf, query: String },
}

#[derive(Debug, Subcommand)]
enum SearchMany {
    Tantivy { path: PathBuf, queries: usize },
    Vortex { path: PathBuf, queries: usize },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let start = Instant::now();
    match cli.command {
        Command::Index(Index::Tantivy { path, documents }) => {
            crate::tantivy::tantivy_index(&path, documents)?
        }
        Command::Index(Index::Vortex {
            path,
            documents,
            buckets,
        }) => crate::vortex::vortex_index(&path, documents, buckets).await?,
        Command::Search(Search::Tantivy { path, query }) => {
            crate::tantivy::tantivy_search(&path, &query)?
        }
        Command::Search(Search::Vortex { path, query }) => {
            crate::vortex::vortex_search(&path, &query).await?
        }
        Command::SearchMany(SearchMany::Tantivy { path, queries }) => {
            crate::tantivy::tantivy_search_many(&path, queries)?
        }
        Command::SearchMany(SearchMany::Vortex { path, queries }) => {
            crate::vortex::vortex_search_many(&path, queries).await?
        }
    }
    println!(">>> elapsed: {:?}", start.elapsed());

    Ok(())
}
