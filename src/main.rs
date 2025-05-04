mod common;
mod tantivy;
mod vortex;
mod vortex_list_expr;

use std::path::PathBuf;

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
}

#[derive(Debug, Subcommand)]
enum Index {
    Tantivy { path: PathBuf },
    Vortex { path: PathBuf, buckets: u16 },
}

#[derive(Debug, Subcommand)]
enum Search {
    Tantivy { path: PathBuf, query: String },
    Vortex { path: PathBuf, query: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Index(Index::Tantivy { path }) => crate::tantivy::tantivy_index(&path)?,
        Command::Index(Index::Vortex { path, buckets }) => {
            crate::vortex::vortex_index(&path, buckets).await?
        }
        Command::Search(Search::Tantivy { path, query }) => {
            crate::tantivy::tantivy_search(&path, &query)?
        }
        Command::Search(Search::Vortex { path, query }) => {
            crate::vortex::vortex_search(&path, &query).await?
        }
    }

    Ok(())
}
