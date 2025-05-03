mod common;
mod tantivy;
mod vortex;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    index: Index,
}

#[derive(Debug, Subcommand)]
enum Index {
    Tantivy { path: PathBuf },
    Vortex { path: PathBuf, buckets: u16 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.index {
        Index::Tantivy { path } => crate::tantivy::tantivy_index(&path)?,
        Index::Vortex { path, buckets } => crate::vortex::vortex_index(&path, buckets).await?,
    }

    Ok(())
}
