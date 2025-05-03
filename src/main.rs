use std::collections::HashSet;
use std::path::{Path, PathBuf};

use clap::Parser;
use tokio::fs::OpenOptions;

use vortex_array::arrays::{BooleanBufferBuilder, ConstantArray, StructArray};
use vortex_array::builders::{ArrayBuilderExt, builder_with_capacity};
use vortex_array::compute;
use vortex_array::stream::ArrayStreamArrayExt;
use vortex_array::validity::Validity;
use vortex_array::{Array, ArrayRef, ToCanonical};
use vortex_dtype::{DType, Nullability};
use vortex_error::{VortexResult, vortex_bail};
use vortex_file::VortexWriteOptions;
use vortex_mask::Mask;

#[derive(Parser, Debug)]
struct Cli {
    path: PathBuf,
    buckets: u16
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    vortex_index(&cli.path, cli.buckets).await?;

    Ok(())
}

///
/// Given a list of (primary) keys and a ListArray<Utf8> of tokens, returns the keys which contain
/// the given token in their lists.
///
fn key_having_token(key: &dyn Array, tokens: &dyn Array, token: &str) -> VortexResult<ArrayRef> {
    if key.len() != tokens.len() {
        vortex_bail!("Keys and tokens must have equal lengths.");
    }

    // TODO: `to_list` seems to be related to decoding, which we don't want to do.
    let tokens = tokens.to_list()?;

    // Find the matching token offsets.
    let matching_tokens = compute::compare(
        tokens.elements(),
        &ConstantArray::new(token, tokens.elements().len()),
        compute::Operator::Eq,
    )?;

    // For each matching token, determine the index of the associated offset.
    let mut key_mask = BooleanBufferBuilder::new(key.len());
    key_mask.resize(key.len());
    for token_offset in matching_tokens.to_bool()?.boolean_buffer().set_indices() {
        let search_result = compute::search_sorted_usize(
            tokens.offsets(),
            token_offset,
            compute::SearchSortedSide::Left,
        )?;
        if let compute::SearchResult::Found(absolute_offset) = search_result {
            key_mask.set_bit(absolute_offset, true);
        }
    }

    // Filter out the matched keys.
    let matching_keys = compute::filter(key, &Mask::from_buffer(key_mask.into()))?;

    Ok(matching_keys)
}

///
/// Given a non-unique set of sample tokens from a dataset, select `pivot_count` bucket values which
/// will roughly equally divide the sample.
///
fn select_buckets_from(mut sample_tokens: Vec<String>, pivot_count: u16) -> Vec<String> {
    assert!(!sample_tokens.is_empty());
    sample_tokens.sort_unstable();

    let bucket_width = sample_tokens.len() as f64 / pivot_count as f64;
    let mut buckets = (0..pivot_count)
        .map(|idx| sample_tokens[(idx as f64 * bucket_width).floor() as usize].clone())
        .collect::<Vec<_>>();
    // TODO: Duplicates represent a very good opportunity for single-valued buckets.
    buckets.dedup();
    buckets
}

fn tokenize(document: &str) -> HashSet<String> {
    document
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .collect()
}

fn documents() -> impl Iterator<Item = Document> {
    include_str!("./all_the_henries.txt").lines().map(tokenize)
}

type Document = HashSet<String>;

async fn vortex_index(path: &Path, bucket_count: u16) -> anyhow::Result<()> {
    // TODO: Support single-valued buckets.
    let buckets = select_buckets_from(
        documents()
            .take(1000)
            .map(|doc| doc.into_iter())
            .flatten()
            .collect(),
        bucket_count,
    );

    let mut builders = buckets
        .iter()
        .map(|_| {
            builder_with_capacity(
                &DType::List(
                    DType::Utf8(Nullability::NonNullable).into(),
                    Nullability::NonNullable,
                )
                .into(),
                128,
            )
        })
        .collect::<Vec<_>>();
    let mut entries_to_append: Vec<Vec<String>> = buckets.iter().map(|_| Vec::new()).collect();
    let mut doc_count = 0;
    for document in documents() {
        // Group the tokens by the bucket that they will be appended to.
        for token in document {
            let idx = match buckets.binary_search(&token) {
                Ok(idx) => idx,
                Err(idx) => idx - 1,
            };
            entries_to_append[idx].push(token);
        }
        // Drain all buckets into the builders. Many of them will be empty, and that is ok.
        for (idx, entries) in entries_to_append.iter_mut().enumerate() {
            builders[idx].append_scalar(&entries.drain(..).collect::<Vec<_>>().into())?;
        }
        doc_count += 1;
    }

    let st = StructArray::try_new(
        buckets.into_iter().map(|s| s.into()).collect(),
        builders.into_iter().map(|mut b| b.finish()).collect(),
        doc_count,
        Validity::NonNullable,
    )?;

    vortex_index_array(path, st.into_array()).await?;
    println!(">>> created {path:?}, with {doc_count} documents in {bucket_count} buckets");
    Ok(())
}

async fn vortex_index_array(path: &Path, array: ArrayRef) -> anyhow::Result<()> {
    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&path)
        .await?;

    VortexWriteOptions::default()
        .write(f, array.to_array_stream())
        .await?;

    Ok(())
}
