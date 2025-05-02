use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;
use tokio::fs::OpenOptions;

use vortex_array::arrays::{BooleanBufferBuilder, ConstantArray, ListArray, StructArray};
use vortex_array::compute;
use vortex_array::stream::ArrayStreamArrayExt;
use vortex_array::{Array, ArrayRef, IntoArray, ToCanonical};
use vortex_btrblocks::BtrBlocksCompressor;
use vortex_buffer::buffer;
use vortex_dtype::{DType, Nullability};
use vortex_error::{VortexResult, vortex_bail};
use vortex_file::VortexWriteOptions;
use vortex_mask::Mask;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let key = buffer![3u64, 1, 5, 7].into_array();

    // NB: u16 here is the max length of each token list.
    let tokens = ListArray::from_iter_slow::<u16, _>(
        vec![vec!["foo", "bar"], vec![], vec!["baz", "bar"], vec!["foo"]],
        Arc::new(DType::Utf8(Nullability::NonNullable).into()),
    )
    .unwrap();

    let compressed_tokens = BtrBlocksCompressor::compress(&BtrBlocksCompressor, &tokens)?;

    let result_array = key_having_token(&key, &compressed_tokens, "foo")?;

    let result = result_array
        .to_primitive()?
        .buffer::<u64>()
        .into_iter()
        .collect::<Vec<_>>();

    println!(">>> {result:?}");

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
/// Given a non-unique set of sample tokens from a dataset, select `pivot_count` pivot values which
/// will roughly equally divide the sample.
///
fn select_pivots_from(mut sample_tokens: Vec<String>, pivot_count: u16) -> Vec<String> {
    assert!(!sample_tokens.is_empty());
    sample_tokens.sort_unstable();

    let bucket_width = sample_tokens.len() as f64 / pivot_count as f64;
    (0..pivot_count)
        .map(|idx| sample_tokens[(idx as f64 * bucket_width).floor() as usize].clone())
        .collect()
}

fn tokenize(document: &str) -> HashSet<String> {
    document
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .collect()
}

fn documents() -> impl Iterator<Item = &'static str> {
    include_str!("./all_the_henries.txt").lines()
}

type Document = (usize, HashSet<String>);

async fn vortex_index(
    temp_dir: TempDir,
    document_sample: impl Iterator<Item = Document>,
    documents: impl Iterator<Item = Document>,
) -> anyhow::Result<PathBuf> {
    let st = StructArray::try_new(todo!(), todo!(), todo!(), todo!())?;

    vortex_index_array(temp_dir, st.into_array()).await
}

async fn vortex_index_array(temp_dir: TempDir, array: ArrayRef) -> anyhow::Result<PathBuf> {
    let filepath = temp_dir.path().join("a.vortex");

    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&filepath)
        .await?;

    VortexWriteOptions::default()
        .write(f, array.to_array_stream())
        .await?;

    Ok(filepath)
}
