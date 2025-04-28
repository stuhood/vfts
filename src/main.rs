use std::sync::Arc;

use vortex_array::arrays::{BooleanBufferBuilder, ConstantArray, ListArray};
use vortex_array::compute;
use vortex_array::{Array, ArrayRef, IntoArray, ToCanonical};
use vortex_btrblocks::BtrBlocksCompressor;
use vortex_buffer::buffer;
use vortex_dtype::DType::Utf8;
use vortex_dtype::Nullability;
use vortex_error::{VortexResult, vortex_bail};
use vortex_mask::Mask;

fn main() -> anyhow::Result<()> {
    let key = buffer![3u64, 1, 5, 7].into_array();

    // NB: u16 here is the max length of each token list.
    let tokens = ListArray::from_iter_slow::<u16, _>(
        vec![vec!["foo", "bar"], vec![], vec!["baz", "bar"], vec!["foo"]],
        Arc::new(Utf8(Nullability::NonNullable).into()),
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
