use std::path::Path;

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

///
/// Given a list of (primary) keys and a ListArray<Utf8> of tokens, returns the keys which contain
/// the given token in their lists.
///
#[allow(dead_code)]
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
/// Given a non-unique sample of tokens from a dataset, select `pivot_count` bucket values which
/// will roughly equally divide the sample.
///
fn select_buckets_from(
    mut sample_tokens: Vec<String>,
    pivot_count: u16,
) -> Vec<(String, BucketType)> {
    assert!(!sample_tokens.is_empty());
    assert!(pivot_count > 0);
    sample_tokens.sort_unstable();

    let bucket_width = sample_tokens.len() as f64 / pivot_count as f64;
    let mut candidates = (0..pivot_count)
        .map(|idx| sample_tokens[(idx as f64 * bucket_width).floor() as usize].clone());

    let mut previous_candidate = candidates.next().unwrap();
    let mut single_value_emitted = false;
    let mut buckets = Vec::with_capacity(pivot_count as usize);
    for candidate in candidates {
        if candidate != previous_candidate {
            // Emit the previous candidate as a multi-valued bucket.
            buckets.push((previous_candidate, BucketType::Multi));
            single_value_emitted = false;
        } else if !single_value_emitted {
            // Emit the previous candidate as a single-valued bucket.
            buckets.push((previous_candidate, BucketType::Single));
            single_value_emitted = true;
        }
        previous_candidate = candidate;
    }
    // Finally, cap with a multi-valued bucket.
    buckets.push((previous_candidate, BucketType::Multi));
    buckets
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
enum BucketType {
    Single,
    Multi,
}

impl BucketType {
    fn column_name(&self, mut token: String) -> String {
        match self {
            BucketType::Single => token.push_str(":s"),
            BucketType::Multi => token.push_str(":m"),
        }
        token
    }
}

pub async fn vortex_index(path: &Path, buckets: u16) -> anyhow::Result<()> {
    let buckets = select_buckets_from(
        crate::common::documents()
            .take(1000)
            .map(|doc| doc.into_iter())
            .flatten()
            .collect(),
        buckets,
    );
    let bucket_count = buckets.len();

    let mut builders = buckets
        .iter()
        .map(|(_, btype)| match btype {
            BucketType::Single => {
                builder_with_capacity(&DType::Bool(Nullability::NonNullable).into(), 128)
            }
            BucketType::Multi => builder_with_capacity(
                &DType::List(
                    DType::Utf8(Nullability::NonNullable).into(),
                    Nullability::NonNullable,
                )
                .into(),
                128,
            ),
        })
        .collect::<Vec<_>>();
    let mut entries_to_append: Vec<Vec<String>> = buckets.iter().map(|_| Vec::new()).collect();
    let mut doc_count = 0;
    for document in crate::common::documents() {
        // Group the tokens by the bucket that they will be appended to.
        for token in document {
            let idx = match buckets
                .binary_search_by_key(&(&token, &BucketType::Single), |(token, btype)| {
                    (token, btype)
                }) {
                Ok(idx) => idx,
                Err(idx) if idx == 0 => 0,
                Err(idx) => idx - 1,
            };
            entries_to_append[idx].push(token);
        }
        // Drain all buckets into the builders. Many of them will be empty, and that is ok.
        for (idx, entries) in entries_to_append.iter_mut().enumerate() {
            match buckets[idx].1 {
                BucketType::Single => {
                    let set = !entries.is_empty();
                    builders[idx].append_scalar(&set.into())?;
                    entries.clear();
                }
                BucketType::Multi => {
                    builders[idx].append_scalar(&entries.drain(..).collect::<Vec<_>>().into())?
                }
            }
        }
        doc_count += 1;
    }

    let st = StructArray::try_new(
        buckets
            .into_iter()
            .map(|(t, btype)| btype.column_name(t).into())
            .collect(),
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
