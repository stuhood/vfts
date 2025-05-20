use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use async_stream::stream;
use futures_util::{StreamExt, future};
use tokio::fs::OpenOptions;
use tokio::runtime::Handle;

use vortex_array::arrays::StructArray;
use vortex_array::builders::{ArrayBuilderExt, builder_with_capacity};
use vortex_array::stream::{ArrayStream, ArrayStreamAdapter};
use vortex_array::validity::Validity;
use vortex_array::{Array, IntoArray};
use vortex_dtype::{DType, Nullability, PType, StructDType};
use vortex_expr::ExprRef;
use vortex_file::{VortexFile, VortexOpenOptions, VortexWriteOptions};
use vortex_io::TokioFile;

use crate::vortex_list_expr::ListContainsExpr;

const ID_COLUMN: &str = "::id::";

const CHUNK_SIZE: usize = 8192;

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

#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
#[repr(u8)]
enum BucketType {
    // NB: `Single` must sort first, since we always attempt our binary searches with an exact
    // match.
    Single = 0,
    Multi = 1,
}

impl BucketType {
    fn column_name(&self, token: &str) -> String {
        format!("{token}:{}", (*self) as u8)
    }
}

pub async fn vortex_index(path: &Path, doc_count: usize, buckets: u16) -> anyhow::Result<()> {
    let document_stream = document_array_stream(doc_count, buckets).await?;
    vortex_index_array(path, document_stream).await?;
    println!(">>> created {path:?}, with up to {buckets} buckets");
    Ok(())
}

async fn document_array_stream(
    doc_count: usize,
    buckets: u16,
) -> anyhow::Result<impl ArrayStream + Unpin> {
    let buckets = select_buckets_from(
        crate::common::documents(1000)
            .map(|(_, doc)| doc.into_iter())
            .flatten()
            .collect(),
        buckets,
    );

    // Construct the `DType` for the `StructArray` that we will be emitting.
    // There is one prefixed `ID_COLUMN`, followed by one column per bucket. The Vortex DType of
    // each bucket is decided by its `BucketType`.
    let column_dtypes: Vec<DType> =
        std::iter::once(DType::Primitive(PType::U64, Nullability::NonNullable).into())
            .chain(buckets.iter().map(|(_, btype)| {
                match btype {
                    BucketType::Single => DType::Bool(Nullability::NonNullable).into(),
                    BucketType::Multi => DType::List(
                        DType::Utf8(Nullability::NonNullable).into(),
                        Nullability::NonNullable,
                    )
                    .into(),
                }
            }))
            .collect();
    let struct_dtype = StructDType::new(
        std::iter::once(ID_COLUMN.into())
            .chain(buckets.iter().map(|(t, btype)| btype.column_name(t).into()))
            .collect(),
        column_dtypes.clone(),
    );
    let dtype = DType::Struct(struct_dtype.clone().into(), Nullability::NonNullable);

    // Create a stream that emits batches of documents as StructArrays.
    let stream = stream! {
        let mut entries_to_append: Vec<Vec<String>> = buckets.iter().map(|_| Vec::new()).collect();
        let mut documents = crate::common::documents(doc_count);
        let mut might_have_more_docs = true;
        while might_have_more_docs {
            let mut builders = column_dtypes
                .iter()
                .map(|dtype| builder_with_capacity(dtype.into(), 1024))
                .collect::<Vec<_>>();
            let mut doc_count = 0;
            while doc_count < CHUNK_SIZE {
                let Some((id, document)) = documents.next() else {
                    // There are no more documents. Finish flushing the current chunk, and then
                    // complete the stream.
                    might_have_more_docs = false;
                    break;
                };
                builders[0].append_scalar(&id.into())?;
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
                            builders[idx + 1].append_scalar(&set.into())?;
                            entries.clear();
                        }
                        BucketType::Multi => builders[idx + 1]
                            .append_scalar(&entries.drain(..).collect::<Vec<_>>().into())?,
                    }
                }
                doc_count += 1;
            }

            let fields = builders.into_iter().map(|mut b| b.finish()).collect();

            yield Ok(StructArray::try_new_with_dtype(
                fields,
                struct_dtype.clone().into(),
                doc_count,
                Validity::NonNullable,
            )?
            .into_array());
        }
    };

    Ok(ArrayStreamAdapter::new(dtype, stream.boxed()))
}

async fn vortex_index_array(
    path: &Path,
    array_stream: impl ArrayStream + Unpin,
) -> anyhow::Result<()> {
    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&path)
        .await?;

    VortexWriteOptions::default().write(f, array_stream).await?;

    Ok(())
}

pub async fn vortex_search(path: &Path, query: &str) -> anyhow::Result<()> {
    let (file, dtype) = vortex_file(path).await?;

    let filter = create_filter(&dtype, crate::common::tokenize(query));

    let counts = future::try_join_all(
        file.scan()?
            .with_filter(filter)
            .with_projection(vortex_expr::lit(true))
            .map(|array| Ok(array.len()))
            .build()?,
    )
    .await?;

    let count = counts.into_iter().map(|c| c.unwrap_or(0)).sum::<usize>();
    println!(">>> {count}");

    Ok(())
}

pub async fn vortex_search_many(path: &Path, queries: usize) -> anyhow::Result<()> {
    let (file, dtype) = vortex_file(path).await?;

    let mut matches = 0;
    for (_, doc) in crate::common::documents(queries) {
        let filter = create_filter(&dtype, doc);

        let counts = future::try_join_all(
            file.scan()?
                .with_filter(filter)
                .with_projection(vortex_expr::lit(true))
                .with_tokio_executor(Handle::current())
                .map(|array| Ok(array.len()))
                .build()?,
        )
        .await?;
        matches += counts.into_iter().map(|c| c.unwrap_or(0)).sum::<usize>();
    }

    println!(">>> {queries} queries matched {matches} docs");
    Ok(())
}

///
/// Binary search on field names to find the bins that we'll be scanning in, and create a filter.
///
fn create_filter(dtype: &Arc<StructDType>, tokens: HashSet<String>) -> ExprRef {
    tokens
        .into_iter()
        .map(|token| {
            let needle: Arc<str> = BucketType::Single.column_name(&token).into();
            let result = dtype.names().binary_search(&needle);
            let (idx, btype) = match result {
                Ok(idx) => (idx, BucketType::Single),
                Err(idx) if idx < 1 => {
                    // NB: Our ID_COLUMN is the first field, so an insertion position of `1`
                    // matches our first bucket.
                    (1, BucketType::Multi)
                }
                Err(idx) => (idx - 1, BucketType::Multi),
            };

            let get_item = vortex_expr::get_item(dtype.names()[idx].clone(), vortex_expr::ident());
            match btype {
                BucketType::Single => get_item,
                BucketType::Multi => ListContainsExpr::new_expr(get_item, token.into()),
            }
        })
        .reduce(vortex_expr::and)
        .unwrap_or_else(|| vortex_expr::lit(false))
}

async fn vortex_file(path: &Path) -> anyhow::Result<(VortexFile, Arc<StructDType>)> {
    let file = VortexOpenOptions::file()
        .open_read_at(TokioFile::open(path)?)
        .await?;

    let dtype = file
        .dtype()
        .as_struct()
        .ok_or_else(|| anyhow!("Does not appear to be an index!"))?
        .clone();

    Ok((file, dtype))
}
