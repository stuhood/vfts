use std::sync::Arc;

use vortex_array::IntoArray;
use vortex_array::arrays::{ListArray, StructArray};
use vortex_array::validity::Validity;
use vortex_btrblocks::BtrBlocksCompressor;
use vortex_buffer::buffer;
use vortex_dtype::DType::Utf8;
use vortex_dtype::Nullability;

fn main() -> anyhow::Result<()> {
    let key = buffer![0u64, 1, 2, 3].into_array();

    // NB: u16 here is the max length of each token list.
    let tokens = ListArray::from_iter_slow::<u16, _>(
        vec![vec!["foo", "bar"], vec![], vec!["baz", "bar"], vec!["foo"]],
        Arc::new(Utf8(Nullability::NonNullable).into()),
    )
    .unwrap();

    let structs = StructArray::try_new(
        ["key".into(), "tokens".into()].into(),
        vec![key, tokens],
        4,
        Validity::NonNullable,
    )?;

    println!(">>> {structs:#?}");

    let compressed_structs = BtrBlocksCompressor::compress(&BtrBlocksCompressor, &structs)?;

    println!(">>> {compressed_structs:#?}");

    Ok(())
}
