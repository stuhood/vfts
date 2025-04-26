use std::sync::Arc;

use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use tempfile::tempdir;
use tokio::fs::OpenOptions;
use vortex_array::arrays::{BooleanBuffer, ChunkedArray, StructArray, VarBinArray};
use vortex_array::stream::ArrayStreamArrayExt;
use vortex_array::validity::Validity;
use vortex_array::{Array, IntoArray};
use vortex_buffer::buffer;
use vortex_datafusion::persistent::VortexFormat;
use vortex_error::vortex_err;
use vortex_file::VortexWriteOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let key = ChunkedArray::from_iter([
        buffer![0u64, 1, 2, 3].into_array(),
        buffer![4u64, 5, 6, 7].into_array(),
    ])
    .into_array();

    let token = ChunkedArray::from_iter([
        VarBinArray::from(vec!["ab", "foo", "bar", "foo"]).into_array(),
        VarBinArray::from(vec!["biz", "boz", "boop", "blep"]).into_array(),
    ])
    .into_array();

    let bit = ChunkedArray::from_iter([
        BooleanBuffer::from(vec![true, false, true, true]).into_array(),
        BooleanBuffer::from(vec![true, false, true, false]).into_array(),
    ])
    .into_array();

    let st = StructArray::try_new(
        ["key".into(), "token".into(), "bit".into()].into(),
        vec![key, token, bit],
        8,
        Validity::NonNullable,
    )?;

    let filepath = temp_dir.path().join("a.vortex");

    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&filepath)
        .await?;

    VortexWriteOptions::default()
        .write(f, st.to_array_stream())
        .await?;

    let ctx: SessionContext = SessionStateBuilder::new()
        .with_config(SessionConfig::new().set_bool("datafusion.catalog.information_schema", true))
        .build()
        .into();

    let format = Arc::new(VortexFormat::default());
    let table_url = ListingTableUrl::parse(
        filepath
            .to_str()
            .ok_or_else(|| vortex_err!("Path is not valid UTF-8"))?,
    )?;
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(ListingOptions::new(format))
        .infer_schema(&ctx.state())
        .await?;

    let listing_table = Arc::new(ListingTable::try_new(config)?);

    ctx.register_table("vortex_tbl", listing_table)?;

    run_query(&ctx, "SELECT * FROM vortex_tbl WHERE bit = true AND token = 'foo'").await?;

    Ok(())
}

async fn run_query(ctx: &SessionContext, query_string: impl AsRef<str>) -> anyhow::Result<()> {
    let query_string = query_string.as_ref();

    ctx.sql(&format!("EXPLAIN ANALYZE {query_string}"))
        .await?
        .show()
        .await?;

    ctx.sql(query_string).await?.show().await?;

    Ok(())
}
