use std::sync::Arc;

use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use tempfile::tempdir;
use tokio::fs::OpenOptions;
use vortex_array::arrays::{ListArray, StructArray};
use vortex_array::stream::ArrayStreamArrayExt;
use vortex_array::validity::Validity;
use vortex_array::IntoArray;
use vortex_buffer::buffer;
use vortex_datafusion::persistent::VortexFormat;
use vortex_dtype::DType::Utf8;
use vortex_dtype::Nullability;
use vortex_error::vortex_err;
use vortex_file::VortexWriteOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let key = buffer![0u64, 1, 2, 3].into_array();

    // NB: u16 here is the max length of each token list.
    let tokens = ListArray::from_iter_slow::<u16, _>(
            vec![vec!["foo", "bar"], vec![], vec!["baz", "bar"], vec!["foo"]],
            Arc::new(Utf8(Nullability::NonNullable).into()),
        )
        .unwrap();

    println!(">>> {tokens:#?}");

    let st = StructArray::try_new(
        ["key".into(), "tokens".into()].into(),
        vec![key, tokens],
        4,
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

    ctx.register_udf(datafusion::functions_array::make_array::MakeArray::new().into());

    ctx.register_table("vortex_tbl", listing_table)?;

    run_query(
        &ctx,
        "SELECT key FROM vortex_tbl WHERE tokens @> make_array('foo')",
    )
    .await?;

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
