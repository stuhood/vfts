use std::path::Path;

use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::tokenizer::SimpleTokenizer;
use tantivy::{Index, IndexWriter};

fn schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("id", NumericOptions::default().set_stored());
    schema_builder.add_text_field(
        "body",
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("simple")
                .set_index_option(IndexRecordOption::Basic),
        ),
    );
    schema_builder.build()
}

pub fn tantivy_index(path: &Path) -> tantivy::Result<()> {
    let schema = schema();
    let index = Index::create_in_dir(path, schema.clone())?;
    index
        .tokenizers()
        .register("simple", SimpleTokenizer::default());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    let id_field = schema.get_field("id").unwrap();
    let body_field = schema.get_field("body").unwrap();
    for (id, document) in crate::common::documents() {
        let mut doc = TantivyDocument::default();
        doc.add_u64(id_field, id);
        doc.add_text(
            body_field,
            document.into_iter().collect::<Vec<_>>().join(" "),
        );
        index_writer.add_document(doc)?;
    }

    index_writer.commit()?;
    Ok(())
}

pub fn tantivy_search(path: &Path, query: &str) -> tantivy::Result<()> {
    let mut index = Index::open_in_dir(path)?;
    index.set_default_multithread_executor()?;
    index
        .tokenizers()
        .register("simple", SimpleTokenizer::default());

    let reader = index.reader_builder().try_into()?;
    let searcher = reader.searcher();

    let body_field = schema().get_field("body").unwrap();
    let query_parser = QueryParser::for_index(&index, vec![body_field]);
    let query = query_parser.parse_query(query)?;

    let count = searcher.search(&query, &Count)?;

    println!(">>> {count}");
    Ok(())
}
