use std::collections::HashMap;
use std::path::Path;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{Index, IndexWriter};

fn schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("id", NumericOptions::default().set_stored());
    schema_builder.add_text_field("body", TEXT);
    schema_builder.build()
}

pub fn tantivy_index(path: &Path) -> tantivy::Result<()> {
    let schema = schema();
    let index = Index::create_in_dir(path, schema.clone())?;
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
    let index = Index::open_in_dir(path)?;

    let reader = index.reader_builder().try_into()?;
    let searcher = reader.searcher();

    let body_field = schema().get_field("body").unwrap();
    let query_parser = QueryParser::for_index(&index, vec![body_field]);
    let query = query_parser.parse_query(query)?;

    let id_field = schema().get_field("id").unwrap();
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    for (score, doc) in top_docs {
        println!(
            ">>> {score:?}:\t{:?}",
            searcher
                .doc::<HashMap<_, _>>(doc)?
                .get(&id_field)
                .unwrap()
                .as_u64()
                .unwrap()
        );
    }
    Ok(())
}
