use std::path::Path;

use tantivy::collector::Count;
use tantivy::query::{BooleanQuery, Query, QueryParser, TermQuery};
use tantivy::schema::*;
use tantivy::tokenizer::SimpleTokenizer;
use tantivy::{Index, IndexWriter, Searcher};

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

pub fn tantivy_index(path: &Path, doc_count: usize) -> tantivy::Result<()> {
    let schema = schema();
    let index = Index::create_in_dir(path, schema.clone())?;
    index
        .tokenizers()
        .register("simple", SimpleTokenizer::default());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    let id_field = schema.get_field("id").unwrap();
    let body_field = schema.get_field("body").unwrap();
    for (id, document) in crate::common::documents(doc_count) {
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
    let (searcher, index, body_field) = searcher(path)?;
    let query_parser = QueryParser::for_index(&index, vec![body_field]);
    let query = query_parser.parse_query(query)?;

    let count = searcher.search(&query, &Count)?;

    println!(">>> {count}");
    Ok(())
}

pub fn tantivy_search_many(path: &Path, queries: usize) -> tantivy::Result<()> {
    let (searcher, _, body_field) = searcher(path)?;

    let mut matches = 0;
    for (_, doc) in crate::common::documents(queries) {
        let query = BooleanQuery::intersection(
            doc.into_iter()
                .map(|term| -> Box<dyn Query> {
                    Box::new(TermQuery::new(
                        Term::from_field_text(body_field, &term),
                        IndexRecordOption::Basic,
                    ))
                })
                .collect(),
        );
        matches += searcher.search(&query, &Count)?;
    }

    println!(">>> {queries} queries matched {matches} docs");
    Ok(())
}

fn searcher(path: &Path) -> tantivy::Result<(Searcher, Index, Field)> {
    let mut index = Index::open_in_dir(path)?;
    index.set_default_multithread_executor()?;
    index
        .tokenizers()
        .register("simple", SimpleTokenizer::default());

    let reader = index.reader_builder().try_into()?;
    let searcher = reader.searcher();

    let body_field = schema().get_field("body").unwrap();
    Ok((searcher, index, body_field))
}
