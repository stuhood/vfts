use std::path::Path;

use tantivy::schema::*;
use tantivy::{Index, IndexWriter};

pub fn tantivy_index(path: &Path) -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_dir(path, schema.clone())?;
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    let body = schema.get_field("body").unwrap();
    for document in crate::common::documents() {
        let mut old_man_doc = TantivyDocument::default();
        old_man_doc.add_text(body, document.into_iter().collect::<Vec<_>>().join(" "));
        index_writer.add_document(old_man_doc)?;
    }

    index_writer.commit()?;
    Ok(())
}
