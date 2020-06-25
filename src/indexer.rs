use std::collections::HashMap;
use std::iter::FromIterator;

use crate::tokenizer::{Tokens, Token};
use crate::term::{Term, DocumentId, TermDocuments, TermDocumentEntry, OffsetIndex};
use crate::index::{Index, HashMapIndex, IndexResult};
use crate::block_index::{BlockIndex, BlockIndexConfig};
use crate::document::{Document, DocumentStorage, create_test_document_storage};

pub struct Indexer {
    index: Box<dyn Index>,
    document_storage: Box<dyn DocumentStorage>,
    next_document_id: DocumentId
}

impl Indexer {
    pub fn new(index: Box<dyn Index>, document_storage: Box<dyn DocumentStorage>) -> Indexer {
        Indexer {
            index,
            document_storage,
            next_document_id: 0
        }
    }

    fn add_to_index(&mut self, term: &Term, entry: TermDocumentEntry) -> IndexResult<()> {
        self.index.add(term, entry)?;
        Ok(())
    }

    pub fn get_from_index(&self, term: &Term) -> IndexResult<TermDocuments> {
        // let t0 = std::time::Instant::now();
        let documents = self.index.read_documents(term)?;
        // println!("Search time: {} ms", (std::time::Instant::now() - t0).as_nanos() as f64 / 1E6);
        Ok(documents)
    }

    pub fn document_storage(&self) -> &Box<dyn DocumentStorage> {
        &self.document_storage
    }

    pub fn add_document(&mut self, document: Document) {
        let document_id = self.next_document_id;
        self.next_document_id += 1;

        self.document_storage.store(document_id, &document);

        let mut token_indices = (0..document.tokens().len()).collect::<Vec<_>>();
        token_indices.sort_by_key(|index| &document.tokens()[*index]);

        let mut prev_token_opt: Option<Token> = None;
        let mut term_entry = TermDocumentEntry::new(document_id);

        for token_index in token_indices {
            match prev_token_opt.as_ref() {
                Some(prev_token) => {
                    if prev_token != &document.tokens()[token_index] {
                        let mut current_term_entry = TermDocumentEntry::new(document_id);
                        std::mem::swap(&mut current_term_entry, &mut term_entry);
                        self.add_to_index(prev_token, current_term_entry).unwrap();
                        prev_token_opt = Some(document.tokens()[token_index].clone());
                    }

                    term_entry.add_offset(token_index as OffsetIndex);
                }
                None => {
                    term_entry.add_offset(token_index as OffsetIndex);
                    prev_token_opt = Some(document.tokens()[token_index].clone());
                }
            }
        }

        if let Some(prev_token) = prev_token_opt {
            self.add_to_index(&prev_token, term_entry).unwrap();
        }
    }

    pub fn print_stats(&self) {
        println!("Number of unique terms: {}", self.index.num_terms());
        println!("Number of documents: {}", self.next_document_id);
        self.index.print_stats();

        let mut term_document_counts = Vec::from_iter(self.index.iter().map(|(term, results)| (term.clone(), results.documents().len())));
        term_document_counts.sort_by_key(|(_, count)| -(*count as i64));

        println!();
        println!("Terms by number of documents: ");
        for i in 0..10 {
            println!("{}: {} (n: {})", i + 1, term_document_counts[i].0, term_document_counts[i].1);
        }
        println!();
    }
}

pub fn create_test_index() -> Box<dyn Index> {
    // Box::new(HashMapIndex::new())

    let mut index_config = BlockIndexConfig::default(None);
    Box::new(BlockIndex::new(index_config))
}

#[test]
fn test_build_index1() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));

    assert_eq!(3, indexer.index.num_terms());
    assert_eq!(3, indexer.get_from_index(&"A".to_owned()).unwrap().len());
    assert_eq!(2, indexer.get_from_index(&"B".to_owned()).unwrap().len());
    assert_eq!(1, indexer.get_from_index(&"C".to_owned()).unwrap().len());
}

#[test]
fn test_build_index2() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));

    assert_eq!(3, indexer.index.num_terms());
    assert_eq!(3, indexer.get_from_index(&"A".to_owned()).unwrap().len());
    assert_eq!(2, indexer.get_from_index(&"B".to_owned()).unwrap().len());
    assert_eq!(1, indexer.get_from_index(&"C".to_owned()).unwrap().len());
}

#[test]
fn test_build_index3() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));

    assert_eq!(3, indexer.index.num_terms());
    assert_eq!(3, indexer.get_from_index(&"A".to_owned()).unwrap().len());
    assert_eq!(2, indexer.get_from_index(&"B".to_owned()).unwrap().len());
    assert_eq!(1, indexer.get_from_index(&"C".to_owned()).unwrap().len());
}

#[test]
fn test_build_index4() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "A".to_owned(), "C".to_owned(), "B".to_owned()]));

    assert_eq!(3, indexer.index.num_terms());

    assert_eq!(1, indexer.get_from_index(&"A".to_owned()).unwrap().len());
    assert_eq!(1, indexer.get_from_index(&"A".to_owned()).unwrap().documents().len());
    assert_eq!(2, indexer.get_from_index(&"A".to_owned()).unwrap().documents()[0].offsets().len());

    assert_eq!(1, indexer.get_from_index(&"B".to_owned()).unwrap().len());
    assert_eq!(1, indexer.get_from_index(&"B".to_owned()).unwrap().documents().len());
    assert_eq!(2, indexer.get_from_index(&"B".to_owned()).unwrap().documents()[0].offsets().len());

    assert_eq!(1, indexer.get_from_index(&"C".to_owned()).unwrap().len());
    assert_eq!(1, indexer.get_from_index(&"C".to_owned()).unwrap().documents().len());
    assert_eq!(1, indexer.get_from_index(&"C".to_owned()).unwrap().documents()[0].offsets().len());
}

#[test]
fn test_build_index5() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));

    assert_eq!(2, indexer.index.num_terms());
    assert_eq!(2, indexer.get_from_index(&"A".to_owned()).unwrap().len());
    assert_eq!(1, indexer.get_from_index(&"B".to_owned()).unwrap().len());
}