use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

use crate::term::{DocumentId, TermDocuments};
use crate::tokenizer::Tokens;
use crate::indexer::{Indexer, create_test_index};
use crate::document::{Document, create_test_document_storage};

pub struct Searcher {

}

impl Searcher {
    pub fn new() -> Searcher {
        Searcher {

        }
    }

    fn intersection_search_documents_internal_two(&self, first: &Vec<DocumentId>, second: &Vec<DocumentId>) -> Vec<DocumentId> {
        let mut results = Vec::new();

        let mut first_index = 0;
        let mut second_index = 0;
        while first_index < first.len() && second_index < second.len() {
            if first[first_index] == second[second_index] {
                results.push(first[first_index]);
                first_index += 1;
                second_index += 1;
            } else if first[first_index] < second[second_index] {
                first_index += 1;
            } else {
                second_index += 1;
            }
        }

        return results;
    }

    pub fn intersection_search_documents_internal(&self, query_term_documents: &Vec<TermDocuments>) -> Vec<DocumentId> {
        let mut query_terms: Vec<Vec<DocumentId>> = query_term_documents
            .iter()
            .map(|term_documents| term_documents.iter().map(|entry| entry.document()).collect())
            .collect();

        let mut results = query_terms.remove(0);
        while query_terms.len() > 0 && results.len() > 0 {
            let current_query_term = query_terms.remove(0);
            let new_results = self.intersection_search_documents_internal_two(&results, &current_query_term);
            results = new_results;
        }

        results
    }

    pub fn intersection_search_documents(&self, indexer: &Indexer, query: Tokens) -> Vec<DocumentId> {
        self.intersection_search_documents_internal(&query.iter().map(|token| indexer.get_from_index(token)).collect())
    }

    pub fn union_search(&self, indexer: &Indexer, query: Tokens) -> Vec<TermDocuments> {
        query.iter().map(|token| indexer.get_from_index(token)).collect()
    }

    pub fn intersection_search(&self, indexer: &Indexer, query: Tokens) -> Vec<TermDocuments> {
        let mut query_term_documents = self.union_search(indexer, query);
        let valid_documents = HashSet::from_iter(self.intersection_search_documents_internal(&query_term_documents).into_iter());

        for term_documents in &mut query_term_documents {
            term_documents.filter(&valid_documents);
        }

        query_term_documents
    }
}


#[test]
fn test_intersection_search1() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));

    let searcher = Searcher::new();
    let results = searcher.intersection_search_documents(&indexer, vec!["A".to_owned(), "B".to_owned()]);
    assert_eq!(2, results.len());
    assert_eq!(0, results[0]);
    assert_eq!(1, results[1]);
}

#[test]
fn test_intersection_search2() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));

    let searcher = Searcher::new();
    let results = searcher.intersection_search_documents(&indexer, vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]);
    assert_eq!(1, results.len());
    assert_eq!(0, results[0]);
}

#[test]
fn test_intersection_search3() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));

    let searcher = Searcher::new();
    let results = searcher.intersection_search_documents(&indexer, vec!["A".to_owned()]);
    assert_eq!(3, results.len());
    assert_eq!(0, results[0]);
    assert_eq!(1, results[1]);
    assert_eq!(2, results[2]);
}

#[test]
fn test_intersection_search4() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));

    let searcher = Searcher::new();
    let results = searcher.intersection_search_documents(&indexer, vec!["C".to_owned()]);
    assert_eq!(1, results.len());
    assert_eq!(0, results[0]);
}

#[test]
fn test_intersection_search5() {
    let mut indexer = Indexer::new(create_test_index(), create_test_document_storage());
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["D".to_owned(), "B".to_owned(), "C".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["A".to_owned()]));
    indexer.add_document(Document::new("".to_owned(), vec!["D".to_owned(), "B".to_owned(), "C".to_owned()]));

    let searcher = Searcher::new();
    let results = searcher.intersection_search_documents(&indexer, vec!["C".to_owned()]);
    assert_eq!(3, results.len());
    assert_eq!(2, results[0]);
    assert_eq!(3, results[1]);
    assert_eq!(6, results[2]);
}