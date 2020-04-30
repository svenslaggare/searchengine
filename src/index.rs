use std::collections::HashMap;

use crate::term::{Term, TermDocumentEntry, TermDocuments};

pub trait Index {
    fn num_terms(&self) -> usize;
    fn print_stats(&self) {}

    fn add(&mut self, term: &Term, entry: TermDocumentEntry);
    fn read_documents(&self, term: &Term) -> TermDocuments;

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=(Term, TermDocuments)> + 'a>;
}

pub struct HashMapIndex {
    index: HashMap<Term, TermDocuments>,
}

impl HashMapIndex {
    pub fn new() -> HashMapIndex {
        HashMapIndex {
            index: HashMap::new()
        }
    }
}

impl Index for HashMapIndex {
    fn num_terms(&self) -> usize {
        self.index.len()
    }

    fn add(&mut self, term: &Term, entry: TermDocumentEntry) {
        self.index.entry(term.clone()).or_insert_with(|| TermDocuments::new()).push(entry);
    }

    fn read_documents(&self, term: &Term) -> TermDocuments {
        self.index[term].clone()
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=(Term, TermDocuments)> + 'a> {
        Box::new(self.index.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>().into_iter())
    }
}