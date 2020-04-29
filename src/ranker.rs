use std::collections::{HashMap, BTreeMap};
use std::cmp::Ordering;

use crate::indexer::Indexer;
use crate::tokenizer::Tokens;
use crate::term::{DocumentId, TermDocuments};

pub struct Ranker {

}

impl Ranker {
    pub fn new() -> Ranker {
        Ranker {

        }
    }

    fn calculate_tf_idf(num_documents: f64, df: f64, tf: f64, length: f64) -> f64 {
        let mut tf = tf;
        let idf = (num_documents / df).log10();
        if tf > 0.0 {
            tf = 1.0 + tf.log10();
        }

        tf * idf / length.sqrt()
    }

    fn order_documents(document_scores: HashMap<DocumentId, f64>) -> Vec<(DocumentId, f64)> {
        let mut document_scores = document_scores.into_iter().collect::<Vec<_>>();
        document_scores.sort_by(|x, y| {
            if x.1 < y.1 {
                Ordering::Greater
            } else if x.1 > y.1 {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        });
        document_scores
    }

    pub fn rank(&self, indexer: &Indexer, query_term_documents: Vec<TermDocuments>) -> Vec<(DocumentId, f64)> {
        let num_documents = indexer.document_storage().num_documents() as f64;

        let mut document_scores = HashMap::new();
        for term_documents in &query_term_documents {
            let score_query = Ranker::calculate_tf_idf(
                num_documents,
                term_documents.len() as f64,
                1.0,
                query_term_documents.len() as f64
            );

            for term_entry in term_documents.iter() {
                let score_document = Ranker::calculate_tf_idf(
                    num_documents,
                    term_documents.len() as f64,
                    term_entry.offsets().len() as f64,
                    indexer.document_storage().get_length(term_entry.document()) as f64
                );

                let score = score_query * score_document;
                *document_scores.entry(term_entry.document()).or_insert(0.0) += score;
            }
        }

        Ranker::order_documents(document_scores)
    }
}