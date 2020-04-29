use std::collections::HashSet;

pub type Term = String;
pub type DocumentId = u64;
pub type OffsetIndex = u32;

#[derive(Clone)]
pub struct TermDocumentEntry {
    document: DocumentId,
    offsets: Vec<OffsetIndex>
}

impl TermDocumentEntry {
    pub fn new(document: DocumentId) -> TermDocumentEntry {
        TermDocumentEntry {
            document,
            offsets: Vec::new()
        }
    }

    pub fn document(&self) -> DocumentId {
        self.document
    }

    pub fn offsets(&self) -> &Vec<OffsetIndex> {
        &self.offsets
    }

    pub fn add_offset(&mut self, offset: OffsetIndex) {
        self.offsets.push(offset);
    }
}

#[derive(Clone)]
pub struct TermDocuments {
    documents: Vec<TermDocumentEntry>
}

impl TermDocuments {
    pub fn new() -> TermDocuments {
        TermDocuments {
            documents: Vec::new()
        }
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn push(&mut self, entry: TermDocumentEntry) {
        self.documents.push(entry);
    }

    pub fn iter(&self) -> impl Iterator<Item=&TermDocumentEntry> {
        self.documents.iter()
    }

    pub fn documents(&self) -> &Vec<TermDocumentEntry> {
        &self.documents
    }

    pub fn filter(&mut self, valid_document_ids: &HashSet<DocumentId>) {
        self.documents.retain(|document| valid_document_ids.contains(&document.document()));
    }
}