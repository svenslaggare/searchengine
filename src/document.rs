use std::fs::File;
use std::io::{BufWriter, Write, BufRead, BufReader, Seek, Read, SeekFrom};
use std::ffi::OsStr;
use std::cell::RefCell;

use crate::tokenizer::Tokens;
use crate::term::DocumentId;


pub struct Document {
    title: String,
    tokens: Tokens
}

impl Document {
    pub fn new(title: String, tokens: Tokens) -> Document {
        Document {
            title,
            tokens
        }
    }

    pub fn tokens(&self) -> &Tokens {
        &self.tokens
    }
}

pub trait DocumentStorage {
    fn num_documents(&self) -> usize;
    fn store(&mut self, id: DocumentId, document: &Document);
    fn get_length(&self, id: DocumentId) -> usize;
    fn get_title(&self, id: DocumentId) -> String;
}

pub struct DocumentFileStorage {
    write_file: File,
    read_file: RefCell<File>,
    last_document_id: Option<DocumentId>
}

const MAX_TITLE_LENGTH: usize = 100;
const ENTRY_SIZE: usize = std::mem::size_of::<u64>() + MAX_TITLE_LENGTH + 1;

impl DocumentFileStorage {
    pub fn new(filename: &str) -> DocumentFileStorage {
        let mut write_file = File::create(filename).unwrap();
        let read_file = RefCell::new(File::open(filename).unwrap());
        write_file.write_all(&(0 as u64).to_le_bytes()).unwrap();

        DocumentFileStorage {
            write_file,
            read_file,
            last_document_id: None
        }
    }

    pub fn from_existing(filename: &str) -> DocumentFileStorage {
        let write_file = std::fs::OpenOptions::new()
            .write(true)
            .open(filename).unwrap();

        let read_file = RefCell::new(File::open(filename).unwrap());

        let mut storage = DocumentFileStorage {
            write_file,
            read_file,
            last_document_id: None
        };

        let num_documents = storage.num_documents();
        if num_documents > 0 {
            storage.last_document_id = Some(num_documents as u64 - 1);
        }

        storage
    }

    fn entry_offset(id: DocumentId) -> u64 {
        id * ENTRY_SIZE as u64 + std::mem::size_of::<u64>() as u64
    }
}

impl DocumentStorage for DocumentFileStorage {
    fn num_documents(&self) -> usize {
        let mut read_file = self.read_file.borrow_mut();
        read_file.seek(SeekFrom::Start(0)).unwrap();
        let mut buffer: [u8; 8] = [0; 8];
        read_file.read_exact(&mut buffer[..]).unwrap();
        u64::from_le_bytes(buffer) as usize
    }

    fn store(&mut self, id: DocumentId, document: &Document) {
        if self.last_document_id.is_some() {
            assert_eq!(id, self.last_document_id.unwrap() + 1);
        } else {
            assert_eq!(id, 0);
        }

        self.write_file.write_all(&(document.tokens.len() as u64).to_le_bytes()).unwrap();

        let mut chars = document.title.bytes().take(MAX_TITLE_LENGTH).collect::<Vec<u8>>();
        self.write_file.write_all(&(chars.len() as u8).to_le_bytes()).unwrap();
        chars.resize(MAX_TITLE_LENGTH, 0);
        self.write_file.write_all(&chars[..]).unwrap();

        let current_offset = self.write_file.seek(SeekFrom::End(0)).unwrap();
        self.write_file.seek(SeekFrom::Start(0)).unwrap();
        self.write_file.write_all(&(id + 1).to_le_bytes()).unwrap();
        self.write_file.seek(SeekFrom::Start(current_offset)).unwrap();

        self.last_document_id = Some(id);
    }

    fn get_length(&self, id: DocumentId) -> usize {
        let mut read_file = self.read_file.borrow_mut();
        read_file.seek(SeekFrom::Start(DocumentFileStorage::entry_offset(id))).unwrap();

        let mut buffer: [u8; 8] = [0; 8];
        read_file.read_exact(&mut buffer[..]).unwrap();
        u64::from_le_bytes(buffer) as usize
    }

    fn get_title(&self, id: u64) -> String {
        let mut read_file = self.read_file.borrow_mut();
        read_file.seek(SeekFrom::Start(DocumentFileStorage::entry_offset(id) + std::mem::size_of::<u64>() as u64)).unwrap();

        let mut buffer: [u8; 1] = [0; 1];
        read_file.read_exact(&mut buffer).unwrap();
        let title_length = u8::from_le_bytes(buffer) as usize;

        let mut buffer = vec![0; title_length];
        read_file.read_exact(&mut buffer[..]).unwrap();
        String::from_utf8(buffer).unwrap_or_else(|_| "unknown".to_owned())
    }
}

fn get_tempfile_name() -> String {
    let named_tempfile = tempfile::Builder::new()
        .suffix(".documents")
        .tempfile().unwrap();

    let filename = named_tempfile
        .path()
        .file_name().and_then(OsStr::to_str).unwrap().to_owned();

    "/tmp/".to_owned() + &filename
}

pub fn create_test_document_storage() -> Box<DocumentFileStorage> {
    Box::new(DocumentFileStorage::new(&get_tempfile_name()))
}

#[test]
fn test_file_storage1() {
    let mut document_storage = create_test_document_storage();
    document_storage.store(0, &Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    document_storage.store(1, &Document::new("".to_owned(), vec!["A".to_owned()]));
    document_storage.store(2, &Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned(), "D".to_owned()]));

    assert_eq!(document_storage.get_length(0), 2);
    assert_eq!(document_storage.get_length(1), 1);
    assert_eq!(document_storage.get_length(2), 4);

    assert_eq!(document_storage.get_title(0), "".to_owned());
    assert_eq!(document_storage.get_title(1), "".to_owned());
    assert_eq!(document_storage.get_title(2), "".to_owned());

    assert_eq!(document_storage.num_documents(), 3);
    assert_eq!(document_storage.last_document_id, Some(2));
}

#[test]
fn test_file_storage2() {
    let mut document_storage = create_test_document_storage();
    document_storage.store(0, &Document::new("Wololo".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    document_storage.store(1, &Document::new("Haha".to_owned(), vec!["A".to_owned()]));
    document_storage.store(2, &Document::new("hihi".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned(), "D".to_owned()]));

    assert_eq!(document_storage.get_length(0), 2);
    assert_eq!(document_storage.get_length(1), 1);
    assert_eq!(document_storage.get_length(2), 4);

    assert_eq!(document_storage.get_title(0), "Wololo".to_owned());
    assert_eq!(document_storage.get_title(1), "Haha".to_owned());
    assert_eq!(document_storage.get_title(2), "hihi".to_owned());

    assert_eq!(document_storage.num_documents(), 3);
    assert_eq!(document_storage.last_document_id, Some(2));
}

#[test]
fn test_file_storage3() {
    let filename = get_tempfile_name();
    let mut document_storage = DocumentFileStorage::new(&filename);
    document_storage.store(0, &Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned()]));
    document_storage.store(1, &Document::new("".to_owned(), vec!["A".to_owned()]));
    document_storage.store(2, &Document::new("".to_owned(), vec!["A".to_owned(), "B".to_owned(), "C".to_owned(), "D".to_owned()]));

    assert_eq!(document_storage.get_length(0), 2);
    assert_eq!(document_storage.get_length(1), 1);
    assert_eq!(document_storage.get_length(2), 4);
    assert_eq!(document_storage.num_documents(), 3);
    assert_eq!(document_storage.last_document_id, Some(2));

    let mut document_storage2 = DocumentFileStorage::from_existing(&filename);
    assert_eq!(document_storage2.get_length(0), 2);
    assert_eq!(document_storage2.get_length(1), 1);
    assert_eq!(document_storage2.get_length(2), 4);
    assert_eq!(document_storage2.num_documents(), 3);
    assert_eq!(document_storage2.last_document_id, Some(2));
}