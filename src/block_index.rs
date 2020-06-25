use std::collections::HashMap;
use std::io::{BufReader, Seek, Read, Write, BufWriter, SeekFrom};
use std::fs::File;
use std::cell::RefCell;
use std::ffi::OsStr;

use crate::term::{TermDocumentEntry, TermDocuments, Term};
use crate::index::{Index, IndexResult, IndexError};
use std::fmt::Formatter;

fn write_u32(storage: &mut Vec<u8>, offset: &mut usize, value: u32) {
    let bytes = value.to_le_bytes();
    let new_size = *offset + bytes.len();
    if *offset + bytes.len() >= storage.len() {
        storage.resize(new_size, 0);
    }

    for value in &bytes {
        storage[*offset] = *value;
        *offset += 1;
    }
}

fn write_u64(storage: &mut Vec<u8>, offset: &mut usize, value: u64) {
    let bytes = value.to_le_bytes();
    let new_size = *offset + bytes.len();
    if *offset + bytes.len() >= storage.len() {
        storage.resize(new_size, 0);
    }

    for value in &bytes {
        storage[*offset] = *value;
        *offset += 1;
    }
}

fn read_u32(storage: &Vec<u8>, offset: &mut usize) -> u32 {
    let mut buffer: [u8; 4] = [0; 4];
    for i in 0..buffer.len() {
        buffer[i] = storage[*offset];
        *offset += 1;
    }

    u32::from_le_bytes(buffer)
}

fn read_u64(storage: &Vec<u8>, offset: &mut usize) -> u64 {
    let mut buffer: [u8; 8] = [0; 8];
    for i in 0..buffer.len() {
        buffer[i] = storage[*offset];
        *offset += 1;
    }

    u64::from_le_bytes(buffer)
}

struct ByteBuffer {
    buffer: Vec<u8>,
    offset: usize
}

impl ByteBuffer {
    pub fn new() -> ByteBuffer {
        ByteBuffer {
            buffer: Vec::new(),
            offset: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn iter(&self) -> impl Iterator<Item=&u8> {
        self.buffer.iter()
    }

    pub fn write_u32(&mut self, value: u32) {
        write_u32(&mut self.buffer, &mut self.offset, value);
    }

    pub fn write_u64(&mut self, value: u64) {
        write_u64(&mut self.buffer, &mut self.offset, value);
    }
}

struct ReadTermResult {
    start: usize,
    size: usize,
    documents: TermDocuments
}

struct TermHeader {
    size: u64,
    num_documents: u64,
    next_document_start: u64
}

#[derive(Debug)]
enum BlockIndexError {
    GenericWriteFail { message: String, },
    IOWriteFail { err: std::io::Error, },
    GenericReadFail { message: String },
    IOReadFail { err: std::io::Error, },
    FileNotFound
}

impl std::error::Error for BlockIndexError {

}

impl std::fmt::Display for BlockIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockIndexError::GenericWriteFail { message } => {
                write!(f, "IO write fail: {}", message)
            },
            BlockIndexError::IOWriteFail { err } => {
                write!(f, "IO write fail: {}", err)
            },
            BlockIndexError::GenericReadFail { message } => {
                write!(f, "IO read fail: {}", message)
            },
            BlockIndexError::IOReadFail { err } => {
                write!(f, "IO read fail: {}", err)
            },
            BlockIndexError::FileNotFound => {
                write!(f, "File not found")
            },
        }
    }
}

impl From<BlockIndexError> for IndexError {
    fn from(err: BlockIndexError) -> Self {
        match err {
            err @ BlockIndexError::GenericWriteFail { .. } => {
                IndexError::WriteFail { inner: Box::new(err) }
            },
            err @ BlockIndexError::IOWriteFail { .. }  => {
                IndexError::WriteFail { inner: Box::new(err) }
            },
            err @ BlockIndexError::GenericReadFail { .. }  => {
                IndexError::ReadFail { inner: Box::new(err) }
            },
            err @ BlockIndexError::IOReadFail { .. }  => {
                IndexError::ReadFail { inner: Box::new(err) }
            },
            err @ BlockIndexError::FileNotFound => {
                IndexError::Other { inner: Box::new(err) }
            },
        }
    }
}

type BlockIndexResult<T> = Result<T, BlockIndexError>;

trait BlockIndexStorage {
    fn num_terms(&self) -> usize;

    fn read_u32(&self, offset: &mut usize) -> BlockIndexResult<u32>;
    fn read_u64(&self, offset: &mut usize) -> BlockIndexResult<u64>;
    fn read_bytes(&self, offset: usize, buffer: &mut [u8]) -> BlockIndexResult<()>;

    fn write_u64(&mut self, offset: &mut usize, value: u64) ->  BlockIndexResult<()>;
    fn write_bytes(&mut self, offset: usize, buffer: &ByteBuffer) -> BlockIndexResult<()>;

    fn allocate_block(&mut self, size: usize) -> (usize, usize);

    fn get_term_start_position(&self, term: &Term) -> usize;
    fn add_term_start_position(&mut self, term: &Term, position: usize);
    fn remove_term_start_position(&mut self, term: &Term);
    fn term_exists(&self, term: &Term) -> bool;
    fn terms_iter(&self) -> Box<dyn Iterator<Item=String>>;
}

struct BlockIndexMemoryStorage {
    storage: Vec<u8>,
    next_term_start: usize,
    term_start_positions: HashMap<Term, usize>,
}

impl BlockIndexMemoryStorage {
    fn new() -> BlockIndexMemoryStorage {
        BlockIndexMemoryStorage {
            storage: Vec::new(),
            next_term_start: 0,
            term_start_positions: HashMap::new()
        }
    }
}

impl BlockIndexStorage for BlockIndexMemoryStorage {
    fn num_terms(&self) -> usize {
        self.term_start_positions.len()
    }

    fn read_u32(&self, offset: &mut usize) -> BlockIndexResult<u32> {
        Ok(read_u32(&self.storage, offset))
    }

    fn read_u64(&self, offset: &mut usize) -> BlockIndexResult<u64> {
        Ok(read_u64(&self.storage, offset))
    }

    fn read_bytes(&self, offset: usize, buffer: &mut [u8]) -> BlockIndexResult<()> {
        for i in 0..buffer.len() {
            buffer[i] = self.storage[offset + i];
        }

        Ok(())
    }

    fn write_u64(&mut self, offset: &mut usize, value: u64) -> BlockIndexResult<()> {
        write_u64(&mut self.storage, offset, value);
        Ok(())
    }

    fn write_bytes(&mut self, offset: usize, buffer: &ByteBuffer) -> BlockIndexResult<()> {
        for (i, value) in buffer.iter().enumerate() {
            self.storage[offset + i] = *value;
        }

        Ok(())
    }

    fn allocate_block(&mut self, size: usize) -> (usize, usize) {
        let term_start = self.next_term_start;
        self.next_term_start += size;
        self.storage.resize(self.storage.len() + size, 0);
        (term_start, size)
    }

    fn get_term_start_position(&self, term: &Term) -> usize {
        self.term_start_positions[term]
    }

    fn add_term_start_position(&mut self, term: &Term, position: usize) {
        self.term_start_positions.insert(term.clone(), position);
    }

    fn remove_term_start_position(&mut self, term: &Term) {
        self.term_start_positions.remove(term);
    }

    fn term_exists(&self, term: &Term) -> bool {
        self.term_start_positions.contains_key(term)
    }

    fn terms_iter(&self) -> Box<dyn Iterator<Item=String>> {
        Box::new(self.term_start_positions.keys().cloned().collect::<Vec<_>>().into_iter())
    }
}

struct BlockIndexFileStorage {
    folder: String,
    term_start_positions: HashMap<Term, usize>,
    next_term_start: usize,
    file_index: RefCell<File>
}

impl BlockIndexFileStorage {
    fn new(folder: &str) -> BlockIndexFileStorage {
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&format!("{}/terms", folder)).unwrap();

        BlockIndexFileStorage {
            folder: folder.to_owned(),
            term_start_positions: HashMap::new(),
            next_term_start: 0,
            file_index: RefCell::new(std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .read(true)
                .open(&format!("{}/index", folder)).unwrap()),
        }
    }

    fn read_term_positions_from_file(file: File) -> BlockIndexResult<HashMap<Term, usize>> {
        let mut term_start_positions = HashMap::new();
        let mut buf_reader = BufReader::new(file);

        let mut buffer: [u8; 8] = [0; 8];
        buf_reader.read_exact(&mut buffer[..]).map_err(|err| BlockIndexError::IOReadFail { err })?;
        let num_terms = u64::from_le_bytes(buffer);

        for _ in 0..num_terms {
            let mut buffer: [u8; 8] = [0; 8];
            buf_reader.read_exact(&mut buffer[..]).map_err(|err| BlockIndexError::IOReadFail { err })?;
            let term_size = u64::from_le_bytes(buffer) as usize;

            let mut buffer = vec![0; term_size];
            buf_reader.read_exact(&mut buffer[..]).map_err(|err| BlockIndexError::IOReadFail { err })?;
            let term = String::from_utf8(buffer)
                .map_err(|err| BlockIndexError::GenericReadFail { message: format!("Failed to convert to string: {}", err) })?;

            let mut buffer: [u8; 8] = [0; 8];
            buf_reader.read_exact(&mut buffer[..]).map_err(|err| BlockIndexError::IOReadFail { err })?;
            let start_position = u64::from_le_bytes(buffer) as usize;

            term_start_positions.insert(term, start_position);
        }

        Ok(term_start_positions)
    }

    fn from_existing(folder: &str) -> BlockIndexResult<BlockIndexFileStorage> {
        let mut file_index = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(&format!("{}/index", folder))
            .map_err(|_| BlockIndexError::FileNotFound)?;

        let next_term_start = file_index
            .seek(SeekFrom::End(0)).map_err(|err| BlockIndexError::IOReadFail { err })? as usize;

        let term_position_file = File::open(&format!("{}/terms", folder)).map_err(|_| BlockIndexError::FileNotFound)?;

        Ok(
            BlockIndexFileStorage {
                folder: folder.to_owned(),
                term_start_positions: BlockIndexFileStorage::read_term_positions_from_file(term_position_file)?,
                next_term_start,
                file_index: RefCell::new(file_index),
            }
        )
    }

    fn write_term_positions_to_file(&mut self) -> BlockIndexResult<()> {
        let mut buf_writer = BufWriter::new(
            std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&format!("{}/terms", self.folder))
                .map_err(|_| BlockIndexError::FileNotFound)?
        );

        buf_writer.write_all(&self.term_start_positions.len().to_le_bytes()).map_err(|err| BlockIndexError::IOWriteFail { err })?;

        for (term, start_position) in &self.term_start_positions {
            let bytes = term.as_bytes();
            buf_writer.write_all(&(bytes.len() as u64).to_le_bytes()).map_err(|err| BlockIndexError::IOWriteFail { err })?;
            buf_writer.write_all(bytes).map_err(|err| BlockIndexError::IOWriteFail { err })?;
            buf_writer.write_all(&(*start_position as u64).to_le_bytes()).map_err(|err| BlockIndexError::IOWriteFail { err })?;
        }

        Ok(())
    }
}

impl Drop for BlockIndexFileStorage {
    fn drop(&mut self) {
        self.write_term_positions_to_file().unwrap();
    }
}

impl BlockIndexStorage for BlockIndexFileStorage {
    fn num_terms(&self) -> usize {
        self.term_start_positions.len()
    }

    fn read_u32(&self, offset: &mut usize) -> BlockIndexResult<u32> {
        let mut buffer: [u8; 4] = [0; 4];
        let mut file = self.file_index.borrow_mut();
        file.seek(SeekFrom::Start(*offset as u64)).map_err(|err| BlockIndexError::IOReadFail { err })?;
        file.read_exact(&mut buffer[..]).map_err(|err| BlockIndexError::IOReadFail { err })?;
        *offset += buffer.len();
        Ok(u32::from_le_bytes(buffer))
    }

    fn read_u64(&self, offset: &mut usize) -> BlockIndexResult<u64> {
        let mut buffer: [u8; 8] = [0; 8];
        let mut file = self.file_index.borrow_mut();
        file.seek(SeekFrom::Start(*offset as u64)).map_err(|err| BlockIndexError::IOReadFail { err })?;
        file.read_exact(&mut buffer[..]).map_err(|err| BlockIndexError::IOReadFail { err })?;
        *offset += buffer.len();
        Ok(u64::from_le_bytes(buffer))
    }

    fn read_bytes(&self, offset: usize, buffer: &mut [u8]) -> BlockIndexResult<()> {
        let mut file = self.file_index.borrow_mut();
        file.seek(SeekFrom::Start(offset as u64)).map_err(|err| BlockIndexError::IOReadFail { err })?;
        file.read_exact(buffer).map_err(|err| BlockIndexError::IOReadFail { err })?;
        Ok(())
    }

    fn write_u64(&mut self, offset: &mut usize, value: u64) -> BlockIndexResult<()> {
        let buffer = value.to_le_bytes();
        let mut file = self.file_index.borrow_mut();
        file.seek(SeekFrom::Start(*offset as u64)).map_err(|err| BlockIndexError::IOWriteFail { err })?;
        file.write_all(&buffer[..]).map_err(|err| BlockIndexError::IOWriteFail { err })?;
        *offset += buffer.len();
        Ok(())
    }

    fn write_bytes(&mut self, offset: usize, buffer: &ByteBuffer) -> BlockIndexResult<()> {
        let mut file = self.file_index.borrow_mut();
        file.seek(SeekFrom::Start(offset as u64)).map_err(|err| BlockIndexError::IOWriteFail { err })?;
        file.write_all(&buffer.buffer[..]).map_err(|err| BlockIndexError::IOWriteFail { err })?;
        Ok(())
    }

    fn allocate_block(&mut self, size: usize) -> (usize, usize) {
        let term_start = self.next_term_start;
        self.next_term_start += size;
        (term_start, size)
    }

    fn get_term_start_position(&self, term: &Term) -> usize {
        self.term_start_positions[term]
    }

    fn add_term_start_position(&mut self, term: &Term, position: usize) {
        self.term_start_positions.insert(term.clone(), position);
    }

    fn remove_term_start_position(&mut self, term: &Term) {
        self.term_start_positions.remove(term);
    }

    fn term_exists(&self, term: &Term) -> bool {
        self.term_start_positions.contains_key(term)
    }

    fn terms_iter(&self) -> Box<dyn Iterator<Item=String>> {
        Box::new(self.term_start_positions.keys().cloned().collect::<Vec<_>>().into_iter())
    }
}

pub struct BlockIndexConfig {
    pub index_folder: Option<String>,
    pub use_existing: bool,
    pub block_size: usize,
    pub reuse_blocks: bool,
    pub use_memory_based: bool
}

impl BlockIndexConfig {
    pub fn default(index_folder: Option<String>) -> BlockIndexConfig {
        BlockIndexConfig {
            index_folder,
            use_existing: false,
            block_size: 256,
            reuse_blocks: true,
            use_memory_based: false
        }
    }

    pub fn use_existing(index_folder: Option<String>) -> BlockIndexConfig {
        let mut config = BlockIndexConfig::default(index_folder);
        config.use_existing = true;
        config
    }

    fn testing() -> BlockIndexConfig {
        BlockIndexConfig {
            index_folder: None,
            use_existing: false,
            block_size: 256,
            reuse_blocks: false,
            use_memory_based: false
        }
    }
}

fn get_tempfile_name() -> String {
    let named_tempfile = tempfile::Builder::new()
        .suffix(".index")
        .tempfile().unwrap();

    let filename = named_tempfile
        .path()
        .file_name().and_then(OsStr::to_str).unwrap().to_owned();

    "/tmp/".to_owned() + &filename
}

pub struct BlockIndex {
    config: BlockIndexConfig,
    storage: Box<dyn BlockIndexStorage>,
    deleted_blocks: Vec<(usize, usize)>,
    num_reallocations: usize,
    num_reused_blocks: usize
}

impl BlockIndex {
    pub fn new(config: BlockIndexConfig) -> BlockIndex {
        let index_folder = config.index_folder.clone().unwrap_or_else(|| get_tempfile_name());
        let use_memory_based = config.use_memory_based;

        let storage: Box<dyn BlockIndexStorage> = if use_memory_based {
            Box::new(BlockIndexMemoryStorage::new())
        } else {
            std::fs::create_dir_all(&index_folder).unwrap();

            if config.use_existing {
                Box::new(BlockIndexFileStorage::from_existing(&index_folder).unwrap())
            } else {
                Box::new(BlockIndexFileStorage::new(&index_folder))
            }
        };

        BlockIndex {
            config,
            storage,
            deleted_blocks: Vec::new(),
            num_reallocations: 0,
            num_reused_blocks: 0
        }
    }

    fn get_block(&mut self, size: usize) -> (usize, usize) {
        // Re-use existing block
        if self.config.reuse_blocks {
            for i in 0..self.deleted_blocks.len() {
                if self.deleted_blocks[i].1 >= size {
                    let block = self.deleted_blocks[i];
                    self.deleted_blocks.remove(i);
                    self.num_reused_blocks += 1;
                    return block;
                }
            }
        }

        // Allocate a new block
        self.storage.allocate_block(size)
    }

    fn read_term_header(&self, offset: usize) -> BlockIndexResult<TermHeader> {
        let mut offset = offset;
        let term_size = self.storage.read_u64(&mut offset)?;
        let num_documents = self.storage.read_u64(&mut offset)?;
        let next_document_start = self.storage.read_u64(&mut offset)?;

        Ok(
            TermHeader {
                size: term_size,
                num_documents,
                next_document_start
            }
        )
    }

    fn write_term_entry(&mut self, term_entry_buffer: &mut ByteBuffer, entry: &TermDocumentEntry) {
        term_entry_buffer.write_u64(entry.document());

        term_entry_buffer.write_u32(entry.offsets().len() as u32);
        for value in entry.offsets() {
            term_entry_buffer.write_u32(*value);
        }
    }

    fn write_new_term(&mut self, term: &Term, entries: Vec<&TermDocumentEntry>, grow_factor: usize) -> BlockIndexResult<()> {
        let mut term_entry_buffer = ByteBuffer::new();

        // Write term header
        term_entry_buffer.write_u64(0); // Size of whole term
        term_entry_buffer.write_u64(entries.len() as u64); // Number of documents
        let next_document_start_index = term_entry_buffer.offset;
        term_entry_buffer.write_u64(0); // Start of next document

        // Write entries
        for entry in entries {
            self.write_term_entry(&mut term_entry_buffer, entry);
        }

        // Patch next document start
        let current_index = term_entry_buffer.offset;
        term_entry_buffer.offset = next_document_start_index;
        term_entry_buffer.write_u64(current_index as u64);
        term_entry_buffer.offset = current_index;

        // We overallocate to get room with next documents
        let wanted_term_size = (((term_entry_buffer.len() * grow_factor) + (self.config.block_size - 1)) / self.config.block_size) * self.config.block_size;
        term_entry_buffer.buffer.resize(wanted_term_size, 0);

        let (term_start, term_size) = self.get_block(wanted_term_size);

        // Patch term size
        term_entry_buffer.offset = 0;
        term_entry_buffer.write_u64(term_size as u64);

        // Write to underlying storage
        self.storage.add_term_start_position(term, term_start);
        self.storage.write_bytes(term_start, &term_entry_buffer)?;
        Ok(())
    }

    fn write_existing_term(&mut self, term: &Term, entry: &TermDocumentEntry) -> BlockIndexResult<()>  {
        let term_start = self.storage.get_term_start_position(term);
        let term_header = self.read_term_header(term_start)?;

        let mut term_entry_buffer = ByteBuffer::new();
        self.write_term_entry(&mut term_entry_buffer, entry);

        if (term_header.next_document_start + term_entry_buffer.len() as u64) < term_header.size {
            // Write to the underlying storage
            self.storage.write_bytes(term_start + term_header.next_document_start as usize, &term_entry_buffer)?;

            // Update header
            let mut update_offset = term_start + std::mem::size_of::<u64>();
            self.storage.write_u64(&mut update_offset, term_header.num_documents + 1)?;
            self.storage.write_u64(&mut update_offset, term_header.next_document_start + term_entry_buffer.len() as u64)?;
        } else {
            // Term block is not large enough, re-allocate
            self.num_reallocations += 1;

            let read_entries = self.read_term_documents_internal(term)?;
            self.deleted_blocks.push((read_entries.start, read_entries.size));

            let mut documents = read_entries.documents;
            documents.push(entry.clone());

            self.storage.remove_term_start_position(term);
            self.write_new_term(term, documents.documents().iter().collect(), 2)?;
        }

        Ok(())
    }

    fn read_term_documents_internal(&self, term: &Term) -> BlockIndexResult<ReadTermResult> {
        let mut term_documents = TermDocuments::new();

        let term_start = self.storage.get_term_start_position(term);
        let term_header = self.read_term_header(term_start)?;

        let mut term_buffer = vec![0u8; term_header.size as usize];
        self.storage.read_bytes(term_start, &mut term_buffer[..])?;

        let mut buffer_offset = 3 * std::mem::size_of::<u64>();
        for _ in 0..term_header.num_documents {
            let document_id = read_u64(&term_buffer, &mut buffer_offset);
            let num_offsets = read_u32(&term_buffer, &mut buffer_offset);

            let mut term_entry = TermDocumentEntry::new(document_id);
            for _ in 0..num_offsets {
                let offset_within_document = read_u32(&term_buffer, &mut buffer_offset);
                term_entry.add_offset(offset_within_document)
            }

            term_documents.push(term_entry);
        }

        Ok(
            ReadTermResult {
                start: term_start,
                size: term_header.size as usize,
                documents: term_documents
            }
        )
    }
}

pub struct BlockIndexIterator<'a> {
    block_index: &'a BlockIndex,
    terms: Vec<String>,
    current_index: usize
}

impl<'a> BlockIndexIterator<'a> {
    pub fn new(block_index: &'a BlockIndex, terms: Vec<String>) -> BlockIndexIterator<'a> {
        BlockIndexIterator {
            block_index,
            terms,
            current_index: 0
        }
    }
}

impl<'a> Iterator for BlockIndexIterator<'a> {
    type Item = (Term, TermDocuments);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.terms.len() {
            return None;
        }

        let index = self.current_index;
        self.current_index += 1;
        Some((self.terms[index].clone(), self.block_index.read_documents(&self.terms[index]).ok()?))
    }
}

impl Index for BlockIndex {
    fn num_terms(&self) -> usize {
        self.storage.num_terms()
    }

    fn print_stats(&self) {
        println!("Number of re-allocations: {}", self.num_reallocations);
        println!("Number of re-used blocks: {}", self.num_reused_blocks);
    }

    fn add(&mut self, term: &Term, entry: TermDocumentEntry) -> IndexResult<()> {
        if self.storage.term_exists(term) {
            self.write_existing_term(term, &entry)?;
        } else {
            self.write_new_term(term, vec![&entry], 1)?;
        }

        Ok(())
    }

    fn read_documents(&self, term: &Term) -> IndexResult<TermDocuments> {
        Ok(self.read_term_documents_internal(term)?.documents)
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=(Term, TermDocuments)> + 'a> {
        let terms = self.storage.terms_iter().collect::<Vec<_>>();
        Box::new(BlockIndexIterator::<'a>::new(self, terms))
    }
}

#[test]
fn test_single_document1() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());
    let mut term_entry = TermDocumentEntry::new(1337);
    term_entry.add_offset(1414);
    term_entry.add_offset(1561);
    term_entry.add_offset(7151);

    index.add(&"A".to_owned(), term_entry.clone()).unwrap();
    let read_term_documents = index.read_documents(&"A".to_owned());

    assert!(read_term_documents.is_ok());
    let read_term_documents = read_term_documents.unwrap();

    assert_eq!(1, read_term_documents.len());

    let read_term_entry = &read_term_documents.documents()[0];
    assert_eq!(term_entry.document(), read_term_entry.document());
    assert_eq!(term_entry.offsets(), read_term_entry.offsets());
}

#[test]
fn test_single_document2() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());
    let mut term_entry1 = TermDocumentEntry::new(1337);
    term_entry1.add_offset(1414);
    term_entry1.add_offset(1561);
    term_entry1.add_offset(7151);

    index.add(&"A".to_owned(), term_entry1.clone()).unwrap();

    let mut term_entry2 = TermDocumentEntry::new(4711);
    term_entry2.add_offset(3151);
    term_entry2.add_offset(3242);
    term_entry2.add_offset(54674);

    index.add(&"B".to_owned(), term_entry2.clone()).unwrap();

    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(1, read_term1_documents.len());
    let read_term_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term_entry1.document(), read_term_entry1.document());
    assert_eq!(term_entry1.offsets(), read_term_entry1.offsets());

    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(1, read_term2_documents.len());
    let read_term_entry2 = &read_term2_documents.documents()[0];
    assert_eq!(term_entry2.document(), read_term_entry2.document());
    assert_eq!(term_entry2.offsets(), read_term_entry2.offsets());
}

#[test]
fn test_multiple_documents1() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term_entry1 = TermDocumentEntry::new(1337);
    term_entry1.add_offset(1414);
    term_entry1.add_offset(1561);
    term_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term_entry1.clone()).unwrap();

    let mut term_entry2 = TermDocumentEntry::new(4711);
    term_entry2.add_offset(32434);
    term_entry2.add_offset(14141);
    term_entry2.add_offset(141);
    index.add(&"A".to_owned(), term_entry2.clone()).unwrap();

    let read_term_documents = index.read_documents(&"A".to_owned());
    assert!(read_term_documents.is_ok());
    let read_term_documents = read_term_documents.unwrap();

    assert_eq!(2, read_term_documents.len());

    let read_term_entry1 = &read_term_documents.documents()[0];
    assert_eq!(term_entry1.document(), read_term_entry1.document());
    assert_eq!(term_entry1.offsets(), read_term_entry1.offsets());

    let read_term_entry2 = &read_term_documents.documents()[1];
    assert_eq!(term_entry2.document(), read_term_entry2.document());
    assert_eq!(term_entry2.offsets(), read_term_entry2.offsets());
}

#[test]
fn test_multiple_documents2() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term_entry1 = TermDocumentEntry::new(1337);
    term_entry1.add_offset(1414);
    term_entry1.add_offset(1561);
    term_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term_entry1.clone()).unwrap();

    let mut term_entry2 = TermDocumentEntry::new(4711);
    term_entry2.add_offset(32434);
    term_entry2.add_offset(14141);
    term_entry2.add_offset(141);
    index.add(&"A".to_owned(), term_entry2.clone()).unwrap();

    let mut term_entry3 = TermDocumentEntry::new(2141);
    term_entry3.add_offset(1341426);
    term_entry3.add_offset(666);
    term_entry3.add_offset(2452);
    index.add(&"A".to_owned(), term_entry3.clone()).unwrap();

    let read_term_documents = index.read_documents(&"A".to_owned());
    assert!(read_term_documents.is_ok());
    let read_term_documents = read_term_documents.unwrap();

    assert_eq!(3, read_term_documents.len());

    let read_term_entry1 = &read_term_documents.documents()[0];
    assert_eq!(term_entry1.document(), read_term_entry1.document());
    assert_eq!(term_entry1.offsets(), read_term_entry1.offsets());

    let read_term_entry2 = &read_term_documents.documents()[1];
    assert_eq!(term_entry2.document(), read_term_entry2.document());
    assert_eq!(term_entry2.offsets(), read_term_entry2.offsets());

    let read_term_entry3 = &read_term_documents.documents()[2];
    assert_eq!(term_entry3.document(), read_term_entry3.document());
    assert_eq!(term_entry3.offsets(), read_term_entry3.offsets());
}

#[test]
fn test_multiple_documents3() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(4711);
    term2_entry1.add_offset(32434);
    term2_entry1.add_offset(14141);
    term2_entry1.add_offset(141);
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();

    let mut term1_entry2 = TermDocumentEntry::new(2141);
    term1_entry2.add_offset(1341426);
    term1_entry2.add_offset(666);
    term1_entry2.add_offset(2452);
    index.add(&"A".to_owned(), term1_entry2.clone()).unwrap();

    // Term 1
    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(2, read_term1_documents.len());

    let read_term1_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term1_entry1.document(), read_term1_entry1.document());
    assert_eq!(term1_entry1.offsets(), read_term1_entry1.offsets());

    let read_term1_entry2 = &read_term1_documents.documents()[1];
    assert_eq!(term1_entry2.document(), read_term1_entry2.document());
    assert_eq!(term1_entry2.offsets(), read_term1_entry2.offsets());

    // Term2
    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(1, read_term2_documents.len());
    let read_term2_entry1 = &read_term2_documents.documents()[0];
    assert_eq!(term2_entry1.document(), read_term2_entry1.document());
    assert_eq!(term2_entry1.offsets(), read_term2_entry1.offsets());
}

#[test]
fn test_multiple_documents4() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(4711);
    term2_entry1.add_offset(32434);
    term2_entry1.add_offset(14141);
    term2_entry1.add_offset(141);
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();

    let mut term1_entry2 = TermDocumentEntry::new(2141);
    term1_entry2.add_offset(1341426);
    term1_entry2.add_offset(666);
    term1_entry2.add_offset(2452);
    index.add(&"A".to_owned(), term1_entry2.clone()).unwrap();

    let mut term2_entry2 = TermDocumentEntry::new(41414);
    term2_entry2.add_offset(5454);
    term2_entry2.add_offset(1313);
    term2_entry2.add_offset(141);
    term2_entry2.add_offset(3131);
    index.add(&"B".to_owned(), term2_entry2.clone()).unwrap();

    // Term 1
    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(2, read_term1_documents.len());

    let read_term1_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term1_entry1.document(), read_term1_entry1.document());
    assert_eq!(term1_entry1.offsets(), read_term1_entry1.offsets());

    let read_term1_entry2 = &read_term1_documents.documents()[1];
    assert_eq!(term1_entry2.document(), read_term1_entry2.document());
    assert_eq!(term1_entry2.offsets(), read_term1_entry2.offsets());

    // Term2
    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(2, read_term2_documents.len());

    let read_term2_entry1 = &read_term2_documents.documents()[0];
    assert_eq!(term2_entry1.document(), read_term2_entry1.document());
    assert_eq!(term2_entry1.offsets(), read_term2_entry1.offsets());

    let read_term2_entry2 = &read_term2_documents.documents()[1];
    assert_eq!(term2_entry2.document(), read_term2_entry2.document());
    assert_eq!(term2_entry2.offsets(), read_term2_entry2.offsets());
}

#[test]
fn test_multiple_documents5() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term1_entry2 = TermDocumentEntry::new(2141);
    term1_entry2.add_offset(1341426);
    term1_entry2.add_offset(666);
    term1_entry2.add_offset(2452);
    index.add(&"A".to_owned(), term1_entry2.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(4711);
    term2_entry1.add_offset(32434);
    term2_entry1.add_offset(14141);
    term2_entry1.add_offset(141);
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();

    let mut term2_entry2 = TermDocumentEntry::new(41414);
    term2_entry2.add_offset(5454);
    term2_entry2.add_offset(1313);
    term2_entry2.add_offset(141);
    term2_entry2.add_offset(3131);
    index.add(&"B".to_owned(), term2_entry2.clone()).unwrap();

    // Term 1
    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(2, read_term1_documents.len());

    let read_term1_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term1_entry1.document(), read_term1_entry1.document());
    assert_eq!(term1_entry1.offsets(), read_term1_entry1.offsets());

    let read_term1_entry2 = &read_term1_documents.documents()[1];
    assert_eq!(term1_entry2.document(), read_term1_entry2.document());
    assert_eq!(term1_entry2.offsets(), read_term1_entry2.offsets());

    // Term2
    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(2, read_term2_documents.len());

    let read_term2_entry1 = &read_term2_documents.documents()[0];
    assert_eq!(term2_entry1.document(), read_term2_entry1.document());
    assert_eq!(term2_entry1.offsets(), read_term2_entry1.offsets());

    let read_term2_entry2 = &read_term2_documents.documents()[1];
    assert_eq!(term2_entry2.document(), read_term2_entry2.document());
    assert_eq!(term2_entry2.offsets(), read_term2_entry2.offsets());
}

#[test]
fn test_multiple_documents6() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term1_entry2 = TermDocumentEntry::new(2141);
    term1_entry2.add_offset(1341426);
    term1_entry2.add_offset(666);
    term1_entry2.add_offset(2452);
    index.add(&"A".to_owned(), term1_entry2.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(4711);
    term2_entry1.add_offset(32434);
    term2_entry1.add_offset(14141);
    term2_entry1.add_offset(141);
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();

    let mut term2_entry2 = TermDocumentEntry::new(41414);
    term2_entry2.add_offset(5454);
    term2_entry2.add_offset(1313);
    term2_entry2.add_offset(141);
    term2_entry2.add_offset(3131);
    index.add(&"B".to_owned(), term2_entry2.clone()).unwrap();

    let mut term1_entry3 = TermDocumentEntry::new(6621);
    term1_entry3.add_offset(13143);
    term1_entry3.add_offset(5454);
    term1_entry3.add_offset(313);
    term1_entry3.add_offset(434);
    term1_entry3.add_offset(131);
    index.add(&"A".to_owned(), term1_entry3.clone()).unwrap();

    // Term 1
    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(3, read_term1_documents.len());

    let read_term1_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term1_entry1.document(), read_term1_entry1.document());
    assert_eq!(term1_entry1.offsets(), read_term1_entry1.offsets());

    let read_term1_entry2 = &read_term1_documents.documents()[1];
    assert_eq!(term1_entry2.document(), read_term1_entry2.document());
    assert_eq!(term1_entry2.offsets(), read_term1_entry2.offsets());

    let read_term1_entry3 = &read_term1_documents.documents()[2];
    assert_eq!(term1_entry3.document(), read_term1_entry3.document());
    assert_eq!(term1_entry3.offsets(), read_term1_entry3.offsets());

    // Term2
    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(2, read_term2_documents.len());

    let read_term2_entry1 = &read_term2_documents.documents()[0];
    assert_eq!(term2_entry1.document(), read_term2_entry1.document());
    assert_eq!(term2_entry1.offsets(), read_term2_entry1.offsets());

    let read_term2_entry2 = &read_term2_documents.documents()[1];
    assert_eq!(term2_entry2.document(), read_term2_entry2.document());
    assert_eq!(term2_entry2.offsets(), read_term2_entry2.offsets());
}

#[test]
fn test_reallocate1() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term_entry1 = TermDocumentEntry::new(1337);
    term_entry1.add_offset(1414);
    term_entry1.add_offset(1561);
    term_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term_entry1.clone()).unwrap();

    let mut term_entry2 = TermDocumentEntry::new(4711);
    for i in 0..2000 {
        term_entry2.add_offset(i);
    }
    index.add(&"A".to_owned(), term_entry2.clone()).unwrap();

    let read_term_documents = index.read_documents(&"A".to_owned());
    assert!(read_term_documents.is_ok());
    let read_term_documents = read_term_documents.unwrap();

    assert_eq!(2, read_term_documents.len());

    let read_term_entry1 = &read_term_documents.documents()[0];
    assert_eq!(term_entry1.document(), read_term_entry1.document());
    assert_eq!(term_entry1.offsets(), read_term_entry1.offsets());

    let read_term_entry2 = &read_term_documents.documents()[1];
    assert_eq!(term_entry2.document(), read_term_entry2.document());
    assert_eq!(term_entry2.offsets(), read_term_entry2.offsets());
}

#[test]
fn test_reallocate2() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(31431);
    term2_entry1.add_offset(131);
    term2_entry1.add_offset(454);
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();

    let mut term1_entry2 = TermDocumentEntry::new(4711);
    for i in 0..2000 {
        term1_entry2.add_offset(i);
    }
    index.add(&"A".to_owned(), term1_entry2.clone()).unwrap();

    // Term 1
    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(2, read_term1_documents.len());

    let read_term1_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term1_entry1.document(), read_term1_entry1.document());
    assert_eq!(term1_entry1.offsets(), read_term1_entry1.offsets());

    let read_term1_entry2 = &read_term1_documents.documents()[1];
    assert_eq!(term1_entry2.document(), read_term1_entry2.document());
    assert_eq!(term1_entry2.offsets(), read_term1_entry2.offsets());

    // Term 2
    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(1, read_term2_documents.len());

    let read_term2_entry1 = &read_term2_documents.documents()[0];
    assert_eq!(term2_entry1.document(), read_term2_entry1.document());
    assert_eq!(term2_entry1.offsets(), read_term2_entry1.offsets());
}

#[test]
fn test_reallocate3() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term1_entry2 = TermDocumentEntry::new(4711);
    for i in 0..2000 {
        term1_entry2.add_offset(i);
    }
    index.add(&"A".to_owned(), term1_entry2.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(31431);
    term2_entry1.add_offset(131);
    term2_entry1.add_offset(454);
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();

    // Term 1
    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(2, read_term1_documents.len());

    let read_term1_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term1_entry1.document(), read_term1_entry1.document());
    assert_eq!(term1_entry1.offsets(), read_term1_entry1.offsets());

    let read_term1_entry2 = &read_term1_documents.documents()[1];
    assert_eq!(term1_entry2.document(), read_term1_entry2.document());
    assert_eq!(term1_entry2.offsets(), read_term1_entry2.offsets());

    // Term 2
    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(1, read_term2_documents.len());

    let read_term2_entry1 = &read_term2_documents.documents()[0];
    assert_eq!(term2_entry1.document(), read_term2_entry1.document());
    assert_eq!(term2_entry1.offsets(), read_term2_entry1.offsets());
}

#[test]
fn test_reallocate4() {
    let mut config = BlockIndexConfig::testing();
    config.reuse_blocks = true;
    let mut index = BlockIndex::new(config);

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term1_entry2 = TermDocumentEntry::new(4711);
    for i in 0..2000 {
        term1_entry2.add_offset(i);
    }
    index.add(&"A".to_owned(), term1_entry2.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(31431);
    term2_entry1.add_offset(131);
    term2_entry1.add_offset(454);

    assert_eq!(1, index.deleted_blocks.len());
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();
    assert_eq!(0, index.deleted_blocks.len());

    // Term 1
    let read_term1_documents = index.read_documents(&"A".to_owned());
    assert!(read_term1_documents.is_ok());
    let read_term1_documents = read_term1_documents.unwrap();

    assert_eq!(2, read_term1_documents.len());

    let read_term1_entry1 = &read_term1_documents.documents()[0];
    assert_eq!(term1_entry1.document(), read_term1_entry1.document());
    assert_eq!(term1_entry1.offsets(), read_term1_entry1.offsets());

    let read_term1_entry2 = &read_term1_documents.documents()[1];
    assert_eq!(term1_entry2.document(), read_term1_entry2.document());
    assert_eq!(term1_entry2.offsets(), read_term1_entry2.offsets());

    // Term 2
    let read_term2_documents = index.read_documents(&"B".to_owned());
    assert!(read_term2_documents.is_ok());
    let read_term2_documents = read_term2_documents.unwrap();

    assert_eq!(1, read_term2_documents.len());

    let read_term2_entry1 = &read_term2_documents.documents()[0];
    assert_eq!(term2_entry1.document(), read_term2_entry1.document());
    assert_eq!(term2_entry1.offsets(), read_term2_entry1.offsets());
}

#[test]
fn test_iterate1() {
    let mut index = BlockIndex::new(BlockIndexConfig::testing());

    let mut term1_entry1 = TermDocumentEntry::new(1337);
    term1_entry1.add_offset(1414);
    term1_entry1.add_offset(1561);
    term1_entry1.add_offset(7151);
    index.add(&"A".to_owned(), term1_entry1.clone()).unwrap();

    let mut term2_entry1 = TermDocumentEntry::new(31431);
    term2_entry1.add_offset(131);
    term2_entry1.add_offset(454);
    index.add(&"B".to_owned(), term2_entry1.clone()).unwrap();

    let mut term2_entry2 = TermDocumentEntry::new(314341);
    term2_entry2.add_offset(2141);
    term2_entry2.add_offset(434);
    term2_entry2.add_offset(424);
    index.add(&"B".to_owned(), term2_entry2.clone()).unwrap();

    let mut block_index_iter_result = index.iter().collect::<Vec<_>>();
    block_index_iter_result.sort_by(|x, y| x.0.cmp(&y.0));

    assert_eq!(block_index_iter_result.len(), 2);

    assert_eq!(&block_index_iter_result[0].0, &"A");
    assert_eq!(block_index_iter_result[0].1.len(), 1);

    assert_eq!(&block_index_iter_result[1].0, &"B");
    assert_eq!(block_index_iter_result[1].1.len(), 2);
}