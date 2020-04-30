use std::path::{Path, PathBuf};
use std::fs::{DirEntry, File};
use std::io::{BufWriter, Write};

use futures::future;

use crate::crawler::Crawler;
use crate::indexer::{Indexer};
use crate::parser::Parser;
use crate::tokenizer::Tokens;
use crate::block_index::{BlockIndex, BlockIndexConfig};
use crate::searcher::Searcher;
use crate::document::{DocumentFileStorage, Document};
use crate::ranker::Ranker;

mod crawler;
mod content_extractor;
mod tokenizer;
mod parser;
mod term;
mod document;
mod index;
mod indexer;
mod block_index;
mod searcher;
mod ranker;

fn visit_dirs<F: FnMut(DirEntry)>(dir: &Path, cb: &mut F) -> std::io::Result<()> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(entry);
            }
        }
    }
    Ok(())
}

async fn main_crawl() -> Result<(), Box<dyn std::error::Error>> {
    let mut crawler = Crawler::new();

    let result_receiver = crawler.result_receiver.clone();
    // std::thread::spawn(move || {
    //     for result in &result_receiver {
    //         // Save content to disk
    //         let (save_path_folder, save_path_name) = crawler::get_save_path(&result.url);
    //         let base_path = format!("data/{}", save_path_folder);
    //         std::fs::create_dir_all(&base_path);
    //         std::fs::write(format!("{}/{}", base_path, save_path_name), result.content);
    //     }
    // });
    let handle = std::thread::spawn(move || {
        let parser = Parser::new();
        let mut indexer = Indexer::new(
            Box::new(BlockIndex::new(BlockIndexConfig::default())),
            Box::new(DocumentFileStorage::new("index/document_storage"))
        );

        for result in &result_receiver {
            if let Some(result) = result {
                let parsed_content = parser.parse(&result.content);
                indexer.add_document(Document::new(
                    parsed_content.title.unwrap_or(String::new()),
                    parsed_content.tokens
                ));
            } else {
                break;
            }
        }

        println!("Exited.");
    });

    crawler.run("https://en.wikipedia.org/wiki/Main_Page");
    handle.join();
    Ok(())
}

fn main_index_from_files() {
    let parser = Parser::new();
    let mut indexer = Indexer::new(
        Box::new(BlockIndex::new(BlockIndexConfig::default())),
        Box::new(DocumentFileStorage::new("index/document_storage"))
    );
    let searcher = Searcher::new();

    let t0 = std::time::Instant::now();
    let mut document_index = 0;
    visit_dirs(
        "test_data/wiki".as_ref(),
        &mut |entry| {
            let content = std::fs::read_to_string(entry.path()).unwrap();
            let parsed_content = parser.parse(&content);
            indexer.add_document(Document::new(
                parsed_content.title.unwrap_or(String::new()),
                parsed_content.tokens
            ));
            document_index += 1;
        }
    ).unwrap();
    println!("Built index in: {} seconds", (std::time::Instant::now() - t0).as_millis() as f64 / 1000.0);

    indexer.print_stats();

    let results = searcher.intersection_search_documents(&indexer, vec!["computer".to_owned(), "hardware".to_owned()]);
    println!("Search results: {}", results.len());
}

fn main_search() {
    let mut indexer = Indexer::new(
        Box::new(BlockIndex::new(BlockIndexConfig::use_existing())),
        Box::new(DocumentFileStorage::from_existing("index/document_storage"))
    );
    let searcher = Searcher::new();
    let ranker = Ranker::new();

    // indexer.print_stats();

    let query = vec!["computer".to_owned(), "hardware".to_owned(), "technology".to_owned()];
    // let results = searcher.intersection_search(
    //     &indexer,
    //     query
    // );
    //
    // println!("Search results: {}", results.len());
    // for document_index in results {
    //     println!("{}", document_index);
    // }

    let results = ranker.rank(&indexer, searcher.intersection_search(&indexer, query));
    println!("Search results: {}", results.len());
    for (document_id, score) in results.iter().take(20) {
        println!("{} (id: {}): ({})", indexer.document_storage().get_title(*document_id), document_id, score);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    main_crawl().await.unwrap();
    // main_search();
    Ok(())
}
