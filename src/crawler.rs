use std::iter::FromIterator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, Mutex, Arc};
use std::future::Future;
use std::collections::{HashSet, VecDeque};

use regex::Regex;

use futures::future::{BoxFuture, FutureExt};
use url::Url;

struct LinkExtractor {
    pattern: Regex
}

impl LinkExtractor {
    fn new() -> LinkExtractor {
        LinkExtractor {
            pattern: Regex::new("<a.*href=\"([^\"]+)\".*>").unwrap()
        }
    }

    fn extract(&self, base_url: &str, content: &str) -> Vec<String> {
        let mut links = Vec::new();

        for result in self.pattern.captures_iter(content) {
            let mut link = result.get(1).unwrap().as_str().to_owned().to_lowercase();
            if link.starts_with("//") {
                link = format!("https:{}", link);
            } else if link.starts_with("/") {
                link = format!("{}{}", base_url, link);
            }

            if link.starts_with("https://en.wikipedia.org") {
                links.push(link);
            }
        }

        links
    }
}

struct CrawledLinks {
    inner: HashSet<String>
}

impl CrawledLinks {
    fn new() -> CrawledLinks {
        CrawledLinks {
            inner: HashSet::new()
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn contains(&self, link: &str) -> bool {
        self.inner.contains(link)
    }

    fn insert(&mut self, link: String) -> bool {
        self.inner.insert(link)
    }
}

#[derive(Debug)]
pub struct CrawlResult {
    pub url: Url,
    pub content: String
}

pub fn get_save_path(parsed_url: &Url) -> (String, String) {
    let save_path = parsed_url.path().to_owned() + &parsed_url.query().map(|x| format!("?{}", x)).unwrap_or(String::new());
    let save_path_parts = save_path.split('/').filter(|x| x != &"").collect::<Vec<_>>();
    let save_path_name: String = (*save_path_parts.last().unwrap()).to_owned();
    let save_path_folder = save_path_parts[..save_path_parts.len() - 1].join("/");
    println!("{}: {}-{}", save_path, save_path_folder, save_path_name);
    (save_path_folder, save_path_name)
}

struct CrawlerWorker {
    link_extractor: LinkExtractor,
    crawled_links: Arc<RwLock<CrawledLinks>>,
    crawl_queue: Arc<crossbeam::queue::SegQueue<String>>,
    results_sender: crossbeam::Sender<Option<CrawlResult>>,
    num_active_requests: Arc<AtomicU64>,
    num_crawled: Arc<AtomicU64>,
}

impl CrawlerWorker {
    fn new(crawled_links: Arc<RwLock<CrawledLinks>>,
           crawl_queue: Arc<crossbeam::queue::SegQueue<String>>,
           results_sender: crossbeam::Sender<Option<CrawlResult>>) -> CrawlerWorker {
        CrawlerWorker {
            link_extractor: LinkExtractor::new(),
            crawled_links,
            crawl_queue,
            results_sender,
            num_active_requests: Arc::new(AtomicU64::new(0)),
            num_crawled: Arc::new(AtomicU64::new(0))
        }
    }

    async fn crawl(&self, url: String) {
        if let Ok(response) = reqwest::get(&url).await {
            let content = response.text().await.unwrap();
            self.num_active_requests.fetch_sub(1, Ordering::SeqCst);

            // Extract link
            let parsed_url = Url::parse(&url).unwrap();
            let base_url = format!("{}://{}", parsed_url.scheme(), parsed_url.domain().unwrap());
            let extracted_links = self.link_extractor.extract(&base_url, &content);

            self.num_crawled.fetch_add(1, Ordering::SeqCst);

            if parsed_url.path().starts_with("/wiki") {
                self.results_sender.send(Some(CrawlResult {
                    url: parsed_url,
                    content
                })).unwrap();
            }

            // Insert the links
            {
                let mut crawled_links_guard = self.crawled_links.write().unwrap();

                let mut total_links = 0;
                let mut new_links = 0;
                for link in extracted_links {
                    let link_url = Url::parse(&link).unwrap();
                    let normalized_link = link_url.scheme().to_owned() + "://" + link_url.domain().unwrap() + link_url.path();

                    // println!("{} -> {}", link, normalized_link);
                    let link = normalized_link;

                    if !crawled_links_guard.contains(&link) {
                        crawled_links_guard.insert(link.clone());
                        self.crawl_queue.push(link);
                        new_links += 1;
                    }

                    total_links += 1;
                }

                println!("Result for {}: {}/{}", url, new_links, total_links);
            }
        } else {
            self.num_active_requests.fetch_sub(1, Ordering::SeqCst);
            println!("Failed to crawl url: {}", url);
        }
    }
}

pub struct Crawler {
    crawled_links: Arc<RwLock<CrawledLinks>>,
    crawl_queue: Arc<crossbeam::queue::SegQueue<String>>,
    pub result_receiver: crossbeam::Receiver<Option<CrawlResult>>,
    worker: Arc<CrawlerWorker>,
}

impl Crawler {
    pub fn new() -> Crawler {
        let crawled_links = Arc::new(RwLock::new(CrawledLinks::new()));
        let crawl_queue = Arc::new(crossbeam::queue::SegQueue::new());
        let (result_sender, result_receiver) = crossbeam::unbounded();

        Crawler {
            crawled_links: crawled_links.clone(),
            crawl_queue: crawl_queue.clone(),
            result_receiver,
            worker: Arc::new(CrawlerWorker::new(crawled_links, crawl_queue, result_sender)),
        }
    }


    pub fn run(&self, start_url: &str) {
        self.crawl_queue.push(start_url.to_owned());
        let start_time = std::time::Instant::now();

        loop {
            println!(
                "Num active: {}, total crawled: {}",
                self.worker.num_active_requests.load(Ordering::SeqCst),
                self.worker.num_crawled.load(Ordering::SeqCst)
            );

            if self.worker.num_active_requests.load(Ordering::SeqCst) < 100 {
                if let Ok(current_url) = self.crawl_queue.pop() {
                    println!("Crawling url: {}", current_url);
                    self.worker.num_active_requests.fetch_add(1, Ordering::SeqCst);

                    let worker_clone = self.worker.clone();
                    tokio::task::spawn(async move {
                        worker_clone.crawl(current_url).await;
                    });
                }

                if self.worker.num_active_requests.load(Ordering::SeqCst) == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            } else {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            if (std::time::Instant::now() - start_time).as_secs() >= 60 {
                self.worker.results_sender.send(None);
                break;
            }
        }
    }
}