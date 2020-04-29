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


struct CrawlerWorker {
    link_extractor: LinkExtractor,
    crawled_links: Arc<RwLock<CrawledLinks>>,
    crawl_queue: Arc<Mutex<VecDeque<String>>>,
    num_active_crawlers: Arc<AtomicU64>
}

impl CrawlerWorker {
    fn new(crawled_links: Arc<RwLock<CrawledLinks>>,
           crawl_queue: Arc<Mutex<VecDeque<String>>>) -> CrawlerWorker {
        CrawlerWorker {
            link_extractor: LinkExtractor::new(),
            crawled_links,
            crawl_queue,
            num_active_crawlers: Arc::new(AtomicU64::new(0))
        }
    }

    fn get_save_path(parsed_url: &Url) -> (String, String) {
        let save_path = parsed_url.path().to_owned() + &parsed_url.query().map(|x| format!("?{}", x)).unwrap_or(String::new());
        let save_path_parts = save_path.split('/').filter(|x| x != &"").collect::<Vec<_>>();
        let save_path_name: String = (*save_path_parts.last().unwrap()).to_owned();
        let save_path_folder = save_path_parts[..save_path_parts.len() - 1].join("/");
        println!("{}: {}-{}", save_path, save_path_folder, save_path_name);
        (save_path_folder, save_path_name)
    }

    async fn crawl(&self, url: String) {
        if let Ok(response) = reqwest::get(&url).await {
            let content = response.text().await.unwrap();
            self.num_active_crawlers.fetch_sub(1, Ordering::SeqCst);

            // Mark the link as crawled
            self.crawled_links.write().unwrap().insert(url.to_owned());

            // Extract link
            let parsed_url = Url::parse(&url).unwrap();
            let base_url = format!("{}://{}", parsed_url.scheme(), parsed_url.domain().unwrap());
            let extracted_links = self.link_extractor.extract(&base_url, &content);

            // Save content to disk
            let (save_path_folder, save_path_name) = CrawlerWorker::get_save_path(&parsed_url);
            tokio::task::spawn(async move {
                let base_path = format!("data/{}", save_path_folder);
                tokio::fs::create_dir_all(&base_path).await.unwrap();
                tokio::fs::write(format!("{}/{}", base_path, save_path_name), content).await;
            });

            // Insert the links
            {
                let crawled_links_guard = self.crawled_links.read().unwrap();
                let mut crawl_queue_guard = self.crawl_queue.lock().unwrap();

                let mut total_links = 0;
                let mut new_links = 0;
                for link in extracted_links{
                    if !crawled_links_guard.contains(&link) {
                        crawl_queue_guard.push_back(link);
                        new_links += 1;
                    }

                    total_links += 1;
                }

                println!("{}: {}/{}", url, new_links, total_links);
            }
        } else {
            self.num_active_crawlers.fetch_sub(1, Ordering::SeqCst);
            println!("Failed to crawl url: {}", url);
        }
    }
}

pub struct Crawler {
    crawled_links: Arc<RwLock<CrawledLinks>>,
    crawl_queue: Arc<Mutex<VecDeque<String>>>,
    worker: Arc<CrawlerWorker>
}

impl Crawler {
    pub fn new() -> Crawler {
        let crawled_links = Arc::new(RwLock::new(CrawledLinks::new()));
        let crawl_queue = Arc::new(Mutex::new(VecDeque::new()));

        Crawler {
            crawled_links: crawled_links.clone(),
            crawl_queue: crawl_queue.clone(),
            worker: Arc::new(CrawlerWorker::new(crawled_links, crawl_queue))
        }
    }

    pub fn run(&self, start_url: &str) {
        self.crawl_queue.lock().unwrap().push_back(start_url.to_owned());

        loop {
            println!(
                "Num active: {}, total crawled: {}",
                self.worker.num_active_crawlers.load(Ordering::SeqCst),
                self.crawled_links.read().unwrap().len()
            );

            if self.worker.num_active_crawlers.load(Ordering::SeqCst) < 100 {
                if let Some(current_url) = self.crawl_queue.lock().unwrap().pop_front() {
                    println!("Crawling url: {}", current_url);
                    self.worker.num_active_crawlers.fetch_add(1, Ordering::SeqCst);

                    let worker_clone = self.worker.clone();
                    tokio::task::spawn(async move {
                        worker_clone.crawl(current_url).await;
                    });
                }
            } else {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

