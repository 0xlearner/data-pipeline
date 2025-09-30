use anyhow::Result;
use scraper::{Html, Selector};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::HtmlConfig;
use crate::extractor::{HtmlExtractor, ScrapedProduct};
use crate::fetcher::{HttpFetcher, HtmlPageProcessor};
use crate::storage::MinioStorage;

/// Unified HtmlFetcher that can operate in two modes:
/// 1. Direct mode: Fetch and scrape immediately
/// 2. Storage mode: Fetch HTML, store in S3, then scrape from storage
pub struct HtmlFetcher {
    http_fetcher: HttpFetcher,
    extractor: HtmlExtractor,
    config: HtmlConfig,
    storage: Option<Arc<MinioStorage>>,
    storage_mode: bool,
}

/// Represents a fetched HTML page with metadata (used in storage mode)
#[derive(Debug, Clone)]
pub struct FetchedPage {
    pub url: String,
    pub category_name: String,
    pub page_number: Option<u32>,
    pub html_content: String,
    pub storage_key: String,
    pub category_path: String,
    pub page_name: String,
}

impl HtmlFetcher {
    /// Create HtmlFetcher with default HTTP configuration (for backward compatibility)
    /// This creates a direct-mode fetcher (no storage)
    pub fn new(config: HtmlConfig) -> Result<Self> {
        // Create a runtime handle to initialize async components
        let rt = tokio::runtime::Handle::try_current().map_err(|_| {
            anyhow::anyhow!("HtmlFetcher::new requires a tokio runtime. Use new_async instead.")
        })?;

        rt.block_on(Self::new_async(config))
    }

    /// Create HtmlFetcher in direct mode (fetch and scrape immediately)
    pub async fn new_async(config: HtmlConfig) -> Result<Self> {
        // Determine source name from config
        let source_name = if config.site.name.is_empty() {
            "unknown_site"
        } else {
            &config.site.name
        };
        let http_fetcher = HttpFetcher::new_for_source(source_name).await?;
        let extractor = HtmlExtractor::new(config.clone());

        Ok(HtmlFetcher {
            http_fetcher,
            extractor,
            config,
            storage: None,
            storage_mode: false,
        })
    }

    /// Create HtmlFetcher in storage mode (fetch HTML, store, then scrape from storage)
    pub async fn new_with_storage(config: HtmlConfig, storage: Arc<MinioStorage>) -> Result<Self> {
        // Determine source name from config
        let source_name = if config.site.name.is_empty() {
            "unknown_site"
        } else {
            &config.site.name
        };
        let http_fetcher = HttpFetcher::new_for_source(source_name).await?;
        let extractor = HtmlExtractor::new(config.clone());

        Ok(HtmlFetcher {
            http_fetcher,
            extractor,
            config,
            storage: Some(storage),
            storage_mode: true,
        })
    }

    /// Create HtmlFetcher with custom HTTP fetcher (for testing or advanced usage)
    pub fn with_http_fetcher(config: HtmlConfig, http_fetcher: HttpFetcher) -> Self {
        let extractor = HtmlExtractor::new(config.clone());
        HtmlFetcher {
            http_fetcher,
            extractor,
            config,
            storage: None,
            storage_mode: false,
        }
    }

    /// Fetch products from all configured categories
    /// In storage mode: fetches HTML, stores it, then scrapes from storage
    /// In direct mode: fetches HTML and scrapes immediately
    pub async fn fetch_all_categories(&self) -> Result<Vec<ScrapedProduct>> {
        if self.storage_mode {
            // Storage mode: fetch HTML, store, then scrape from storage
            self.fetch_store_and_scrape_all_categories().await
        } else {
            // Direct mode: fetch and scrape immediately
            self.fetch_and_scrape_all_categories_direct().await
        }
    }

    /// Direct mode: Fetch and scrape immediately (original behavior)
    async fn fetch_and_scrape_all_categories_direct(&self) -> Result<Vec<ScrapedProduct>> {
        let mut all_products = Vec::new();

        for (category_name, category_config) in &self.config.categories {
            info!("Scraping category: {}", category_name);

            match self.scrape_category_direct(category_name, category_config).await {
                Ok(products) => {
                    info!("Scraped {} products from {}", products.len(), category_name);
                    all_products.extend(products);
                }
                Err(e) => {
                    error!("Failed to scrape category {}: {}", category_name, e);
                    continue;
                }
            }

            // Rate limiting between categories
            let delay = Duration::from_millis(
                self.config.scraping.delay_between_requests_ms + (rand::random::<u64>() % 1000),
            );
            sleep(delay).await;
        }

        Ok(all_products)
    }

    /// Storage mode: Fetch HTML, store in S3, then scrape from storage
    async fn fetch_store_and_scrape_all_categories(&self) -> Result<Vec<ScrapedProduct>> {
        let storage = self.storage.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Storage not configured for storage mode"))?;

        // Step 1: Fetch and store HTML pages
        info!("🔄 Step 1: Fetching and storing HTML pages");
        let _fetched_pages = self.fetch_and_store_all_categories().await?;

        // Step 2: Process stored HTML pages
        info!("🔄 Step 2: Processing stored HTML pages");
        let processor = HtmlPageProcessor::new(self.config.clone(), storage.clone())?;
        let products = processor.process_all_stored_pages().await?;

        Ok(products)
    }

    /// Fetch and store HTML pages for all configured categories (storage mode)
    async fn fetch_and_store_all_categories(&self) -> Result<Vec<FetchedPage>> {
        let mut all_pages = Vec::new();

        for (category_name, category_config) in &self.config.categories {
            info!("Fetching HTML pages for category: {}", category_name);

            match self.fetch_and_store_category(category_name, category_config).await {
                Ok(pages) => {
                    info!("Fetched {} pages from {}", pages.len(), category_name);
                    all_pages.extend(pages);
                }
                Err(e) => {
                    error!("Failed to fetch category {}: {}", category_name, e);
                    continue;
                }
            }

            // Rate limiting between categories
            let delay = Duration::from_millis(
                self.config.scraping.delay_between_requests_ms + (rand::random::<u64>() % 1000),
            );
            sleep(delay).await;
        }

        Ok(all_pages)
    }

    /// Fetch and store HTML pages for a specific category (storage mode)
    async fn fetch_and_store_category(
        &self,
        category_name: &str,
        category_config: &crate::config::html_config::CategoryConfig,
    ) -> Result<Vec<FetchedPage>> {
        let storage = self.storage.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Storage not configured for storage mode"))?;

        let mut all_pages = Vec::new();

        // Extract category path and page name from URL
        let (category_path, page_name) = self.extract_path_info(&category_config.base_url);

        // First, fetch the first page to determine total page count
        let first_page_url = category_config.base_url.clone();
        info!("Fetching page 1 of {}: {}", category_name, first_page_url);

        let first_page_html = match self.fetch_page_with_retry(&first_page_url, 3).await {
            Ok(html) => html,
            Err(e) => {
                error!("Failed to fetch first page of {}: {}", category_name, e);
                return Ok(all_pages);
            }
        };

        // Store first page
        let storage_key = storage.store_raw_html(
            "naheed",
            &category_path,
            &page_name,
            None,
            &first_page_html,
        ).await?;

        all_pages.push(FetchedPage {
            url: first_page_url.clone(),
            category_name: category_name.to_string(),
            page_number: None,
            html_content: first_page_html.clone(),
            storage_key,
            category_path: category_path.clone(),
            page_name: page_name.clone(),
        });

        // Extract total page count from first page
        let total_pages = self
            .extract_total_page_count(&first_page_html)
            .unwrap_or_else(|| {
                info!(
                    "Could not extract total page count for {}, using configured max_pages",
                    category_name
                );
                self.config.scraping.max_pages_per_category
            });

        info!(
            "Found {} total pages for category {}",
            total_pages, category_name
        );

        // Fetch remaining pages (pages 2 to total_pages)
        for page in 2..=total_pages {
            let url = format!("{}?p={}", category_config.base_url, page);
            info!("Fetching page {} of {}: {}", page, category_name, url);

            match self.fetch_and_store_page(&url, category_name, &category_path, &page_name, Some(page as u32)).await {
                Ok(fetched_page) => {
                    all_pages.push(fetched_page);
                }
                Err(e) => {
                    error!("Failed to fetch page {} of {}: {}", page, category_name, e);
                    continue;
                }
            }

            // Rate limiting between pages
            let delay = Duration::from_millis(self.config.scraping.delay_between_requests_ms);
            sleep(delay).await;
        }

        Ok(all_pages)
    }

    /// Scrape a specific category (direct mode)
    async fn scrape_category_direct(
        &self,
        category_name: &str,
        category_config: &crate::config::html_config::CategoryConfig,
    ) -> Result<Vec<ScrapedProduct>> {
        let mut all_products = Vec::new();

        // First, fetch the first page to determine total page count
        let first_page_url = category_config.base_url.clone();
        info!("Scraping page 1 of {}: {}", category_name, first_page_url);

        let first_page_html = match self.fetch_page_with_retry(&first_page_url, 3).await {
            Ok(html) => html,
            Err(e) => {
                error!("Failed to fetch first page of {}: {}", category_name, e);
                return Ok(all_products);
            }
        };

        // Extract total page count from first page
        let total_pages = self
            .extract_total_page_count(&first_page_html)
            .unwrap_or_else(|| {
                info!(
                    "Could not extract total page count for {}, using configured max_pages",
                    category_name
                );
                self.config.scraping.max_pages_per_category
            });

        info!(
            "Found {} total pages for category {}",
            total_pages, category_name
        );

        // Process first page
        match self.extractor.extract_products_from_html(
            &first_page_html,
            category_name,
            Some(first_page_url),
        ) {
            Ok(products) => {
                info!(
                    "Scraped {} products from page 1 of {}",
                    products.len(),
                    category_name
                );
                all_products.extend(products);
            }
            Err(e) => {
                warn!(
                    "Failed to extract products from page 1 of {}: {}",
                    category_name, e
                );
            }
        }

        // Process remaining pages if there are more than 1
        let max_pages_to_process =
            std::cmp::min(total_pages, self.config.scraping.max_pages_per_category);

        for page in 2..=max_pages_to_process {
            let url = format!("{}?p={}", category_config.base_url, page);
            info!(
                "Scraping page {} of {} (total: {}): {}",
                page, category_name, total_pages, url
            );

            // Rate limiting between pages
            let delay = Duration::from_millis(
                self.config.scraping.delay_between_requests_ms + (rand::random::<u64>() % 2000),
            );
            sleep(delay).await;

            match self.scrape_page(&url, category_name).await {
                Ok(products) => {
                    info!(
                        "Scraped {} products from page {} of {}",
                        products.len(),
                        page,
                        category_name
                    );
                    all_products.extend(products);
                }
                Err(e) => {
                    warn!("Failed to scrape page {} of {}: {}", page, category_name, e);
                    // Continue with next page instead of breaking, since we know total page count
                    continue;
                }
            }
        }

        Ok(all_products)
    }

    /// Fetch and store a single page (storage mode)
    async fn fetch_and_store_page(
        &self,
        url: &str,
        category_name: &str,
        category_path: &str,
        page_name: &str,
        page_number: Option<u32>,
    ) -> Result<FetchedPage> {
        let storage = self.storage.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Storage not configured for storage mode"))?;

        let html = self.fetch_page_with_retry(url, 3).await?;

        let storage_key = storage.store_raw_html(
            "naheed",
            category_path,
            page_name,
            page_number,
            &html,
        ).await?;

        Ok(FetchedPage {
            url: url.to_string(),
            category_name: category_name.to_string(),
            page_number,
            html_content: html,
            storage_key,
            category_path: category_path.to_string(),
            page_name: page_name.to_string(),
        })
    }

    /// Extract category path and page name from Naheed URL
    /// Example: https://www.naheed.pk/groceries-pets/fresh-products/fruits
    /// Returns: ("groceries-pets/fresh-products", "fruits")
    fn extract_path_info(&self, url: &str) -> (String, String) {
        if let Some(naheed_path) = url.strip_prefix("https://www.naheed.pk/") {
            let path_segments: Vec<&str> = naheed_path.split('/').collect();

            if path_segments.len() >= 3 {
                let category_path = path_segments[..path_segments.len()-1].join("/");
                let page_name = path_segments.last().unwrap().to_string();
                (category_path, page_name)
            } else {
                ("unknown".to_string(), "unknown".to_string())
            }
        } else {
            ("unknown".to_string(), "unknown".to_string())
        }
    }

    /// Scrape a single page (direct mode)
    async fn scrape_page(&self, url: &str, category_name: &str) -> Result<Vec<ScrapedProduct>> {
        let html = self.fetch_page_with_retry(url, 3).await?;
        // Delegate to extractor
        self.extractor
            .extract_products_from_html(&html, category_name, Some(url.to_string()))
    }

    /// Fetch HTML page with retry logic (delegate to HTTP fetcher)
    async fn fetch_page_with_retry(&self, url: &str, _max_retries: usize) -> Result<String> {
        // The HttpFetcher already has retry logic built-in
        self.http_fetcher.get_html_smart(url).await
    }

    /// Extract total page count from HTML using pagination selectors
    fn extract_total_page_count(&self, html: &str) -> Option<usize> {
        let document = Html::parse_document(html);

        // Try each pagination selector from config
        for selector_str in &self.config.selectors.pagination_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                // For #am-page-count, extract direct page count
                if selector_str == "#am-page-count" {
                    if let Some(count) = document
                        .select(&selector)
                        .next()
                        .and_then(|element| element.text().collect::<String>().trim().parse().ok())
                    {
                        info!("✅ Found total page count from #am-page-count: {}", count);
                        return Some(count);
                    }
                } else {
                    // For pagination containers, find the highest page number from links
                    if let Some(count) = self.extract_page_count_from_links(&document, &selector) {
                        info!(
                            "✅ Found total page count from pagination links ({}): {}",
                            selector_str, count
                        );
                        return Some(count);
                    }
                }
            }
        }

        info!("⚠️ Could not extract total page count from any pagination selectors");
        None
    }

    /// Extract page count from pagination links (e.g., "1 2 3 4 5 Next")
    fn extract_page_count_from_links(&self, document: &Html, selector: &Selector) -> Option<usize> {
        let mut max_page = 1;

        for pagination_container in document.select(selector) {
            // Look for page number links within the pagination container
            let link_selector = Selector::parse("a, span").ok()?;

            for element in pagination_container.select(&link_selector) {
                let text_string = element.text().collect::<String>();
                let text = text_string.trim();

                // Try to parse as page number, skip non-numeric text like "Next", "Previous"
                if let Ok(page_num) = text.parse::<usize>() {
                    max_page = max_page.max(page_num);
                }
            }
        }

        // Only return if we found pages beyond 1
        if max_page > 1 { Some(max_page) } else { None }
    }

    // HTTP fetching and data extraction methods moved to HttpFetcher and HtmlExtractor
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::html_config::{ScrapingConfig, SelectorConfig, SiteConfig};
    use std::collections::HashMap;

    fn create_test_config() -> HtmlConfig {
        HtmlConfig {
            site: SiteConfig {
                name: "Test Site".to_string(),
                base_url: "https://example.com".to_string(),
                user_agent: None,
            },
            scraping: ScrapingConfig::default(),
            selectors: SelectorConfig {
                product_selectors: vec!["li.item".to_string()],
                name_selectors: vec!["h2".to_string()],
                price_selectors: vec![".price".to_string()],
                category_selectors: vec!["h1".to_string()],
                pagination_selectors: vec!["#am-page-count".to_string(), ".pagination".to_string()],
            },
            categories: HashMap::new(),
            fields: None,
            extraction_rules: None,
            exclusion: None,
            ml_model: None,
            anti_bot: None,
            headers: None,
        }
    }

    #[test]
    fn test_extract_page_count_from_am_page_count() {
        let config = create_test_config();
        let fetcher = HtmlFetcher {
            http_fetcher: HttpFetcher::default(),
            extractor: HtmlExtractor::new(config.clone()),
            config,
            storage: None,
            storage_mode: false,
        };

        let html = r#"
            <html>
                <body>
                    <div id="am-page-count" style="display: none">5</div>
                    <div class="products">
                        <li class="item">Product 1</li>
                        <li class="item">Product 2</li>
                    </div>
                </body>
            </html>
        "#;

        let result = fetcher.extract_total_page_count(html);
        assert_eq!(result, Some(5));
    }

    #[test]
    fn test_extract_page_count_from_pagination_links() {
        let config = create_test_config();
        let fetcher = HtmlFetcher {
            http_fetcher: HttpFetcher::default(),
            extractor: HtmlExtractor::new(config.clone()),
            config,
            storage: None,
            storage_mode: false,
        };

        let html = r#"
            <html>
                <body>
                    <div class="pagination">
                        <a href="?p=1">1</a>
                        <a href="?p=2">2</a>
                        <a href="?p=3">3</a>
                        <span>4</span>
                        <a href="?p=5">5</a>
                        <a href="?p=6">Next</a>
                    </div>
                </body>
            </html>
        "#;

        let result = fetcher.extract_total_page_count(html);
        assert_eq!(result, Some(5));
    }

    #[test]
    fn test_extract_page_count_no_pagination() {
        let config = create_test_config();
        let fetcher = HtmlFetcher {
            http_fetcher: HttpFetcher::default(),
            extractor: HtmlExtractor::new(config.clone()),
            config,
            storage: None,
            storage_mode: false,
        };

        let html = r#"
            <html>
                <body>
                    <div class="products">
                        <li class="item">Product 1</li>
                        <li class="item">Product 2</li>
                    </div>
                </body>
            </html>
        "#;

        let result = fetcher.extract_total_page_count(html);
        assert_eq!(result, None);
    }
}
