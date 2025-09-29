use anyhow::Result;
use scraper::{Html, Selector};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::HtmlConfig;
use crate::extractor::{HtmlExtractor, ScrapedProduct};
use crate::fetcher::HttpFetcher;

/// Refactored HtmlFetcher that coordinates HTTP fetching and HTML extraction
/// Delegates HTTP operations to HttpFetcher and data processing to HtmlExtractor
pub struct HtmlFetcher {
    http_fetcher: HttpFetcher,
    extractor: HtmlExtractor,
    config: HtmlConfig,
}

impl HtmlFetcher {
    /// Create HtmlFetcher with default HTTP configuration (for backward compatibility)
    pub fn new(config: HtmlConfig) -> Result<Self> {
        // Create a runtime handle to initialize async components
        let rt = tokio::runtime::Handle::try_current().map_err(|_| {
            anyhow::anyhow!("HtmlFetcher::new requires a tokio runtime. Use new_async instead.")
        })?;

        rt.block_on(Self::new_async(config))
    }

    /// Create HtmlFetcher with global configuration (preferred method)
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
        })
    }

    /// Create HtmlFetcher with custom HTTP fetcher (for testing or advanced usage)
    pub fn with_http_fetcher(config: HtmlConfig, http_fetcher: HttpFetcher) -> Self {
        let extractor = HtmlExtractor::new(config.clone());
        HtmlFetcher {
            http_fetcher,
            extractor,
            config,
        }
    }

    /// Fetch products from all configured categories
    pub async fn fetch_all_categories(&self) -> Result<Vec<ScrapedProduct>> {
        let mut all_products = Vec::new();

        for (category_name, category_config) in &self.config.categories {
            info!("Scraping category: {}", category_name);

            match self.scrape_category(category_name, category_config).await {
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

    /// Scrape a specific category
    async fn scrape_category(
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

    /// Scrape a single page
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
