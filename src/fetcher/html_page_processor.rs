use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::config::HtmlConfig;
use crate::extractor::{HtmlExtractor, ScrapedProduct};
use crate::storage::MinioStorage;

/// HTML Page Processor - Stage 2: Read HTML pages from S3 and scrape them
/// This processor reads stored HTML pages and extracts product data
pub struct HtmlPageProcessor {
    extractor: HtmlExtractor,
    storage: Arc<MinioStorage>,
    config: HtmlConfig,
}

/// Represents a stored HTML page with metadata for processing
#[derive(Debug, Clone)]
pub struct StoredPage {
    pub storage_key: String,
    pub category_name: String,
    pub url: String,
    pub page_number: Option<u32>,
    pub category_path: String,
    pub page_name: String,
}

impl HtmlPageProcessor {
    /// Create a new HTML page processor
    pub fn new(config: HtmlConfig, storage: Arc<MinioStorage>) -> Result<Self> {
        let extractor = HtmlExtractor::new(config.clone());
        
        Ok(Self {
            extractor,
            storage,
            config,
        })
    }

    /// Process all stored HTML pages for all categories
    pub async fn process_all_stored_pages(&self) -> Result<Vec<ScrapedProduct>> {
        let mut all_products = Vec::new();

        // Get list of stored pages from S3
        let stored_pages = self.discover_stored_pages().await?;
        
        info!("Found {} stored HTML pages to process", stored_pages.len());

        for stored_page in stored_pages {
            info!("Processing stored page: {}", stored_page.storage_key);

            match self.process_stored_page(&stored_page).await {
                Ok(products) => {
                    info!(
                        "Extracted {} products from stored page: {}",
                        products.len(),
                        stored_page.storage_key
                    );
                    all_products.extend(products);
                }
                Err(e) => {
                    error!(
                        "Failed to process stored page {}: {}",
                        stored_page.storage_key, e
                    );
                    continue;
                }
            }
        }

        Ok(all_products)
    }

    /// Process a specific stored HTML page
    async fn process_stored_page(&self, stored_page: &StoredPage) -> Result<Vec<ScrapedProduct>> {
        // Read HTML content from S3
        let html_content = self.storage.get_raw_content(&stored_page.storage_key).await?;
        
        // Convert bytes to string
        let html_str = String::from_utf8(html_content)
            .map_err(|e| anyhow::anyhow!("Failed to convert HTML bytes to string: {}", e))?;

        // Extract products using the HTML extractor
        self.extractor.extract_products_from_html(
            &html_str,
            &stored_page.category_name,
            Some(stored_page.url.clone()),
        )
    }

    /// Discover stored HTML pages in S3
    /// This method lists all HTML files in the raw storage for today
    async fn discover_stored_pages(&self) -> Result<Vec<StoredPage>> {
        let mut stored_pages = Vec::new();

        // Get today's date for the storage path
        let today = chrono::Utc::now().format("%Y/%m/%d").to_string();
        let base_prefix = format!("{}/raw/naheed/", today);

        // List all objects with the base prefix
        let object_keys = self.storage.list_objects_with_prefix(&base_prefix).await?;

        for key in object_keys {
            // Skip JSON files, only process HTML files
            if key.ends_with(".json") {
                continue;
            }

            // Parse the storage key to extract metadata
            if let Some(stored_page) = self.parse_storage_key(&key) {
                stored_pages.push(stored_page);
            } else {
                warn!("Could not parse storage key: {}", key);
            }
        }

        // Sort by category and page number for consistent processing order
        stored_pages.sort_by(|a, b| {
            a.category_name.cmp(&b.category_name)
                .then(a.page_number.unwrap_or(1).cmp(&b.page_number.unwrap_or(1)))
        });

        Ok(stored_pages)
    }

    /// Parse a storage key to extract page metadata
    /// Example key: "2025/09/30/raw/naheed/groceries-pets/fresh-products/fruits.html"
    /// Example key: "2025/09/30/raw/naheed/groceries-pets/fresh-products/fruits.html?p=2"
    fn parse_storage_key(&self, key: &str) -> Option<StoredPage> {
        // Split the key into parts
        let parts: Vec<&str> = key.split('/').collect();
        
        // Expected format: YYYY/MM/DD/raw/naheed/category_path.../page_name.html[?p=N]
        if parts.len() < 6 || parts[3] != "raw" || parts[4] != "naheed" {
            return None;
        }

        // Extract the file name (last part)
        let file_name = parts.last()?;
        
        // Parse page number from file name if present
        let (base_file_name, page_number) = if file_name.contains("?p=") {
            let file_parts: Vec<&str> = file_name.split("?p=").collect();
            let base_name = file_parts[0];
            let page_num = file_parts.get(1)?.parse::<u32>().ok()?;
            (base_name, Some(page_num))
        } else {
            (*file_name, None)
        };

        // Extract page name (remove .html extension)
        let page_name = base_file_name.strip_suffix(".html")?.to_string();

        // Extract category path (everything between naheed/ and the file name)
        let category_path_parts = &parts[5..parts.len()-1];
        let category_path = category_path_parts.join("/");

        // Reconstruct the URL
        let url = if let Some(page_num) = page_number {
            format!("https://www.naheed.pk/{}/{}?p={}",
                   category_path, page_name, page_num)
        } else {
            format!("https://www.naheed.pk/{}/{}",
                   category_path, page_name)
        };

        // Find matching category name from config
        let category_name = self.find_category_name_for_url(&url)?;

        Some(StoredPage {
            storage_key: key.to_string(),
            category_name,
            url,
            page_number,
            category_path,
            page_name,
        })
    }

    /// Find the category name that matches a given URL
    fn find_category_name_for_url(&self, url: &str) -> Option<String> {
        // Remove page parameter for matching
        let base_url = url.split('?').next()?;
        
        for (category_name, category_config) in &self.config.categories {
            if category_config.base_url == base_url {
                return Some(category_name.clone());
            }
        }
        
        // If no exact match, try to infer from URL path
        if let Some(path) = base_url.strip_prefix("https://www.naheed.pk/") {
            let segments: Vec<&str> = path.split('/').collect();
            if let Some(last_segment) = segments.last() {
                // Use the last segment as category name
                return Some(last_segment.to_string());
            }
        }
        
        None
    }

    /// Process stored pages for a specific category
    pub async fn process_category_from_storage(&self, category_name: &str) -> Result<Vec<ScrapedProduct>> {
        let stored_pages = self.discover_stored_pages().await?;
        let mut category_products = Vec::new();

        for stored_page in stored_pages {
            if stored_page.category_name == category_name {
                match self.process_stored_page(&stored_page).await {
                    Ok(products) => {
                        info!(
                            "Extracted {} products from {} page {}",
                            products.len(),
                            category_name,
                            stored_page.page_number.unwrap_or(1)
                        );
                        category_products.extend(products);
                    }
                    Err(e) => {
                        error!(
                            "Failed to process {} page {}: {}",
                            category_name,
                            stored_page.page_number.unwrap_or(1),
                            e
                        );
                    }
                }
            }
        }

        Ok(category_products)
    }

    /// Get statistics about stored pages
    pub async fn get_storage_stats(&self) -> Result<StorageStats> {
        let stored_pages = self.discover_stored_pages().await?;
        
        let mut categories = std::collections::HashMap::new();
        let mut total_pages = 0;

        for page in stored_pages {
            total_pages += 1;
            *categories.entry(page.category_name).or_insert(0) += 1;
        }

        Ok(StorageStats {
            total_pages,
            categories,
        })
    }
}

/// Statistics about stored HTML pages
#[derive(Debug)]
pub struct StorageStats {
    pub total_pages: usize,
    pub categories: std::collections::HashMap<String, usize>,
}
