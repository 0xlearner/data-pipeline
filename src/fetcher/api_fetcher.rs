use anyhow::{Result, anyhow};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::ApiConfig;
use crate::extractor::ApiExtractor;
use crate::fetcher::HttpFetcher;

/// Refactored ApiFetcher that coordinates HTTP fetching and data extraction
/// Delegates HTTP operations to HttpFetcher and data processing to ApiExtractor
pub struct ApiFetcher {
    http_fetcher: HttpFetcher,
    extractor: ApiExtractor,
    config: ApiConfig,
}

impl ApiFetcher {
    /// Create ApiFetcher with default HTTP configuration (for backward compatibility)
    pub fn new(config: ApiConfig) -> Result<Self> {
        // Create a runtime handle to initialize async components
        let rt = tokio::runtime::Handle::try_current().map_err(|_| {
            anyhow::anyhow!("ApiFetcher::new requires a tokio runtime. Use new_async instead.")
        })?;

        rt.block_on(Self::new_async(config))
    }

    /// Create ApiFetcher with global configuration (preferred method)
    pub async fn new_async(config: ApiConfig) -> Result<Self> {
        // Determine source name from config (use api.name if available, otherwise derive from base_url)
        let source_name = if config.api.name.is_empty() {
            "unknown_api"
        } else {
            &config.api.name
        };

        let mut http_fetcher = HttpFetcher::new_for_source(source_name).await?;
        if !config.request.headers.is_empty() {
            http_fetcher = http_fetcher.with_headers(config.request.headers.clone());
        }
        let extractor = ApiExtractor::new(config.clone());

        Ok(ApiFetcher {
            http_fetcher,
            extractor,
            config,
        })
    }

    /// Create ApiFetcher with custom HTTP fetcher (for testing or advanced usage)
    pub fn with_http_fetcher(config: ApiConfig, http_fetcher: HttpFetcher) -> Self {
        let extractor = ApiExtractor::new(config.clone());
        ApiFetcher {
            http_fetcher,
            extractor,
            config,
        }
    }

    pub async fn fetch_all_categories(&self) -> Result<Vec<Value>> {
        let mut all_data = Vec::new();

        match self.config.request.method.as_str() {
            "GET" => {
                let category_urls = self.config.build_category_urls();
                for (category_key, url) in category_urls {
                    info!("Fetching GET category: {}", category_key);

                    // Check if pagination is disabled
                    let data = if self.config.pagination.r#type == "none" {
                        match self.fetch_get_single(&url).await {
                            Ok(data) => data,
                            Err(e) => {
                                error!("Failed to fetch category {}: {}", category_key, e);
                                continue;
                            }
                        }
                    } else {
                        match self.fetch_get_paginated(&url).await {
                            Ok(data) => data,
                            Err(e) => {
                                error!("Failed to fetch category {}: {}", category_key, e);
                                continue;
                            }
                        }
                    };

                    info!("Fetched {} products from {}", data.len(), category_key);
                    all_data.extend(data);
                }
            }
            "POST" => {
                // Check if this is a GraphQL API
                if self.config.request.graphql_query.is_some() {
                    // GraphQL API (like Pandamart)
                    for (category_key, category) in &self.config.categories {
                        if let Some(ref category_id) = category.category_id {
                            info!("Fetching GraphQL category: {}", category_key);
                            match self.fetch_graphql_single(category_id).await {
                                Ok(data) => {
                                    info!("Fetched {} products from {}", data.len(), category_key);
                                    all_data.extend(data);
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to fetch GraphQL category {}: {}",
                                        category_key, e
                                    );
                                }
                            }
                        }
                    }
                } else {
                    // Regular POST API (like BazaarApp)
                    let category_slugs = self.config.get_category_slugs();
                    for (category_key, category_slug) in category_slugs {
                        info!("Fetching POST category: {}", category_key);
                        match self.fetch_post_paginated(&category_slug).await {
                            Ok(data) => {
                                info!("Fetched {} products from {}", data.len(), category_key);
                                all_data.extend(data);
                            }
                            Err(e) => {
                                error!("Failed to fetch category {}: {}", category_key, e);
                            }
                        }
                    }
                }
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported HTTP method: {}",
                    self.config.request.method
                ));
            }
        }

        Ok(all_data)
    }

    // Method for single GET requests (no pagination)
    pub async fn fetch_get_single(&self, url: &str) -> Result<Vec<Value>> {
        info!("Fetching single GET request from: {}", url);

        // Handle potential API errors gracefully
        let data = match self.fetch_with_get(url).await {
            Ok(json) => json,
            Err(e) => {
                return Err(anyhow!("Failed to fetch from {}: {}", url, e));
            }
        };

        let products = self.extract_products(&data)?;
        info!("Found {} products in single request", products.len());

        Ok(products)
    }

    pub async fn fetch_get_paginated(&self, url: &str) -> Result<Vec<Value>> {
        let mut all_products = Vec::new();
        let mut page = 1; // KraveMart uses 1-based pagination
        let max_pages = 50; // Safety limit to prevent infinite loops

        loop {
            // Safety check to prevent infinite loops
            if page > max_pages {
                warn!(
                    "Reached maximum page limit ({}) for URL {}, stopping",
                    max_pages, url
                );
                break;
            }

            let paginated_url = format!("{}?page={}", url, page);
            info!("Fetching GET page {} from: {}", page, paginated_url);

            // Handle potential API errors gracefully
            let data = match self.fetch_with_get(&paginated_url).await {
                Ok(json) => json,
                Err(e) => {
                    warn!(
                        "Failed to fetch page {} from {}: {}",
                        page, paginated_url, e
                    );
                    // Don't stop for a single failure, but break if it continues
                    if page > 1 {
                        // if it's not the first page, we can probably stop
                        break;
                    }
                    page += 1;
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            let products = self.extract_products(&data)?;

            if !products.is_empty() {
                info!("Found {} products on page {}", products.len(), page);
                all_products.extend(products);
            }

            if !self.extractor.has_more_pages(&data, page) {
                break;
            }

            page += 1;

            // Rate limiting
            sleep(Duration::from_millis(500)).await;
        }

        info!(
            "Completed pagination: {} total products across {} pages",
            all_products.len(),
            page
        );

        Ok(all_products)
    }

    pub async fn fetch_post_paginated(&self, category_slug: &str) -> Result<Vec<Value>> {
        let mut all_products = Vec::new();
        let mut page = 0; // BazaarApp uses 0-based pagination
        let mut consecutive_empty_pages = 0;
        let max_consecutive_empty = 2; // Stop after 2 consecutive empty responses
        let max_pages = 50; // Safety limit to prevent infinite loops

        loop {
            // Safety check to prevent infinite loops
            if page >= max_pages {
                warn!(
                    "Reached maximum page limit ({}) for category {}, stopping",
                    max_pages, category_slug
                );
                break;
            }

            info!("Fetching POST page {} for category {}", page, category_slug);

            let request_body = self
                .config
                .build_pagination_request_body(category_slug, page)?;

            // Handle potential API errors gracefully
            let data = match self.fetch_with_post(&request_body).await {
                Ok(json) => json,
                Err(e) => {
                    warn!(
                        "Failed to fetch page {} for category {}: {}",
                        page, category_slug, e
                    );
                    consecutive_empty_pages += 1;
                    if consecutive_empty_pages >= max_consecutive_empty {
                        info!(
                            "Too many consecutive failures, stopping pagination for category {}",
                            category_slug
                        );
                        break;
                    }
                    page += 1;
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            let products = self.extract_products(&data)?;

            if products.is_empty() {
                consecutive_empty_pages += 1;
                info!(
                    "No products found on page {} for category {} (consecutive empty: {})",
                    page, category_slug, consecutive_empty_pages
                );

                if consecutive_empty_pages >= max_consecutive_empty {
                    info!(
                        "Reached {} consecutive empty pages, stopping pagination for category {}",
                        max_consecutive_empty, category_slug
                    );
                    break;
                }
            } else {
                // Reset consecutive empty counter when we find products
                consecutive_empty_pages = 0;
                info!(
                    "Found {} products on page {} for category {}",
                    products.len(),
                    page,
                    category_slug
                );
                all_products.extend(products);
            }

            page += 1;

            // Rate limiting
            sleep(Duration::from_millis(500)).await;
        }

        info!(
            "Completed pagination for category {}: {} total products across {} pages",
            category_slug,
            all_products.len(),
            page
        );

        Ok(all_products)
    }

    // Method for GraphQL POST requests (like Pandamart)
    pub async fn fetch_graphql_single(&self, category_id: &str) -> Result<Vec<Value>> {
        info!("Fetching GraphQL request for category: {}", category_id);

        let request_body = self.extractor.build_graphql_request_body(category_id)?;

        // Handle potential API errors gracefully
        let data = match self.fetch_with_post(&request_body).await {
            Ok(json) => json,
            Err(e) => {
                return Err(anyhow!(
                    "Failed to fetch GraphQL for category {}: {}",
                    category_id,
                    e
                ));
            }
        };

        let products = self.extract_products(&data)?;
        info!(
            "Found {} products in GraphQL request for category {}",
            products.len(),
            category_id
        );

        Ok(products)
    }

    async fn fetch_with_get(&self, url: &str) -> Result<Value> {
        // Delegate to HTTP fetcher
        self.http_fetcher.get_json(url).await
    }

    async fn fetch_with_post(&self, request_body: &Value) -> Result<Value> {
        let url = self.config.build_request_url();
        let body = request_body.to_string();

        // Delegate to HTTP fetcher
        self.http_fetcher.post_json(&url, &body).await
    }

    // All data processing methods have been moved to ApiExtractor
    // ApiFetcher now only coordinates between HttpFetcher and ApiExtractor

    fn extract_products(&self, data: &Value) -> Result<Vec<Value>> {
        // Delegate to data extractor
        self.extractor.extract_products(data)
    }
}
