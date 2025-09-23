use anyhow::{anyhow, Result};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use wreq::{Client, Response};
use wreq_util::Emulation;

use crate::config::ApiConfig;

pub struct UnifiedFetcher {
    client: Client,
    config: ApiConfig,
}

impl UnifiedFetcher {
    pub fn new(config: ApiConfig) -> Result<Self> {
        let client = Client::builder()
            .emulation(Emulation::Firefox136)
            .build()?;

        Ok(UnifiedFetcher { client, config })
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
            _ => {
                return Err(anyhow!("Unsupported HTTP method: {}", self.config.request.method));
            }
        }
        
        Ok(all_data)
    }
    
    // Method for single GET requests (no pagination)
    pub async fn fetch_get_single(&self, url: &str) -> Result<Vec<Value>> {
        info!("Fetching single GET request from: {}", url);

        // Handle potential API errors gracefully
        let response = match self.fetch_with_get(url).await {
            Ok(resp) => resp,
            Err(e) => {
                return Err(anyhow!("Failed to fetch from {}: {}", url, e));
            }
        };

        // Parse JSON response
        let data: Value = match response.json().await {
            Ok(json) => json,
            Err(e) => {
                return Err(anyhow!("Failed to parse JSON response from {}: {}", url, e));
            }
        };

        let products = self.extract_products(&data)?;
        info!("Found {} products in single request", products.len());

        Ok(products)
    }

    pub async fn fetch_get_paginated(&self, url: &str) -> Result<Vec<Value>> {
        let mut all_products = Vec::new();
        let mut page = 1; // KraveMart uses 1-based pagination
        let mut consecutive_empty_pages = 0;
        let max_consecutive_empty = 2; // Stop after 2 consecutive empty responses
        let max_pages = 50; // Safety limit to prevent infinite loops

        loop {
            // Safety check to prevent infinite loops
            if page > max_pages {
                warn!("Reached maximum page limit ({}) for URL {}, stopping", max_pages, url);
                break;
            }

            let paginated_url = format!("{}?page={}", url, page);
            info!("Fetching GET page {} from: {}", page, paginated_url);

            // Handle potential API errors gracefully
            let response = match self.fetch_with_get(&paginated_url).await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!("Failed to fetch page {} from {}: {}", page, paginated_url, e);
                    consecutive_empty_pages += 1;
                    if consecutive_empty_pages >= max_consecutive_empty {
                        info!("Too many consecutive failures, stopping pagination");
                        break;
                    }
                    page += 1;
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            // Parse JSON response
            let data: Value = match response.json().await {
                Ok(json) => json,
                Err(e) => {
                    warn!("Failed to parse JSON response for page {} from {}: {}", page, paginated_url, e);
                    consecutive_empty_pages += 1;
                    if consecutive_empty_pages >= max_consecutive_empty {
                        info!("Too many consecutive JSON parse failures, stopping pagination");
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
                info!("No products found on page {} (consecutive empty: {})", page, consecutive_empty_pages);

                if consecutive_empty_pages >= max_consecutive_empty {
                    info!("Reached {} consecutive empty pages, stopping pagination", max_consecutive_empty);
                    break;
                }
            } else {
                // Reset consecutive empty counter when we find products
                consecutive_empty_pages = 0;
                info!("Found {} products on page {}", products.len(), page);
                all_products.extend(products);
            }

            page += 1;

            // Rate limiting
            sleep(Duration::from_millis(500)).await;
        }

        info!("Completed pagination: {} total products across {} pages", all_products.len(), page - 1);

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
                warn!("Reached maximum page limit ({}) for category {}, stopping", max_pages, category_slug);
                break;
            }

            info!("Fetching POST page {} for category {}", page, category_slug);

            let request_body = self.build_post_request_body(category_slug, page)?;

            // Handle potential API errors gracefully
            let response = match self.fetch_with_post(&request_body).await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!("Failed to fetch page {} for category {}: {}", page, category_slug, e);
                    consecutive_empty_pages += 1;
                    if consecutive_empty_pages >= max_consecutive_empty {
                        info!("Too many consecutive failures, stopping pagination for category {}", category_slug);
                        break;
                    }
                    page += 1;
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            // Parse JSON response
            let data: Value = match response.json().await {
                Ok(json) => json,
                Err(e) => {
                    warn!("Failed to parse JSON response for page {} of category {}: {}", page, category_slug, e);
                    consecutive_empty_pages += 1;
                    if consecutive_empty_pages >= max_consecutive_empty {
                        info!("Too many consecutive JSON parse failures, stopping pagination for category {}", category_slug);
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
                info!("No products found on page {} for category {} (consecutive empty: {})",
                    page, category_slug, consecutive_empty_pages);

                if consecutive_empty_pages >= max_consecutive_empty {
                    info!("Reached {} consecutive empty pages, stopping pagination for category {}",
                        max_consecutive_empty, category_slug);
                    break;
                }
            } else {
                // Reset consecutive empty counter when we find products
                consecutive_empty_pages = 0;
                info!("Found {} products on page {} for category {}", products.len(), page, category_slug);
                all_products.extend(products);
            }

            page += 1;

            // Rate limiting
            sleep(Duration::from_millis(500)).await;
        }

        info!("Completed pagination for category {}: {} total products across {} pages",
            category_slug, all_products.len(), page);

        Ok(all_products)
    }

    async fn fetch_with_get(&self, url: &str) -> Result<Response> {
        let mut request = self.client.get(url);
        
        // Add authorization if configured
        if let Some(ref auth) = self.config.request.authorization {
            request = request.header("Authorization", auth);
        }
        
        // Add any additional headers
        for (key, value) in &self.config.request.headers {
            request = request.header(key, value);
        }
        
        let response = request.send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("HTTP error: {}", response.status()));
        }
        
        Ok(response)
    }

    async fn fetch_with_post(&self, request_body: &Value) -> Result<Response> {
        let url = self.config.build_request_url();
        
        let mut request = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(request_body);
        
        // Add authorization if configured
        if let Some(ref auth) = self.config.request.authorization {
            request = request.header("Authorization", auth);
        }
        
        // Add any additional headers
        for (key, value) in &self.config.request.headers {
            request = request.header(key, value);
        }
        
        let response = request.send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("HTTP error: {}", response.status()));
        }
        
        Ok(response)
    }

    fn build_post_request_body(&self, category_slug: &str, page: i32) -> Result<Value> {
        // Build request body matching BazaarApp's expected structure
        let body = serde_json::json!({
            "productChannel": self.config.request.product_channel.as_ref().unwrap_or(&"WEB_APP".to_string()),
            "paginationRequestDTO": {
                "page": page,
                "size": self.config.request.page_size.unwrap_or(20)
            },
            "searchKey": "",
            "brandIds": [],
            "coreCategorySlug": category_slug,
            "subcategorySlugs": []
        });

        Ok(body)
    }

    fn extract_products(&self, data: &Value) -> Result<Vec<Value>> {
        // Try different extraction patterns based on configuration
        if let Some(ref extraction_path) = self.config.response.data_path {
            return self.extract_by_path(data, extraction_path);
        }
        
        // Fallback to common patterns
        self.extract_by_common_patterns(data)
    }

    fn extract_by_path(&self, data: &Value, path: &str) -> Result<Vec<Value>> {
        let path_parts: Vec<&str> = path.split('.').collect();
        let mut current = data;
        
        for part in path_parts {
            if part.ends_with("[]") {
                // Array access
                let field = &part[..part.len()-2];
                if let Some(array) = current.get(field).and_then(|v| v.as_array()) {
                    return Ok(array.clone());
                } else {
                    return Ok(Vec::new());
                }
            } else {
                // Object access
                current = current.get(part).unwrap_or(&Value::Null);
            }
        }
        
        if let Some(array) = current.as_array() {
            Ok(array.clone())
        } else {
            Ok(Vec::new())
        }
    }

    fn extract_by_common_patterns(&self, data: &Value) -> Result<Vec<Value>> {
        // Pattern 1: Direct array (BazaarApp style)
        if let Some(products_array) = data.as_array() {
            return Ok(products_array.clone());
        }
        
        // Pattern 2: KraveMart style - data[].l2_products[]
        if let Some(data_array) = data.get("data").and_then(|d| d.as_array()) {
            let mut all_products = Vec::new();
            for item in data_array {
                if let Some(l2_products) = item.get("l2_products").and_then(|p| p.as_array()) {
                    all_products.extend(l2_products.clone());
                } else if let Some(krave_mart_products) = item.get("krave_mart_products").and_then(|p| p.as_array()) {
                    all_products.extend(krave_mart_products.clone());
                }
            }
            return Ok(all_products);
        }
        
        // Pattern 3: Simple products field
        if let Some(products) = data.get("products").and_then(|p| p.as_array()) {
            return Ok(products.clone());
        }
        
        // Pattern 4: Items field
        if let Some(items) = data.get("items").and_then(|i| i.as_array()) {
            return Ok(items.clone());
        }
        
        // If no pattern matches, return empty
        warn!("No products found in response structure");
        Ok(Vec::new())
    }
}
