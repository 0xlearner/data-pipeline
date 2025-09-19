use anyhow::{Result, anyhow};
use wreq::{Client, Response};
use wreq_util::Emulation;
use serde_json::Value;
use tracing::{info, error};
use crate::config::ApiConfig;

pub struct HttpFetcher {
    client: Client,
    config: ApiConfig,
}

impl HttpFetcher {
    pub fn new(config: ApiConfig) -> Result<Self> {
        let client = Client::builder()
            .emulation(Emulation::Firefox139)
            .build()?;
        
        Ok(HttpFetcher { client, config })
    }

    pub async fn fetch_all_categories(&self) -> Result<Vec<Value>> {
        let mut all_data = Vec::new();
        let category_urls = self.config.build_category_urls();
        
        for (category_key, url) in category_urls {
            info!("Fetching category: {}", category_key);
            match self.fetch_paginated(&url).await {
                Ok(data) => {
                    info!("Fetched {} products from {}", data.len(), category_key);
                    all_data.extend(data);
                }
                Err(e) => {
                    error!("Failed to fetch category {}: {}", category_key, e);
                    // Continue with other categories even if one fails
                }
            }
        }
        
        Ok(all_data)
    }

    pub async fn fetch_paginated(&self, endpoint: &str) -> Result<Vec<Value>> {
        let mut all_data = Vec::new();
        let mut page = 1;
        
        loop {
            let url = format!("{}?{}={}", 
                endpoint,
                self.config.pagination.page_param,
                page
            );

            let url = if let Some(limit) = self.config.pagination.default_limit {
                format!("{}&{}={}", url, self.config.pagination.limit_param.as_deref().unwrap_or("limit"), limit)
            } else {
                url
            };

            info!("Fetching page {} from {}", page, url);
            
            match self.fetch_with_auth(&url).await {
                Ok(response) => {
                    let data: Value = response.json().await?;
                    let products = self.extract_products(&data)?;
                    
                    if products.is_empty() {
                        break;
                    }
                    
                    all_data.extend(products);
                    page += 1;
                }
                Err(e) => {
                    error!("Failed to fetch page {}: {}", page, e);
                    break;
                }
            }
        }
        
        Ok(all_data)
    }

    async fn fetch_with_auth(&self, url: &str) -> Result<Response> {
        let response = self.client
            .get(url)
            .header("Authorization", format!("Bearer {}", self.config.api.auth_token))
            .header("Content-Type", "application/json")
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("HTTP error: {}", response.status()));
        }
        
        Ok(response)
    }

    fn extract_products(&self, data: &Value) -> Result<Vec<Value>> {
        let mut products = Vec::new();

        // Navigate through the structure: object►data►0►l2_products►0►
        if let Some(data_array) = data.get("data").and_then(|d| d.as_array()) {
            for data_item in data_array {
                if let Some(krave_mart_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                    for product in krave_mart_products {
                        products.push(product.clone());
                    }
                }
            }
        }

        Ok(products)
    }
}