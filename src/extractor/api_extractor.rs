use anyhow::{Result, anyhow};
use serde_json::Value;
use tracing::{info, warn};

use crate::config::ApiConfig;

/// Pure data extractor for API responses
/// Only handles data extraction and transformation, no HTTP operations
pub struct ApiExtractor {
    config: ApiConfig,
}

impl ApiExtractor {
    pub fn new(config: ApiConfig) -> Self {
        Self { config }
    }

    /// Extract products from API response data
    pub fn extract_products(&self, data: &Value) -> Result<Vec<Value>> {
        if self.config.api.name == "pandamart" {
            let mut products = Vec::new();
            if let Some(category_products) = data
                .get("data")
                .and_then(|d| d.get("categoryProductList"))
                .and_then(|cpl| cpl.get("categoryProducts"))
                .and_then(|cp| cp.as_array())
            {
                for category_product in category_products {
                    if let Some(category_name) =
                        category_product.get("name").and_then(|n| n.as_str())
                    {
                        if let Some(items) =
                            category_product.get("items").and_then(|i| i.as_array())
                        {
                            for item in items {
                                if let Value::Object(mut map) = item.clone() {
                                    map.insert(
                                        "category_name".to_string(),
                                        Value::String(category_name.to_string()),
                                    );
                                    products.push(Value::Object(map));
                                }
                            }
                        }
                    }
                }
            }
            return Ok(products);
        }

        // Try different extraction patterns based on configuration
        if let Some(ref extraction_path) = self.config.response.data_path {
            let extracted = self.extract_by_path(data, extraction_path)?;
            // Flatten the result if it's a list of lists
            return Ok(extracted
                .into_iter()
                .flat_map(|v| {
                    if let Value::Array(arr) = v {
                        arr
                    } else {
                        vec![v]
                    }
                })
                .collect());
        }

        // Fallback to common patterns
        self.extract_by_common_patterns(data)
    }

    /// Extract data using configured path
    pub fn extract_by_path(&self, data: &Value, path: &str) -> Result<Vec<Value>> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current_values = vec![data];

        for part in parts {
            let mut next_values = Vec::new();
            let is_array = part.ends_with("[]");
            let key = if is_array {
                &part[..part.len() - 2]
            } else {
                part
            };

            for value in &current_values {
                if key.is_empty() && is_array {
                    if let Some(arr) = value.as_array() {
                        next_values.extend(arr.iter());
                    }
                } else if let Some(nested_value) = value.get(key) {
                    if is_array {
                        if let Some(arr) = nested_value.as_array() {
                            next_values.extend(arr.iter());
                        }
                    } else {
                        next_values.push(nested_value);
                    }
                }
            }
            current_values = next_values;
        }

        Ok(current_values.into_iter().cloned().collect())
    }

    /// Extract using common API response patterns
    pub fn extract_by_common_patterns(&self, data: &Value) -> Result<Vec<Value>> {
        // Common patterns for API responses
        let patterns = [
            "data.products",
            "data.items",
            "data.results",
            "data.data",
            "products",
            "items",
            "results",
            "data",
        ];

        for pattern in &patterns {
            if let Ok(products) = self.extract_by_path(data, pattern) {
                if !products.is_empty() {
                    info!(
                        "Found {} products using pattern '{}'",
                        products.len(),
                        pattern
                    );
                    return Ok(products);
                }
            }
        }

        // If no pattern works, try to extract from root if it's an array
        if let Value::Array(arr) = data {
            info!("Using root array with {} items", arr.len());
            return Ok(arr.clone());
        }

        warn!("No products found using any extraction pattern");
        Ok(vec![])
    }

    /// Build GraphQL request body for category
    pub fn build_graphql_request_body(&self, category_id: &str) -> Result<Value> {
        let query = self
            .config
            .request
            .graphql_query
            .as_ref()
            .ok_or_else(|| anyhow!("GraphQL query not configured"))?;

        let mut variables = self
            .config
            .request
            .graphql_variables
            .clone()
            .unwrap_or_default();

        // Replace category placeholder in variables
        if let Some(category_field) = &self.config.request.category_field {
            variables.insert(
                category_field.clone(),
                Value::String(category_id.to_string()),
            );
        }

        Ok(serde_json::json!({
            "query": query,
            "variables": variables
        }))
    }

    /// Check if response indicates more pages available
    pub fn has_more_pages(&self, data: &Value, current_page: i32) -> bool {
        if let Some(ref total_pages_path) = self.config.response.total_pages_path {
            if let Ok(total_pages_values) = self.extract_by_path(data, total_pages_path) {
                if let Some(total_pages) = total_pages_values.get(0).and_then(|v| v.as_i64()) {
                    return current_page < total_pages as i32;
                }
            }
        }
        // Check for pagination indicators in response
        if let Some(pagination) = data.get("pagination") {
            if let Some(has_more) = pagination.get("hasMore") {
                return has_more.as_bool().unwrap_or(false);
            }

            if let Some(total_pages) = pagination.get("totalPages") {
                if let Some(total) = total_pages.as_i64() {
                    return current_page < total as i32;
                }
            }
        }

        // Check if current response has data
        let products = self.extract_products(data).unwrap_or_default();

        // If we got fewer products than page size, probably no more pages
        if let Some(page_size) = self.config.request.page_size {
            return products.len() >= page_size as usize;
        }

        // Default: assume no more pages if we got empty response
        !products.is_empty()
    }

    /// Get the configured page size or default
    pub fn get_page_size(&self) -> i32 {
        self.config.request.page_size.unwrap_or(20)
    }

    /// Get the maximum pages to fetch (safety limit)
    pub fn get_max_pages(&self) -> i32 {
        // Use default limit as max pages if available, otherwise default to 100
        self.config
            .pagination
            .default_limit
            .map(|l| l as i32)
            .unwrap_or(100)
    }
}
