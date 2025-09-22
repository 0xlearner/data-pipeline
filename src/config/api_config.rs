use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub api: ApiSection,
    pub request: RequestConfig,
    pub response: ResponseConfig,
    pub pagination: PaginationConfig,
    pub fields: FieldConfig,
    pub categories: HashMap<String, CategoryConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSection {
    pub name: String,
    pub base_url: String,
    pub auth_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationConfig {
    pub r#type: String,
    pub page_param: String,
    pub limit_param: Option<String>,
    pub default_limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    pub target_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryConfig {
    pub name: String,
    pub category_ids: Option<String>,
    pub core_category_slug: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestConfig {
    pub method: String, // "GET" or "POST"
    pub endpoint: Option<String>, // For POST requests
    pub authorization: Option<String>, // Bearer token, etc.
    pub headers: HashMap<String, String>, // Additional headers
    pub product_channel: Option<String>, // For POST requests
    pub category_field: Option<String>, // Field name for category in POST body
    pub page_size: Option<i32>, // Items per page
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseConfig {
    pub data_path: Option<String>, // Path to extract products, e.g., "data[].l2_products[]"
}

impl ApiConfig {
    pub fn from_file(path: &str) -> Result<Self, anyhow::Error> {
        let content = std::fs::read_to_string(path)?;
        let config: ApiConfig = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn build_category_urls(&self) -> Vec<(String, String)> {
        let mut urls = Vec::new();

        for (key, category) in &self.categories {
            if let Some(ref category_ids) = category.category_ids {
                let url = format!(
                    "{}/api/v2/es/categories/{}/products/1242164",
                    self.api.base_url, category_ids
                );
                urls.push((key.clone(), url));
            }
        }

        urls
    }

    pub fn get_category_slugs(&self) -> Vec<(String, String)> {
        self.categories
            .iter()
            .filter_map(|(key, category)| {
                category.core_category_slug.as_ref().map(|slug| (key.clone(), slug.clone()))
            })
            .collect()
    }

    pub fn build_request_url(&self) -> String {
        if let Some(ref endpoint) = self.request.endpoint {
            format!("{}{}", self.api.base_url, endpoint)
        } else {
            self.api.base_url.clone()
        }
    }
}
