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
    pub page_param: Option<String>,
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
    pub category_id: Option<String>,
    pub core_category_slug: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestConfig {
    pub method: String,                   // "GET" or "POST"
    pub endpoint: Option<String>,         // For POST requests
    pub authorization: Option<String>,    // Bearer token, etc.
    pub headers: HashMap<String, String>, // Additional headers
    pub product_channel: Option<String>,  // For POST requests
    pub category_field: Option<String>,   // Field name for category in POST body
    pub page_size: Option<i32>,           // Items per page
    pub graphql_query: Option<String>,    // GraphQL query for GraphQL APIs
    pub graphql_variables: Option<HashMap<String, serde_json::Value>>, // GraphQL variables
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseConfig {
    pub data_path: Option<String>, // Path to extract products, e.g., "data[].l2_products[]"
    pub total_pages_path: Option<String>,
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
                // KraveMart pattern: multiple category IDs
                let url = format!(
                    "{}/api/v2/es/categories/{}/products/1242164",
                    self.api.base_url, category_ids
                );
                urls.push((key.clone(), url));
            } else if let Some(ref category_id) = category.category_id {
                // Dealcart pattern: single category ID with endpoint and query params
                if let Some(ref endpoint) = self.request.endpoint {
                    let url = format!(
                        "{}{}?warehouse_id=1&limit={}&category_id={}",
                        self.api.base_url,
                        endpoint,
                        self.pagination.default_limit.unwrap_or(2000),
                        category_id
                    );
                    urls.push((key.clone(), url));
                }
            }
        }

        urls
    }

    pub fn get_category_slugs(&self) -> Vec<(String, String)> {
        self.categories
            .iter()
            .filter_map(|(key, category)| {
                category
                    .core_category_slug
                    .as_ref()
                    .map(|slug| (key.clone(), slug.clone()))
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

    pub fn build_pagination_request_body(
        &self,
        category_slug: &str,
        page: i32,
    ) -> Result<serde_json::Value, anyhow::Error> {
        if self.api.name == "bazaarapp" {
            let page_size = self.request.page_size.unwrap_or(20);
            let product_channel = self.request.product_channel.as_deref().unwrap_or("WEB_APP");
            return Ok(serde_json::json!({
                "productChannel": product_channel,
                "paginationRequestDTO": {
                    "page": page,
                    "size": page_size
                },
                "searchKey": "",
                "brandIds": [],
                "coreCategorySlug": category_slug,
                "subcategorySlugs": []
            }));
        }

        let mut body = serde_json::json!({});

        // Add category field if configured
        if let Some(ref category_field) = self.request.category_field {
            body[category_field] = serde_json::Value::String(category_slug.to_string());
        }

        // Add product channel if configured
        if let Some(ref product_channel) = self.request.product_channel {
            body["productChannel"] = serde_json::Value::String(product_channel.clone());
        }

        // Add pagination fields
        if let Some(ref page_param) = self.pagination.page_param {
            body[page_param] = serde_json::Value::Number(page.into());
        }

        if let Some(page_size) = self.request.page_size {
            if let Some(ref limit_param) = self.pagination.limit_param {
                body[limit_param] = serde_json::Value::Number(page_size.into());
            }
        }

        Ok(body)
    }
}
