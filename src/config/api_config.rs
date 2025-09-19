use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub api: ApiSection,
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
    pub category_ids: String,
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
            let url = format!(
                "{}/api/v2/es/categories/{}/products/1242164",
                self.api.base_url, category.category_ids
            );
            urls.push((key.clone(), url));
        }

        urls
    }
}
