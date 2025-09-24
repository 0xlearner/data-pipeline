use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for HTML-based data sources (web scraping)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HtmlConfig {
    pub site: SiteConfig,
    pub scraping: ScrapingConfig,
    pub selectors: SelectorConfig,
    pub categories: HashMap<String, CategoryConfig>,
}

/// Basic site information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteConfig {
    pub name: String,
    pub base_url: String,
    pub user_agent: Option<String>,
}

/// Scraping behavior configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapingConfig {
    pub delay_between_requests_ms: u64,
    pub max_pages_per_category: usize,
    pub max_retries: usize,
    pub timeout_seconds: u64,
    pub respect_robots_txt: bool,
}

/// CSS selectors for extracting data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectorConfig {
    pub product_selectors: Vec<String>,
    pub name_selectors: Vec<String>,
    pub price_selectors: Vec<String>,
    pub category_selectors: Vec<String>,
    pub pagination_selectors: Vec<String>,
}

/// Category-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryConfig {
    pub name: String,
    pub base_url: String,
    pub enabled: bool,
}

impl HtmlConfig {
    /// Load configuration from TOML file
    pub fn from_file(path: &str) -> Result<Self, anyhow::Error> {
        let content = std::fs::read_to_string(path)?;
        let config: HtmlConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Get all enabled categories
    pub fn get_enabled_categories(&self) -> Vec<(&String, &CategoryConfig)> {
        self.categories
            .iter()
            .filter(|(_, config)| config.enabled)
            .collect()
    }

    /// Build URLs for all categories
    pub fn build_category_urls(&self) -> Vec<(String, String)> {
        self.categories
            .iter()
            .filter(|(_, config)| config.enabled)
            .map(|(key, config)| (key.clone(), config.base_url.clone()))
            .collect()
    }
}

impl Default for ScrapingConfig {
    fn default() -> Self {
        Self {
            delay_between_requests_ms: 2000,
            max_pages_per_category: 10,
            max_retries: 3,
            timeout_seconds: 30,
            respect_robots_txt: true,
        }
    }
}

impl Default for SelectorConfig {
    fn default() -> Self {
        Self {
            product_selectors: vec![
                ".product-item".to_string(),
                ".product-card".to_string(),
                "[data-product-id]".to_string(),
                ".grid-item".to_string(),
            ],
            name_selectors: vec![
                ".product-name".to_string(),
                ".item-title".to_string(),
                "h3".to_string(),
                "h4".to_string(),
                ".title".to_string(),
            ],
            price_selectors: vec![
                ".price".to_string(),
                "[data-price-amount]".to_string(),
                ".cost".to_string(),
                ".amount".to_string(),
                "[class*='price']".to_string(),
            ],
            category_selectors: vec![
                ".page-title".to_string(),
                "[data-ui-id='page-title-wrapper']".to_string(),
                ".breadcrumb".to_string(),
                "h1".to_string(),
            ],
            pagination_selectors: vec![
                ".pagination".to_string(),
                ".pager".to_string(),
                ".page-numbers".to_string(),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_configs() {
        let scraping_config = ScrapingConfig::default();
        assert_eq!(scraping_config.delay_between_requests_ms, 2000);
        assert_eq!(scraping_config.max_pages_per_category, 10);

        let selector_config = SelectorConfig::default();
        assert!(!selector_config.product_selectors.is_empty());
        assert!(!selector_config.name_selectors.is_empty());
    }

    #[test]
    fn test_enabled_categories_filter() {
        let mut categories = HashMap::new();
        categories.insert("fruits".to_string(), CategoryConfig {
            name: "Fresh Fruits".to_string(),
            base_url: "https://example.com/fruits".to_string(),
            enabled: true,
        });
        categories.insert("disabled".to_string(), CategoryConfig {
            name: "Disabled Category".to_string(),
            base_url: "https://example.com/disabled".to_string(),
            enabled: false,
        });

        let config = HtmlConfig {
            site: SiteConfig {
                name: "Test Site".to_string(),
                base_url: "https://example.com".to_string(),
                user_agent: None,
            },
            scraping: ScrapingConfig::default(),
            selectors: SelectorConfig::default(),
            categories,
        };

        let enabled = config.get_enabled_categories();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].0, "fruits");
    }
}
