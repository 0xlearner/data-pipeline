use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{info, warn};

use crate::fetcher::html_fetcher::ScrapedProduct;

/// HTML-specific processor that converts scraped products to JSON format
/// for unified processing through the existing pipeline
pub struct HtmlProcessor {
    // Future: ML model for enhanced extraction
    // ml_model: Option<ProductMLModel>,
}

impl HtmlProcessor {
    pub fn new() -> Self {
        Self {
            // ml_model: None,
        }
    }

    /// Convert scraped products to JSON format compatible with JsonFlattener
    pub fn process_scraped_products(&self, products: Vec<ScrapedProduct>) -> Result<Vec<Value>> {
        let mut processed_products = Vec::new();
        let mut successful_count = 0;
        let mut failed_count = 0;

        for (index, product) in products.iter().enumerate() {
            match self.convert_to_json(product) {
                Ok(json_product) => {
                    processed_products.push(json_product);
                    successful_count += 1;
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to convert scraped product at index {} to JSON: {}",
                        index, e
                    );
                    warn!("Failed product: {} (ID: {})", product.name, product.product_id);
                }
            }
        }

        info!(
            "HTML processing completed: {} successful, {} failed",
            successful_count, failed_count
        );

        Ok(processed_products)
    }

    /// Convert a single scraped product to JSON format
    fn convert_to_json(&self, product: &ScrapedProduct) -> Result<Value> {
        // Validate required fields
        if product.name.is_empty() {
            return Err(anyhow!("Product name is empty"));
        }
        if product.price.is_empty() {
            return Err(anyhow!("Product price is empty"));
        }
        if product.product_id.is_empty() {
            return Err(anyhow!("Product ID is empty"));
        }

        // Clean and normalize the price
        let cleaned_price = self.clean_price(&product.price)?;

        // Create JSON object compatible with existing JsonFlattener
        let json_product = serde_json::json!({
            "name": product.name.trim(),
            "price": cleaned_price,
            "product_id": product.product_id.trim(),
            "category": product.category.trim(),
            "url": product.url,
            "source_type": "html",
            // Add fields that JsonFlattener expects
            "cost_price": cleaned_price,
            "mrp": cleaned_price, // For HTML sources, we often only have one price
            "sku": product.product_id.trim(),
            "category_name": product.category.trim(),
            "units_of_mass": "N/A", // Will be extracted by rule normalizer if present in name
            "sku_percent_off": "0.00" // Default, can be calculated later if MRP differs
        });

        Ok(json_product)
    }

    /// Clean and normalize price text
    fn clean_price(&self, price_text: &str) -> Result<String> {
        // Remove common price prefixes and suffixes
        let cleaned = price_text
            .replace("Rs.", "")
            .replace("Rs", "")
            .replace("PKR", "")
            .replace("₨", "")
            .replace(",", "")
            .trim()
            .to_string();

        // Extract numeric value
        let numeric_part: String = cleaned
            .chars()
            .filter(|c| c.is_numeric() || *c == '.')
            .collect();

        if numeric_part.is_empty() {
            return Err(anyhow!("No numeric value found in price: {}", price_text));
        }

        // Validate it's a valid number
        match numeric_part.parse::<f64>() {
            Ok(price_value) => {
                if price_value <= 0.0 {
                    return Err(anyhow!("Invalid price value: {}", price_value));
                }
                Ok(price_value.to_string())
            }
            Err(_) => Err(anyhow!("Failed to parse price: {}", numeric_part)),
        }
    }

    /// Enhanced product validation
    pub fn validate_product(&self, product: &ScrapedProduct) -> bool {
        // Basic validation rules
        if product.name.len() < 3 || product.name.len() > 200 {
            return false;
        }

        if product.price.is_empty() {
            return false;
        }

        if product.product_id.is_empty() {
            return false;
        }

        // Check if name contains alphabetic characters
        if !product.name.chars().any(|c| c.is_alphabetic()) {
            return false;
        }

        // Validate price format
        if self.clean_price(&product.price).is_err() {
            return false;
        }

        true
    }

    /// Filter out invalid or unwanted products
    pub fn filter_products(&self, products: Vec<ScrapedProduct>) -> Vec<ScrapedProduct> {
        products
            .into_iter()
            .filter(|product| self.validate_product(product))
            .filter(|product| !self.is_excluded_product(product))
            .collect()
    }

    /// Check if product should be excluded based on content
    fn is_excluded_product(&self, product: &ScrapedProduct) -> bool {
        let name_lower = product.name.to_lowercase();

        // Exclude common non-product items
        let excluded_keywords = [
            "advertisement",
            "sponsored",
            "banner",
            "footer",
            "header",
            "navigation",
            "menu",
            "breadcrumb",
            "pagination",
            "filter",
            "sort",
            "view all",
            "show more",
            "load more",
        ];

        for keyword in &excluded_keywords {
            if name_lower.contains(keyword) {
                return true;
            }
        }

        false
    }

    /// Extract additional metadata from HTML if needed
    pub fn extract_metadata(&self, product: &ScrapedProduct) -> HashMap<String, String> {
        let mut metadata = HashMap::new();

        // Extract units from product name if present
        if let Some(units) = self.extract_units_from_name(&product.name) {
            metadata.insert("units_of_mass".to_string(), units);
        }

        // Extract brand if present
        if let Some(brand) = self.extract_brand_from_name(&product.name) {
            metadata.insert("brand".to_string(), brand);
        }

        // Add source information
        metadata.insert("extraction_method".to_string(), "html_scraping".to_string());
        metadata.insert("source_category".to_string(), product.category.clone());

        if let Some(ref url) = product.url {
            metadata.insert("source_url".to_string(), url.clone());
        }

        metadata
    }

    /// Extract units from product name (kg, g, ml, l, etc.)
    fn extract_units_from_name(&self, name: &str) -> Option<String> {
        let name_lower = name.to_lowercase();

        // Common unit patterns
        let unit_patterns = [
            ("kg", "kg"),
            ("kilogram", "kg"),
            ("gram", "g"),
            ("gm", "g"),
            ("g ", "g"),
            ("ml", "ml"),
            ("milliliter", "ml"),
            ("liter", "l"),
            ("litre", "l"),
            ("l ", "l"),
            ("piece", "piece"),
            ("pcs", "piece"),
            ("pack", "pack"),
            ("dozen", "dozen"),
        ];

        for (pattern, unit) in &unit_patterns {
            if name_lower.contains(pattern) {
                return Some(unit.to_string());
            }
        }

        None
    }

    /// Extract brand from product name (first word or known brands)
    fn extract_brand_from_name(&self, name: &str) -> Option<String> {
        let words: Vec<&str> = name.split_whitespace().collect();
        
        if words.is_empty() {
            return None;
        }

        // Known brands (can be expanded)
        let known_brands = [
            "brightfarms",
            "nestle",
            "unilever",
            "p&g",
            "colgate",
            "johnson",
            "loreal",
        ];

        let name_lower = name.to_lowercase();
        for brand in &known_brands {
            if name_lower.contains(brand) {
                return Some(brand.to_string());
            }
        }

        // Fallback: use first word if it looks like a brand (capitalized)
        let first_word = words[0];
        if first_word.chars().next().unwrap_or('a').is_uppercase() && first_word.len() > 2 {
            return Some(first_word.to_string());
        }

        None
    }
}

impl Default for HtmlProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_cleaning() {
        let processor = HtmlProcessor::new();

        assert_eq!(processor.clean_price("Rs. 150").unwrap(), "150");
        assert_eq!(processor.clean_price("PKR 1,500").unwrap(), "1500");
        assert_eq!(processor.clean_price("₨ 99.50").unwrap(), "99.5");
        assert_eq!(processor.clean_price("2100").unwrap(), "2100");

        assert!(processor.clean_price("invalid").is_err());
        assert!(processor.clean_price("Rs. 0").is_err());
        assert!(processor.clean_price("").is_err());
    }

    #[test]
    fn test_units_extraction() {
        let processor = HtmlProcessor::new();

        assert_eq!(processor.extract_units_from_name("Onion 1 kg"), Some("kg".to_string()));
        assert_eq!(processor.extract_units_from_name("Milk 500 ml"), Some("ml".to_string()));
        assert_eq!(processor.extract_units_from_name("Eggs 1 dozen"), Some("dozen".to_string()));
        assert_eq!(processor.extract_units_from_name("Simple Product"), None);
    }

    #[test]
    fn test_product_validation() {
        let processor = HtmlProcessor::new();

        let valid_product = ScrapedProduct {
            name: "Fresh Bananas".to_string(),
            price: "Rs. 150".to_string(),
            product_id: "12345".to_string(),
            category: "Fruits".to_string(),
            url: None,
            raw_html: "".to_string(),
        };

        assert!(processor.validate_product(&valid_product));

        let invalid_product = ScrapedProduct {
            name: "".to_string(),
            price: "Rs. 150".to_string(),
            product_id: "12345".to_string(),
            category: "Fruits".to_string(),
            url: None,
            raw_html: "".to_string(),
        };

        assert!(!processor.validate_product(&invalid_product));
    }

    #[test]
    fn test_json_conversion() {
        let processor = HtmlProcessor::new();

        let product = ScrapedProduct {
            name: "Fresh Bananas".to_string(),
            price: "Rs. 150".to_string(),
            product_id: "12345".to_string(),
            category: "Fruits".to_string(),
            url: Some("https://example.com/bananas".to_string()),
            raw_html: "".to_string(),
        };

        let json = processor.convert_to_json(&product).unwrap();
        
        assert_eq!(json["name"], "Fresh Bananas");
        assert_eq!(json["price"], "150");
        assert_eq!(json["product_id"], "12345");
        assert_eq!(json["category"], "Fruits");
        assert_eq!(json["source_type"], "html");
    }
}
