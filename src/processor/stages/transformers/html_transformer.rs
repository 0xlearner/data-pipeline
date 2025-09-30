use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Instant;

use crate::extractor::ScrapedProduct;
use crate::processor::stages::source_transformer::{
    SourceTransformer, SourceType, RawSourceData, TransformationResult, TransformerConfig
};

/// Transformer for HTML scraped data
/// 
/// Converts scraped products from HTML sources into standardized JSON format
/// that can be processed by the standard pipeline stages.
pub struct HtmlTransformer {
    config: TransformerConfig,
}

impl HtmlTransformer {
    /// Create a new HTML transformer
    pub fn new() -> Self {
        Self {
            config: TransformerConfig::default(),
        }
    }
    
    /// Create a new HTML transformer with custom configuration
    pub fn with_config(config: TransformerConfig) -> Self {
        Self { config }
    }
    
    /// Convert a single scraped product to standardized JSON
    fn transform_product(&self, product: &ScrapedProduct) -> Result<Value> {
        // Clean and normalize the price
        let cleaned_price = self.clean_price(&product.price)?;
        
        // Create JSON object compatible with existing JsonFlattener
        let json_product = json!({
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
    
    /// Clean and normalize price strings
    fn clean_price(&self, price_str: &str) -> Result<String> {
        if price_str.is_empty() {
            return Ok("0.00".to_string());
        }
        
        // Remove common currency symbols and formatting
        let cleaned = price_str
            .replace("$", "")
            .replace("₹", "")
            .replace("Rs.", "")
            .replace("Rs", "")
            .replace(",", "")
            .replace(" ", "")
            .trim()
            .to_string();
        
        // Try to parse as float to validate
        match cleaned.parse::<f64>() {
            Ok(price) => {
                if price < 0.0 {
                    Ok("0.00".to_string())
                } else {
                    Ok(format!("{:.2}", price))
                }
            }
            Err(_) => {
                // Try to extract first number found
                let re = regex::Regex::new(r"(\d+(?:\.\d+)?)")?;
                if let Some(captures) = re.captures(&cleaned) {
                    if let Some(number_match) = captures.get(1) {
                        let price: f64 = number_match.as_str().parse()?;
                        return Ok(format!("{:.2}", price));
                    }
                }
                
                // If all else fails, return 0.00
                Ok("0.00".to_string())
            }
        }
    }
    
    /// Validate that a scraped product has required fields
    fn validate_product(&self, product: &ScrapedProduct) -> bool {
        !product.name.trim().is_empty() && 
        !product.product_id.trim().is_empty()
    }
}

impl SourceTransformer for HtmlTransformer {
    fn source_type(&self) -> SourceType {
        SourceType::HtmlScraping
    }
    
    fn name(&self) -> &str {
        "html_transformer"
    }
    
    fn transform(&self, raw_data: RawSourceData) -> Result<TransformationResult> {
        let start_time = Instant::now();
        
        let scraped_products = match raw_data {
            RawSourceData::Html(products) => products,
            _ => return Err(anyhow!("HtmlTransformer can only process HTML data")),
        };
        
        let mut transformed_data = Vec::new();
        let mut items_transformed = 0;
        let mut items_failed = 0;
        let mut warnings = Vec::new();
        
        for (index, product) in scraped_products.iter().enumerate() {
            // Validate product first
            if !self.validate_product(product) {
                items_failed += 1;
                warnings.push(format!(
                    "Product at index {} failed validation: missing name or product_id", 
                    index
                ));
                
                if !self.config.skip_invalid_items {
                    return Err(anyhow!("Invalid product at index {}: missing required fields", index));
                }
                continue;
            }
            
            match self.transform_product(product) {
                Ok(json_product) => {
                    transformed_data.push(json_product);
                    items_transformed += 1;
                }
                Err(e) => {
                    items_failed += 1;
                    warnings.push(format!(
                        "Failed to transform product at index {}: {}", 
                        index, e
                    ));
                    
                    if !self.config.skip_invalid_items {
                        return Err(anyhow!("Failed to transform product at index {}: {}", index, e));
                    }
                }
            }
            
            // Check max items limit
            if self.config.max_items > 0 && items_transformed >= self.config.max_items {
                warnings.push(format!(
                    "Reached max items limit ({}), stopping transformation", 
                    self.config.max_items
                ));
                break;
            }
        }
        
        let transformation_time = start_time.elapsed().as_millis() as u64;
        
        if items_transformed == 0 && items_failed > 0 {
            return Err(anyhow!(
                "All {} items failed transformation", 
                items_failed
            ));
        }
        
        Ok(TransformationResult::partial_success(
            transformed_data,
            items_transformed,
            items_failed,
            transformation_time,
            warnings,
        )
        .with_metadata("source_type".to_string(), "html_scraping".to_string())
        .with_metadata("original_count".to_string(), scraped_products.len().to_string())
        .with_source_metric("transformation_rate".to_string(), 
            items_transformed as f64 / (items_transformed + items_failed) as f64))
    }
    
    fn get_field_mappings(&self) -> HashMap<String, String> {
        let mut mappings = HashMap::new();
        
        // HTML scraping typically produces these fields
        mappings.insert("name".to_string(), "name".to_string());
        mappings.insert("price".to_string(), "cost_price".to_string());
        mappings.insert("product_id".to_string(), "product_id".to_string());
        mappings.insert("category".to_string(), "category_name".to_string());
        mappings.insert("url".to_string(), "url".to_string());
        mappings.insert("sku".to_string(), "sku".to_string());
        mappings.insert("cost_price".to_string(), "cost_price".to_string());
        mappings.insert("mrp".to_string(), "mrp".to_string());
        mappings.insert("sku_percent_off".to_string(), "discount".to_string());
        mappings.insert("units_of_mass".to_string(), "units_of_mass".to_string());
        
        // Apply custom field mappings from config
        for (key, value) in &self.config.custom_field_mappings {
            mappings.insert(key.clone(), value.clone());
        }
        
        mappings
    }
    
    fn can_transform(&self, raw_data: &RawSourceData) -> bool {
        matches!(raw_data, RawSourceData::Html(_))
    }
    
    fn get_config(&self) -> TransformerConfig {
        self.config.clone()
    }
}

impl Default for HtmlTransformer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_product() -> ScrapedProduct {
        ScrapedProduct {
            name: "Test Product".to_string(),
            price: "$19.99".to_string(),
            product_id: "TEST123".to_string(),
            category: "Electronics".to_string(),
            url: Some("https://example.com/product/123".to_string()),
            raw_html: "<div>Test Product</div>".to_string(),
            category_path: None,
            page_name: None,
            page_number: None,
        }
    }

    #[test]
    fn test_html_transformer_basic() {
        let transformer = HtmlTransformer::new();
        let products = vec![create_test_product()];
        let raw_data = RawSourceData::Html(products);
        
        let result = transformer.transform(raw_data).unwrap();
        
        assert_eq!(result.metrics.items_transformed, 1);
        assert_eq!(result.metrics.items_failed, 0);
        assert_eq!(result.data.len(), 1);
        
        let product = &result.data[0];
        assert_eq!(product["name"], "Test Product");
        assert_eq!(product["cost_price"], "19.99");
        assert_eq!(product["product_id"], "TEST123");
    }

    #[test]
    fn test_price_cleaning() {
        let transformer = HtmlTransformer::new();
        
        assert_eq!(transformer.clean_price("$19.99").unwrap(), "19.99");
        assert_eq!(transformer.clean_price("₹1,234.50").unwrap(), "1234.50");
        assert_eq!(transformer.clean_price("Rs. 500").unwrap(), "500.00");
        assert_eq!(transformer.clean_price("invalid").unwrap(), "0.00");
        assert_eq!(transformer.clean_price("").unwrap(), "0.00");
    }

    #[test]
    fn test_validation() {
        let transformer = HtmlTransformer::new();
        
        let valid_product = create_test_product();
        assert!(transformer.validate_product(&valid_product));
        
        let invalid_product = ScrapedProduct {
            name: "".to_string(),
            price: "$19.99".to_string(),
            product_id: "TEST123".to_string(),
            category: "Electronics".to_string(),
            url: Some("https://example.com/product/123".to_string()),
            raw_html: "<div>Invalid Product</div>".to_string(),
            category_path: None,
            page_name: None,
            page_number: None,
        };
        assert!(!transformer.validate_product(&invalid_product));
    }

    #[test]
    fn test_field_mappings() {
        let transformer = HtmlTransformer::new();
        let mappings = transformer.get_field_mappings();
        
        assert_eq!(mappings.get("price"), Some(&"cost_price".to_string()));
        assert_eq!(mappings.get("category"), Some(&"category_name".to_string()));
        assert_eq!(mappings.get("sku_percent_off"), Some(&"discount".to_string()));
    }
}
