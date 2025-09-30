use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Instant;

use crate::processor::stages::source_transformer::{
    SourceTransformer, SourceType, RawSourceData, TransformationResult, TransformerConfig
};

/// Transformer for Pandamart GraphQL API data
/// 
/// Converts Pandamart-specific JSON structure into standardized format
/// that can be processed by the standard pipeline stages.
pub struct PandamartTransformer {
    config: TransformerConfig,
}

impl PandamartTransformer {
    /// Create a new Pandamart transformer
    pub fn new() -> Self {
        Self {
            config: TransformerConfig::default(),
        }
    }
    
    /// Create a new Pandamart transformer with custom configuration
    pub fn with_config(config: TransformerConfig) -> Self {
        Self { config }
    }
    
    /// Transform a single Pandamart product to standardized JSON
    fn transform_product(&self, item: &Value) -> Result<Value> {
        // Extract fields using Pandamart's specific structure
        let name = self.extract_string(item, &["name", "title", "productName"])?;
        let product_id = self.extract_string(item, &["productID", "id", "itemId"])?;
        
        // Extract prices - Pandamart uses originalPrice and price
        let original_price = self.extract_price(item, &["originalPrice", "mrp", "listPrice"])?;
        let current_price = self.extract_price(item, &["price", "salePrice", "currentPrice"])?;
        
        // Calculate discount
        let discount = if original_price > 0.0 && current_price < original_price {
            ((original_price - current_price) / original_price * 100.0).round()
        } else {
            0.0
        };
        
        // Extract category
        let category = self.extract_string(item, &["categoryProducts.name", "category", "categorySection"])
            .unwrap_or_else(|_| "Unknown".to_string());
        
        // Extract SKU from attributes if available
        let sku = self.extract_sku_from_attributes(item)
            .unwrap_or_else(|| product_id.clone());
        
        // Extract units of mass from attributes
        let units_of_mass = self.extract_units_from_attributes(item)
            .unwrap_or_else(|| "N/A".to_string());
        
        // Create standardized JSON object
        let json_product = json!({
            "name": name.trim(),
            "product_id": product_id.trim(),
            "cost_price": format!("{:.2}", current_price),
            "mrp": format!("{:.2}", original_price),
            "sku": sku.trim(),
            "category_name": category.trim(),
            "units_of_mass": units_of_mass,
            "sku_percent_off": format!("{:.2}", discount),
            "source_type": "pandamart",
            // Additional Pandamart-specific fields
            "availability": item.get("availability").and_then(|v| v.as_bool()).unwrap_or(true),
            "store_id": item.get("store_info")
                .and_then(|s| s.get("store_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        });
        
        Ok(json_product)
    }
    
    /// Extract string value from multiple possible field paths
    fn extract_string(&self, item: &Value, field_paths: &[&str]) -> Result<String> {
        for path in field_paths {
            if let Some(value) = self.get_nested_value(item, path) {
                if let Some(s) = value.as_str() {
                    if !s.trim().is_empty() {
                        return Ok(s.to_string());
                    }
                }
            }
        }
        Err(anyhow!("No valid string found for paths: {:?}", field_paths))
    }
    
    /// Extract price value from multiple possible field paths
    fn extract_price(&self, item: &Value, field_paths: &[&str]) -> Result<f64> {
        for path in field_paths {
            if let Some(value) = self.get_nested_value(item, path) {
                if let Some(price) = value.as_f64() {
                    return Ok(price);
                }
                if let Some(price_str) = value.as_str() {
                    if let Ok(price) = price_str.parse::<f64>() {
                        return Ok(price);
                    }
                }
            }
        }
        Ok(0.0) // Default to 0.0 if no price found
    }
    
    /// Get nested value using dot notation (e.g., "store_info.store_id")
    fn get_nested_value<'a>(&self, item: &'a Value, path: &str) -> Option<&'a Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = item;
        
        for part in parts {
            current = current.get(part)?;
        }
        
        Some(current)
    }
    
    /// Extract SKU from Pandamart's attributes array
    fn extract_sku_from_attributes(&self, item: &Value) -> Option<String> {
        if let Some(attributes) = item.get("attributes").and_then(|v| v.as_array()) {
            // Pandamart typically stores SKU in attributes[0].value
            if let Some(first_attr) = attributes.get(0) {
                if let Some(value) = first_attr.get("value").and_then(|v| v.as_str()) {
                    return Some(value.to_string());
                }
            }
        }
        None
    }
    
    /// Extract units of mass from Pandamart's attributes array
    fn extract_units_from_attributes(&self, item: &Value) -> Option<String> {
        if let Some(attributes) = item.get("attributes").and_then(|v| v.as_array()) {
            // Pandamart typically stores units in attributes[2].value
            if let Some(units_attr) = attributes.get(2) {
                if let Some(value) = units_attr.get("value").and_then(|v| v.as_str()) {
                    return Some(value.to_string());
                }
            }
        }
        None
    }
    
    /// Validate that a Pandamart product has required fields
    fn validate_product(&self, item: &Value) -> bool {
        // Check for required fields
        let has_name = self.extract_string(item, &["name", "title", "productName"]).is_ok();
        let has_id = self.extract_string(item, &["productID", "id", "itemId"]).is_ok();
        
        has_name && has_id
    }
}

impl SourceTransformer for PandamartTransformer {
    fn source_type(&self) -> SourceType {
        SourceType::Pandamart
    }
    
    fn name(&self) -> &str {
        "pandamart_transformer"
    }
    
    fn transform(&self, raw_data: RawSourceData) -> Result<TransformationResult> {
        let start_time = Instant::now();
        
        let json_data = match raw_data {
            RawSourceData::Json(data) => data,
            _ => return Err(anyhow!("PandamartTransformer can only process JSON data")),
        };
        
        let mut transformed_data = Vec::new();
        let mut items_transformed = 0;
        let mut items_failed = 0;
        let mut warnings = Vec::new();
        
        for (index, item) in json_data.iter().enumerate() {
            // Validate product first
            if !self.validate_product(item) {
                items_failed += 1;
                warnings.push(format!(
                    "Product at index {} failed validation: missing required fields", 
                    index
                ));
                
                if !self.config.skip_invalid_items {
                    return Err(anyhow!("Invalid product at index {}: missing required fields", index));
                }
                continue;
            }
            
            match self.transform_product(item) {
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
        .with_metadata("source_type".to_string(), "pandamart".to_string())
        .with_metadata("original_count".to_string(), json_data.len().to_string())
        .with_source_metric("transformation_rate".to_string(), 
            items_transformed as f64 / (items_transformed + items_failed) as f64))
    }
    
    fn get_field_mappings(&self) -> HashMap<String, String> {
        let mut mappings = HashMap::new();
        
        // Pandamart-specific field mappings
        mappings.insert("productID".to_string(), "product_id".to_string());
        mappings.insert("name".to_string(), "name".to_string());
        mappings.insert("price".to_string(), "cost_price".to_string());
        mappings.insert("originalPrice".to_string(), "mrp".to_string());
        mappings.insert("categoryProducts.name".to_string(), "category_name".to_string());
        mappings.insert("attributes[0].value".to_string(), "sku".to_string());
        mappings.insert("attributes[2].value".to_string(), "units_of_mass".to_string());
        
        // Apply custom field mappings from config
        for (key, value) in &self.config.custom_field_mappings {
            mappings.insert(key.clone(), value.clone());
        }
        
        mappings
    }
    
    fn can_transform(&self, raw_data: &RawSourceData) -> bool {
        matches!(raw_data, RawSourceData::Json(_))
    }
    
    fn get_config(&self) -> TransformerConfig {
        self.config.clone()
    }
}

impl Default for PandamartTransformer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_pandamart_product() -> Value {
        json!({
            "productID": "12345",
            "name": "Fresh Bananas",
            "description": "Premium quality bananas",
            "price": 150.0,
            "originalPrice": 200.0,
            "categoryProducts": {
                "name": "Fresh Fruits"
            },
            "availability": true,
            "attributes": [
                {"value": "BAN123"},
                {"name": "color", "value": "yellow"},
                {"name": "weight", "value": "1kg"}
            ],
            "store_info": {
                "store_id": "pandamart_001",
                "location": "Downtown"
            }
        })
    }

    #[test]
    fn test_pandamart_transformer_basic() {
        let transformer = PandamartTransformer::new();
        let products = vec![create_test_pandamart_product()];
        let raw_data = RawSourceData::Json(products);
        
        let result = transformer.transform(raw_data).unwrap();
        
        assert_eq!(result.metrics.items_transformed, 1);
        assert_eq!(result.metrics.items_failed, 0);
        assert_eq!(result.data.len(), 1);
        
        let product = &result.data[0];
        assert_eq!(product["name"], "Fresh Bananas");
        assert_eq!(product["product_id"], "12345");
        assert_eq!(product["cost_price"], "150.00");
        assert_eq!(product["mrp"], "200.00");
        assert_eq!(product["sku"], "BAN123");
        assert_eq!(product["category_name"], "Fresh Fruits");
        assert_eq!(product["units_of_mass"], "1kg");
        assert_eq!(product["sku_percent_off"], "25.00");
    }

    #[test]
    fn test_field_extraction() {
        let transformer = PandamartTransformer::new();
        let product = create_test_pandamart_product();
        
        assert_eq!(
            transformer.extract_string(&product, &["name"]).unwrap(),
            "Fresh Bananas"
        );
        assert_eq!(
            transformer.extract_price(&product, &["price"]).unwrap(),
            150.0
        );
        assert_eq!(
            transformer.extract_sku_from_attributes(&product).unwrap(),
            "BAN123"
        );
        assert_eq!(
            transformer.extract_units_from_attributes(&product).unwrap(),
            "1kg"
        );
    }

    #[test]
    fn test_nested_value_extraction() {
        let transformer = PandamartTransformer::new();
        let product = create_test_pandamart_product();
        
        let category = transformer.get_nested_value(&product, "categoryProducts.name");
        assert_eq!(category.unwrap().as_str().unwrap(), "Fresh Fruits");
        
        let store_id = transformer.get_nested_value(&product, "store_info.store_id");
        assert_eq!(store_id.unwrap().as_str().unwrap(), "pandamart_001");
    }
}
