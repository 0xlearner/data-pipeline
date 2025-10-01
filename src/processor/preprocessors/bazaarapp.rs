use anyhow::Result;
use serde_json::Value;

use super::base::Preprocessor;

/// BazaarApp-specific data preprocessor
/// Handles field mapping and cleanup for BazaarApp products
pub struct BazaarAppPreprocessor;

impl BazaarAppPreprocessor {
    pub fn new() -> Self {
        Self
    }
}

impl Preprocessor for BazaarAppPreprocessor {
    fn can_process(&self, item: &Value) -> bool {
        // BazaarApp products have specific fields like variantTitleSlug, actualPrice, discountedPrice
        item.get("variantTitleSlug").is_some() 
            && item.get("actualPrice").is_some() 
            && item.get("discountedPrice").is_some()
    }

    fn process(&self, item: &Value) -> Result<Value> {
        let mut processed = item.clone();
        
        if let Value::Object(ref mut map) = processed {
            // Map BazaarApp fields to standard fields
            
            // Product name: use title field
            if let Some(title) = item.get("title").and_then(|v| v.as_str()) {
                map.insert("name".to_string(), Value::String(title.to_string()));
            }
            
            // Product ID: use id field
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                map.insert("product_id".to_string(), Value::String(id.to_string()));
            }
            
            // Cost price: use discountedPrice
            if let Some(discounted_price) = item.get("discountedPrice").and_then(|v| v.as_f64()) {
                map.insert("cost_price".to_string(), Value::Number(serde_json::Number::from_f64(discounted_price).unwrap()));
            }
            
            // MRP: use actualPrice
            if let Some(actual_price) = item.get("actualPrice").and_then(|v| v.as_f64()) {
                map.insert("mrp".to_string(), Value::Number(serde_json::Number::from_f64(actual_price).unwrap()));
            }
            
            // Category: use category field directly
            if let Some(category) = item.get("category").and_then(|v| v.as_str()) {
                map.insert("category_name".to_string(), Value::String(category.to_string()));
            }
            
            // SKU: already present in sku field, keep as is
            
            // Calculate discount percentage
            if let (Some(actual_price), Some(discounted_price)) = (
                item.get("actualPrice").and_then(|v| v.as_f64()),
                item.get("discountedPrice").and_then(|v| v.as_f64())
            ) {
                if actual_price > 0.0 {
                    let discount_percent = ((actual_price - discounted_price) / actual_price) * 100.0;
                    map.insert("sku_percent_off".to_string(), Value::String(format!("{:.2}", discount_percent)));
                } else {
                    map.insert("sku_percent_off".to_string(), Value::String("0.00".to_string()));
                }
            }
        }
        
        Ok(processed)
    }

    fn name(&self) -> &'static str {
        "BazaarApp"
    }
}

impl Default for BazaarAppPreprocessor {
    fn default() -> Self {
        Self::new()
    }
}
