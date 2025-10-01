use anyhow::Result;
use serde_json::Value;

use super::base::Preprocessor;

/// Pandamart-specific data preprocessor
/// Handles GraphQL response structure with attributes array and category extraction
pub struct PandamartPreprocessor;

impl PandamartPreprocessor {
    pub fn new() -> Self {
        Self
    }
    
    /// Extract SKU from attributes array
    fn extract_sku(&self, item: &Value) -> String {
        if let Some(attributes) = item.get("attributes").and_then(|v| v.as_array()) {
            for attr in attributes {
                if let (Some(key), Some(value)) = (
                    attr.get("key").and_then(|k| k.as_str()),
                    attr.get("value").and_then(|v| v.as_str())
                ) {
                    if key == "sku" {
                        return value.to_string();
                    }
                }
            }
        }
        
        // Fallback: use productID if no SKU found
        item.get("productID")
            .and_then(|v| v.as_str())
            .map(|id| format!("PM-{}", id))
            .unwrap_or_else(|| "Unknown-SKU".to_string())
    }
    
    /// Extract units of mass from attributes array
    fn extract_units_of_mass(&self, item: &Value) -> String {
        if let Some(attributes) = item.get("attributes").and_then(|v| v.as_array()) {
            for attr in attributes {
                if let (Some(key), Some(value)) = (
                    attr.get("key").and_then(|k| k.as_str()),
                    attr.get("value").and_then(|v| v.as_str())
                ) {
                    if key == "baseUnit" || key == "baseContentValue" {
                        return value.to_string();
                    }
                }
            }
        }
        "1".to_string() // Default unit
    }
}

impl Preprocessor for PandamartPreprocessor {
    fn can_process(&self, item: &Value) -> bool {
        // Pandamart products have productID, price, originalPrice, and attributes array
        item.get("productID").is_some()
            && item.get("price").is_some()
            && item.get("originalPrice").is_some()
            && item.get("attributes").and_then(|v| v.as_array()).is_some()
    }

    fn process(&self, item: &Value) -> Result<Value> {
        let mut processed = item.clone();
        
        if let Value::Object(ref mut map) = processed {
            // Product ID: already in productID field, map to product_id
            if let Some(product_id) = item.get("productID").and_then(|v| v.as_str()) {
                map.insert("product_id".to_string(), Value::String(product_id.to_string()));
            }
            
            // Product name: already in name field, keep as is
            
            // Cost price: use price field
            if let Some(price) = item.get("price").and_then(|v| v.as_f64()) {
                map.insert("cost_price".to_string(), Value::Number(serde_json::Number::from_f64(price).unwrap()));
            }
            
            // MRP: use originalPrice field
            if let Some(original_price) = item.get("originalPrice").and_then(|v| v.as_f64()) {
                map.insert("mrp".to_string(), Value::Number(serde_json::Number::from_f64(original_price).unwrap()));
            }
            
            // Extract and set SKU
            let sku = self.extract_sku(item);
            map.insert("sku".to_string(), Value::String(sku));
            
            // Extract units of mass
            let units_of_mass = self.extract_units_of_mass(item);
            map.insert("units_of_mass".to_string(), Value::String(units_of_mass));
            
            // Calculate discount percentage
            if let (Some(original_price), Some(price)) = (
                item.get("originalPrice").and_then(|v| v.as_f64()),
                item.get("price").and_then(|v| v.as_f64())
            ) {
                if original_price > 0.0 {
                    let discount_percent = ((original_price - price) / original_price) * 100.0;
                    map.insert("sku_percent_off".to_string(), Value::String(format!("{:.2}", discount_percent)));
                } else {
                    map.insert("sku_percent_off".to_string(), Value::String("0.00".to_string()));
                }
            }
            
            // For Pandamart, we need to get category from the parent categoryProducts structure
            // Since this preprocessor works on individual items, we'll set a default category
            // The category should be set by the extraction logic that processes categoryProducts
            if !map.contains_key("category_name") {
                map.insert("category_name".to_string(), Value::String("General".to_string()));
            }
            
            // Remove complex arrays to prevent flattening issues
            map.remove("attributes");
            map.remove("activeCampaigns");
            map.remove("productBadges");
            map.remove("weightableAttributes");
            map.remove("urls");
            map.remove("tags");
        }
        
        Ok(processed)
    }

    fn name(&self) -> &'static str {
        "Pandamart"
    }
}

impl Default for PandamartPreprocessor {
    fn default() -> Self {
        Self::new()
    }
}
