use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;

use crate::processor::stages::source_transformer::{
    SourceTransformer, SourceType, RawSourceData, TransformationResult, TransformerConfig
};

/// Transformer for generic JSON API data
/// 
/// This is a pass-through transformer for JSON data that's already in a
/// compatible format. It can optionally apply field mappings and validation.
pub struct JsonTransformer {
    config: TransformerConfig,
}

impl JsonTransformer {
    /// Create a new JSON transformer
    pub fn new() -> Self {
        Self {
            config: TransformerConfig::default(),
        }
    }
    
    /// Create a new JSON transformer with custom configuration
    pub fn with_config(config: TransformerConfig) -> Self {
        Self { config }
    }
    
    /// Transform a single JSON item (mostly pass-through with optional field mapping)
    fn transform_item(&self, item: &Value) -> Result<Value> {
        if !self.config.custom_field_mappings.is_empty() {
            // Apply field mappings if configured
            self.apply_field_mappings(item)
        } else {
            // Pass through as-is
            Ok(item.clone())
        }
    }
    
    /// Apply custom field mappings to transform field names
    fn apply_field_mappings(&self, item: &Value) -> Result<Value> {
        if let Some(obj) = item.as_object() {
            let mut new_obj = serde_json::Map::new();
            
            for (key, value) in obj {
                // Check if this field should be mapped to a different name
                let new_key = self.config.custom_field_mappings
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| key.clone());
                
                new_obj.insert(new_key, value.clone());
            }
            
            Ok(Value::Object(new_obj))
        } else {
            // Not an object, return as-is
            Ok(item.clone())
        }
    }
    
    /// Validate that a JSON item has basic structure
    fn validate_item(&self, item: &Value) -> bool {
        // Basic validation - ensure it's an object
        if !item.is_object() {
            return false;
        }
        
        // Check for any required fields specified in config
        if let Some(required_fields) = self.config.source_config.get("required_fields") {
            if let Some(fields_array) = required_fields.as_array() {
                for field in fields_array {
                    if let Some(field_name) = field.as_str() {
                        if !item.get(field_name).is_some() {
                            return false;
                        }
                    }
                }
            }
        }
        
        true
    }
}

impl SourceTransformer for JsonTransformer {
    fn source_type(&self) -> SourceType {
        SourceType::JsonApi
    }
    
    fn name(&self) -> &str {
        "json_transformer"
    }
    
    fn transform(&self, raw_data: RawSourceData) -> Result<TransformationResult> {
        let start_time = Instant::now();
        
        let json_data = match raw_data {
            RawSourceData::Json(data) => data,
            _ => return Err(anyhow!("JsonTransformer can only process JSON data")),
        };
        
        let mut transformed_data = Vec::new();
        let mut items_transformed = 0;
        let mut items_failed = 0;
        let mut warnings = Vec::new();
        
        for (index, item) in json_data.iter().enumerate() {
            // Validate item first
            if !self.validate_item(item) {
                items_failed += 1;
                warnings.push(format!(
                    "Item at index {} failed validation", 
                    index
                ));
                
                if !self.config.skip_invalid_items {
                    return Err(anyhow!("Invalid item at index {}: failed validation", index));
                }
                continue;
            }
            
            match self.transform_item(item) {
                Ok(transformed_item) => {
                    transformed_data.push(transformed_item);
                    items_transformed += 1;
                }
                Err(e) => {
                    items_failed += 1;
                    warnings.push(format!(
                        "Failed to transform item at index {}: {}", 
                        index, e
                    ));
                    
                    if !self.config.skip_invalid_items {
                        return Err(anyhow!("Failed to transform item at index {}: {}", index, e));
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
        
        // For JSON transformer, having no transformed items is only an error if all items failed
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
        .with_metadata("source_type".to_string(), "json_api".to_string())
        .with_metadata("original_count".to_string(), json_data.len().to_string())
        .with_source_metric("transformation_rate".to_string(), 
            if items_transformed + items_failed > 0 {
                items_transformed as f64 / (items_transformed + items_failed) as f64
            } else {
                1.0
            }))
    }
    
    fn get_field_mappings(&self) -> HashMap<String, String> {
        // Return the custom field mappings from config
        self.config.custom_field_mappings.clone()
    }
    
    fn can_transform(&self, raw_data: &RawSourceData) -> bool {
        matches!(raw_data, RawSourceData::Json(_))
    }
    
    fn get_config(&self) -> TransformerConfig {
        self.config.clone()
    }
}

impl Default for JsonTransformer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_transformer_passthrough() {
        let transformer = JsonTransformer::new();
        let data = vec![
            json!({"name": "Product 1", "price": 100}),
            json!({"name": "Product 2", "price": 200}),
        ];
        let raw_data = RawSourceData::Json(data.clone());
        
        let result = transformer.transform(raw_data).unwrap();
        
        assert_eq!(result.metrics.items_transformed, 2);
        assert_eq!(result.metrics.items_failed, 0);
        assert_eq!(result.data.len(), 2);
        assert_eq!(result.data[0], data[0]);
        assert_eq!(result.data[1], data[1]);
    }

    #[test]
    fn test_json_transformer_with_field_mappings() {
        let mut config = TransformerConfig::default();
        config.custom_field_mappings.insert("price".to_string(), "cost_price".to_string());
        config.custom_field_mappings.insert("name".to_string(), "product_name".to_string());
        
        let transformer = JsonTransformer::with_config(config);
        let data = vec![json!({"name": "Product 1", "price": 100})];
        let raw_data = RawSourceData::Json(data);
        
        let result = transformer.transform(raw_data).unwrap();
        
        assert_eq!(result.metrics.items_transformed, 1);
        assert_eq!(result.data.len(), 1);
        
        let transformed = &result.data[0];
        assert_eq!(transformed["product_name"], "Product 1");
        assert_eq!(transformed["cost_price"], 100);
        assert!(transformed.get("name").is_none());
        assert!(transformed.get("price").is_none());
    }

    #[test]
    fn test_json_transformer_validation() {
        let mut config = TransformerConfig::default();
        config.source_config.insert(
            "required_fields".to_string(),
            json!(["name", "price"])
        );
        
        let transformer = JsonTransformer::with_config(config);
        
        // Valid item
        let valid_item = json!({"name": "Product 1", "price": 100});
        assert!(transformer.validate_item(&valid_item));
        
        // Invalid item (missing required field)
        let invalid_item = json!({"name": "Product 1"});
        assert!(!transformer.validate_item(&invalid_item));
        
        // Invalid item (not an object)
        let invalid_item2 = json!("not an object");
        assert!(!transformer.validate_item(&invalid_item2));
    }

    #[test]
    fn test_json_transformer_max_items() {
        let mut config = TransformerConfig::default();
        config.max_items = 2;
        
        let transformer = JsonTransformer::with_config(config);
        let data = vec![
            json!({"name": "Product 1"}),
            json!({"name": "Product 2"}),
            json!({"name": "Product 3"}),
        ];
        let raw_data = RawSourceData::Json(data);
        
        let result = transformer.transform(raw_data).unwrap();
        
        assert_eq!(result.metrics.items_transformed, 2);
        assert_eq!(result.data.len(), 2);
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("max items limit"));
    }

    #[test]
    fn test_field_mappings() {
        let mut config = TransformerConfig::default();
        config.custom_field_mappings.insert("old_name".to_string(), "new_name".to_string());
        
        let transformer = JsonTransformer::with_config(config);
        let mappings = transformer.get_field_mappings();
        
        assert_eq!(mappings.get("old_name"), Some(&"new_name".to_string()));
    }
}
