use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;


use super::base::FieldExtractor;

/// Standard field extractor that handles common e-commerce product fields
/// Supports multiple field naming conventions and fallback logic
pub struct StandardFieldExtractor;

impl StandardFieldExtractor {
    pub fn new() -> Self {
        Self
    }
    
    /// Helper function to safely extract string values
    fn get_string(&self, item: &Value, key: &str) -> String {
        item.get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    /// Helper function to safely extract number values
    fn get_number(&self, item: &Value, key: &str) -> Option<String> {
        item.get(key).and_then(|v| match v {
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    // Format as integer if it's a whole number
                    if f.fract() == 0.0 {
                        Some((f as i64).to_string())
                    } else {
                        Some(f.to_string())
                    }
                } else {
                    Some(n.to_string())
                }
            }
            Value::String(s) => {
                s.parse::<f64>().ok().map(|f| {
                    // Format as integer if it's a whole number
                    if f.fract() == 0.0 {
                        (f as i64).to_string()
                    } else {
                        f.to_string()
                    }
                })
            }
            _ => None,
        })
    }
    
    /// Extract product identifier using multiple field name conventions
    fn extract_identifier(&self, item: &Value) -> Option<String> {
        if let Some(product_id) = item.get("product_id").and_then(|v| v.as_u64()) {
            Some(product_id.to_string())
        } else if let Some(product_id) = item.get("productID").and_then(|v| v.as_str()) {
            // Pandamart: productID field
            Some(product_id.to_string())
        } else {
            let sku = self.get_string(item, "sku");
            if !sku.is_empty() {
                Some(sku)
            } else {
                let id = self.get_string(item, "id");
                if !id.is_empty() {
                    Some(id)
                } else {
                    let variant = self.get_string(item, "variantTitleSlug");
                    if !variant.is_empty() {
                        Some(variant)
                    } else {
                        None
                    }
                }
            }
        }
    }
    
    /// Extract product name using multiple field name conventions
    fn extract_name(&self, item: &Value) -> String {
        let name = self.get_string(item, "name");
        if !name.is_empty() {
            name
        } else {
            let title = self.get_string(item, "title");
            if !title.is_empty() {
                title
            } else {
                self.get_string(item, "productName")
            }
        }
    }
    
    /// Extract SKU using multiple field name conventions
    fn extract_sku(&self, item: &Value) -> String {
        let direct_sku = self.get_string(item, "sku");
        if !direct_sku.is_empty() {
            direct_sku
        } else {
            // Pandamart: Extract from attributes array where key="sku"
            item.get("attributes")
                .and_then(|attrs| attrs.as_array())
                .and_then(|arr| {
                    arr.iter().find(|attr| {
                        attr.get("key")
                            .and_then(|k| k.as_str())
                            .map(|k| k == "sku")
                            .unwrap_or(false)
                    })
                })
                .and_then(|attr| attr.get("value"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    // Fallback to item_id or identifier
                    let item_id = self.get_string(item, "item_id");
                    if !item_id.is_empty() {
                        item_id
                    } else {
                        String::new()
                    }
                })
        }
    }

    /// Extract cost price using multiple field name conventions and fallbacks
    fn extract_cost_price(&self, item: &Value) -> Option<String> {
        self.get_number(item, "cost_price")
            .or_else(|| self.get_number(item, "special_price"))
            .or_else(|| self.get_number(item, "discountedPrice"))
            .or_else(|| self.get_number(item, "discounted_price"))
            .or_else(|| self.get_number(item, "price")) // Pandamart: price field
            // Dealcart: Extract from groupRanges[0].discountedPrice
            .or_else(|| {
                item.get("groupRanges")
                    .and_then(|ranges| ranges.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|first_range| first_range.get("discountedPrice"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
    }

    /// Extract MRP using multiple field name conventions and fallbacks
    fn extract_mrp(&self, item: &Value) -> Option<String> {
        self.get_number(item, "mrp")
            .or_else(|| self.get_number(item, "product_price"))
            .or_else(|| self.get_number(item, "actualPrice"))
            .or_else(|| self.get_number(item, "actual_price"))
            .or_else(|| self.get_number(item, "originalPrice")) // Pandamart: originalPrice field
            .or_else(|| self.get_number(item, "original_price")) // Pandamart: original_price field
            // Dealcart: Extract from inventories[0].dcImsMrp
            .or_else(|| {
                item.get("inventories")
                    .and_then(|inventories| inventories.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|first_inventory| first_inventory.get("dcImsMrp"))
                    .and_then(|v| v.as_u64())
                    .map(|n| n.to_string())
            })
    }
}

impl FieldExtractor for StandardFieldExtractor {
    fn extract_fields(&self, item: &Value) -> Result<HashMap<String, String>> {
        let mut record = HashMap::new();

        // Extract identifier - try multiple field names
        if let Some(identifier) = self.extract_identifier(item) {
            record.insert("product_id".to_string(), identifier);
        }

        // Extract name
        let name = self.extract_name(item);
        if !name.is_empty() {
            record.insert("name".to_string(), name);
        }

        // Extract cost price
        if let Some(cost_price) = self.extract_cost_price(item) {
            record.insert("cost_price".to_string(), cost_price);
        }

        // Extract MRP
        if let Some(mrp) = self.extract_mrp(item) {
            record.insert("mrp".to_string(), mrp);
        }

        // Extract SKU
        let sku = self.extract_sku(item);
        if !sku.is_empty() {
            record.insert("sku".to_string(), sku);
        }

        // Extract category name (should be set by preprocessors)
        let category = self.get_string(item, "category_name");
        if !category.is_empty() {
            record.insert("category_name".to_string(), category);
        }

        // Extract discount percentage
        let discount = self.get_string(item, "sku_percent_off");
        if !discount.is_empty() {
            record.insert("sku_percent_off".to_string(), discount);
        }

        Ok(record)
    }
    
    fn name(&self) -> &'static str {
        "Standard"
    }
}

impl Default for StandardFieldExtractor {
    fn default() -> Self {
        Self::new()
    }
}
