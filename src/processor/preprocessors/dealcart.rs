use anyhow::Result;
use serde_json::Value;

use super::base::Preprocessor;

/// DealCart-specific data preprocessor
/// Handles simple product structure
pub struct DealCartPreprocessor;

impl DealCartPreprocessor {
    pub fn new() -> Self {
        Self
    }

    /// Extract category name from DealCart product's productCategory array
    /// Priority: 1) isPrimaryCategory=true, 2) isPreferred=true, 3) first category
    fn extract_category_name(&self, product: &Value) -> String {
        if let Some(categories) = product.get("productCategory").and_then(|c| c.as_array()) {
            // Strategy 1: Look for primary category
            for category_item in categories {
                if let Some(is_primary) = category_item.get("isPrimaryCategory").and_then(|v| v.as_bool()) {
                    if is_primary {
                        if let Some(category_name) = category_item
                            .get("category")
                            .and_then(|cat| cat.get("name"))
                            .and_then(|name| name.as_str())
                        {
                            return category_name.to_string();
                        }
                    }
                }
            }

            // Strategy 2: Look for preferred category
            for category_item in categories {
                if let Some(is_preferred) = category_item.get("isPreferred").and_then(|v| v.as_bool()) {
                    if is_preferred {
                        if let Some(category_name) = category_item
                            .get("category")
                            .and_then(|cat| cat.get("name"))
                            .and_then(|name| name.as_str())
                        {
                            return category_name.to_string();
                        }
                    }
                }
            }

            // Strategy 3: Take the first category
            if let Some(first_category) = categories.first() {
                if let Some(category_name) = first_category
                    .get("category")
                    .and_then(|cat| cat.get("name"))
                    .and_then(|name| name.as_str())
                {
                    return category_name.to_string();
                }
            }
        }

        "Unknown Category".to_string()
    }
}

impl Preprocessor for DealCartPreprocessor {
    fn can_process(&self, item: &Value) -> bool {
        // DealCart products have inventories array with dcImsMrp field
        item.get("inventories")
            .and_then(|inv| inv.as_array())
            .and_then(|arr| arr.first())
            .and_then(|first| first.get("dcImsMrp"))
            .is_some()
            || item.get("groupRanges")
                .and_then(|gr| gr.as_array())
                .and_then(|arr| arr.first())
                .and_then(|first| first.get("discountedPrice"))
                .is_some()
    }

    fn process(&self, item: &Value) -> Result<Value> {
        let mut processed = item.clone();

        if let Value::Object(ref mut map) = processed {
            // Extract MRP from inventories[0].dcImsMrp
            let mrp = item
                .get("inventories")
                .and_then(|inv| inv.as_array())
                .and_then(|arr| arr.first())
                .and_then(|first| first.get("dcImsMrp"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            // Extract cost price from groupRanges[0].discountedPrice
            let cost_price = item
                .get("groupRanges")
                .and_then(|gr| gr.as_array())
                .and_then(|arr| arr.first())
                .and_then(|first| first.get("discountedPrice"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(mrp);

            // Product ID: use id field
            if let Some(id) = item.get("id").and_then(|v| v.as_u64()) {
                map.insert("product_id".to_string(), Value::String(id.to_string()));
            }

            // Product name: already in name field, keep as is

            // Set pricing
            map.insert(
                "cost_price".to_string(),
                Value::Number(serde_json::Number::from_f64(cost_price).unwrap()),
            );
            map.insert(
                "mrp".to_string(),
                Value::Number(serde_json::Number::from_f64(mrp).unwrap()),
            );

            // Extract category from productCategory array - find primary category
            let category_name = self.extract_category_name(item);
            map.insert("category_name".to_string(), Value::String(category_name));

            // Calculate discount percentage
            if mrp > 0.0 {
                let discount_percent = ((mrp - cost_price) / mrp) * 100.0;
                map.insert(
                    "sku_percent_off".to_string(),
                    Value::String(format!("{:.2}", discount_percent)),
                );
            } else {
                map.insert(
                    "sku_percent_off".to_string(),
                    Value::String("0.00".to_string()),
                );
            }

            // Generate SKU from product ID if not present
            if !map.contains_key("sku") {
                if let Some(id) = item.get("id").and_then(|v| v.as_u64()) {
                    map.insert("sku".to_string(), Value::String(format!("DC-{}", id)));
                }
            }

            // Remove complex arrays to prevent flattening issues
            map.remove("inventories");
            map.remove("productCategory");
            map.remove("groupRanges");
            map.remove("images");
            map.remove("productBundle");
        }

        Ok(processed)
    }

    fn name(&self) -> &'static str {
        "DealCart"
    }
}

impl Default for DealCartPreprocessor {
    fn default() -> Self {
        Self::new()
    }
}
