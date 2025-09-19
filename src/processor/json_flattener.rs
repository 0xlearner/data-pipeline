use anyhow::{Result, anyhow};
use polars::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{info, warn};

pub struct JsonFlattener;

impl JsonFlattener {
    pub fn new() -> Self {
        JsonFlattener
    }

    pub fn flatten_to_dataframe(&self, json_data: &[Value]) -> Result<DataFrame> {
        let mut records = Vec::new();
        let mut successful_count = 0;
        let mut failed_count = 0;

        for (index, item) in json_data.iter().enumerate() {
            match self.extract_fields_directly(item) {
                Ok(record) => {
                    records.push(record);
                    successful_count += 1;
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to extract fields from product at index {}: {}",
                        index, e
                    );

                    // Log some details about the failed item
                    if let Some(product_name) = item.get("name").and_then(|v| v.as_str()) {
                        warn!("Failed product name: {}", product_name);
                    }
                    if let Some(product_id) = item.get("product_id") {
                        warn!("Failed product ID: {}", product_id);
                    }
                }
            }
        }

        info!(
            "Field extraction summary: {} successful, {} failed out of {} total",
            successful_count,
            failed_count,
            json_data.len()
        );

        self.records_to_dataframe(records)
    }

    pub fn extract_fields_directly(&self, item: &Value) -> Result<HashMap<String, String>> {
        let mut record = HashMap::new();

        // Helper function to safely extract string values
        let get_string = |key: &str| -> String {
            item.get(key)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };

        // Helper function to safely extract number values
        let get_number = |key: &str| -> Option<String> {
            item.get(key).and_then(|v| match v {
                Value::Number(n) => Some(n.to_string()),
                Value::String(s) => s.parse::<f64>().ok().map(|f| f.to_string()),
                _ => None,
            })
        };

        // Extract required fields with fallbacks
        let product_id = item
            .get("product_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("Missing or invalid product_id"))?;

        let name = get_string("name");
        if name.is_empty() {
            return Err(anyhow!("Missing or empty name field"));
        }

        // Extract cost_price with fallback to special_price
        let cost_price = get_number("cost_price")
            .or_else(|| get_number("special_price"));
        if let Some(cost_price) = cost_price {
            record.insert("cost_price".to_string(), cost_price);
        }

        // Extract mrp with fallback to product_price
        let mrp = get_number("mrp")
            .or_else(|| get_number("product_price"));
        if let Some(mrp) = mrp {
            record.insert("mrp".to_string(), mrp);
        }

        // Extract name
        record.insert("name".to_string(), name);

        // Extract sku with fallback to product_id
        let sku = item
            .get("sku")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("PRODUCT_{}", product_id));
        record.insert("sku".to_string(), sku);

        // Extract product_id
        record.insert("product_id".to_string(), product_id.to_string());

        // Extract sku_percent_off
        let sku_percent_off = get_string("sku_percent_off");
        record.insert("sku_percent_off".to_string(), sku_percent_off);

        // Extract category names
        let category_names =
            if let Some(categories) = item.get("categories").and_then(|v| v.as_array()) {
                let names: Vec<String> = categories
                    .iter()
                    .filter_map(|cat| {
                        cat.get("category_name")
                            .and_then(|name| name.as_str())
                            .map(|s| s.trim().to_lowercase())
                    })
                    .collect();
                names.join(", ")
            } else {
                String::new()
            };
        record.insert("category_name".to_string(), category_names);

        Ok(record)
    }

    fn records_to_dataframe(&self, records: Vec<HashMap<String, String>>) -> Result<DataFrame> {
        if records.is_empty() {
            return Ok(DataFrame::empty());
        }

        let mut series_vec = Vec::new();
        let fields = [
            "cost_price",
            "mrp",
            "name",
            "sku",
            "product_id",
            "sku_percent_off",
            "category_name",
        ];

        for field in fields.iter() {
            let values: Vec<String> = records
                .iter()
                .map(|record| record.get(*field).cloned().unwrap_or_default())
                .collect();

            let series = Series::new((*field).into(), values);
            series_vec.push(series.into());
        }

        DataFrame::new(series_vec).map_err(|e| anyhow!("Failed to create DataFrame: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_price_fallback_logic() {
        let flattener = JsonFlattener::new();

        // Test case 1: Primary fields are present
        let product_with_primary = json!({
            "product_id": 123,
            "name": "Test Product",
            "cost_price": 100.0,
            "mrp": 150.0,
            "special_price": 80.0,
            "product_price": 120.0,
            "sku": "TEST123",
            "sku_percent_off": "20%",
            "categories": []
        });

        let result = flattener.extract_fields_directly(&product_with_primary).unwrap();
        assert_eq!(result.get("cost_price").unwrap(), "100.0");
        assert_eq!(result.get("mrp").unwrap(), "150.0");

        // Test case 2: Primary fields are null, fallback fields are present
        let product_with_fallback = json!({
            "product_id": 124,
            "name": "Fallback Product",
            "cost_price": null,
            "mrp": null,
            "special_price": "234.00",
            "product_price": "390.00",
            "sku": "FALLBACK123",
            "sku_percent_off": "40%",
            "categories": []
        });

        let result = flattener.extract_fields_directly(&product_with_fallback).unwrap();
        assert_eq!(result.get("cost_price").unwrap(), "234");
        assert_eq!(result.get("mrp").unwrap(), "390");

        // Test case 3: No price fields present
        let product_no_prices = json!({
            "product_id": 125,
            "name": "No Price Product",
            "sku": "NOPRICE123",
            "sku_percent_off": "0%",
            "categories": []
        });

        let result = flattener.extract_fields_directly(&product_no_prices).unwrap();
        assert!(!result.contains_key("cost_price"));
        assert!(!result.contains_key("mrp"));
    }

    #[test]
    fn test_real_world_sample_data() {
        let flattener = JsonFlattener::new();

        // Test with the exact sample data from the user's issue
        let sample_product = json!({
            "store_id": 1242164,
            "sku": "BNDL7002230",
            "default_image": "https://k2-products.s3.ap-southeast-1.amazonaws.com/product-images/default-images/qtJCA86vtEgodXiyXcs7O3JxcvRNUecgKUhaJqvq.jpg",
            "is_enabled": 1,
            "meta_keywords": "",
            "images": [],
            "categories": [
                {
                    "store_id": 0,
                    "category_name": "Fruits & Vegetables",
                    "category_id": 4960,
                    "product_id": 103922,
                    "parent_category": {
                        "parent_name": "inDrive",
                        "parent_id": 4959,
                        "id": 4960
                    },
                    "cat_search_elastic": ""
                }
            ],
            "inventories": {
                "store_id": 1242164,
                "quantity": 22
            },
            "product_display_order": 4,
            "sku_promotion_text": "",
            "video_youtube_link": "",
            "sticker_image_link": "",
            "search_boost": "",
            "product_price": "390.00",
            "special_price": "234.00",
            "display_in_store": 1,
            "sku_percent_off": "40% off",
            "product_id": 103922,
            "name": "Kfresh Potatoes (Aalu) - 3 Kg",
            "description": "Kfresh Potatoes (Aalu) - 3 Kg",
            "store_type": "express",
            "deals": null,
            "mrp": null,
            "cost_price": null,
            "search_no_space": null
        });

        let result = flattener.extract_fields_directly(&sample_product).unwrap();

        // Verify that fallback logic worked correctly
        assert_eq!(result.get("cost_price").unwrap(), "234"); // special_price -> cost_price
        assert_eq!(result.get("mrp").unwrap(), "390"); // product_price -> mrp
        assert_eq!(result.get("name").unwrap(), "Kfresh Potatoes (Aalu) - 3 Kg");
        assert_eq!(result.get("sku").unwrap(), "BNDL7002230");
        assert_eq!(result.get("sku_percent_off").unwrap(), "40% off");
        assert_eq!(result.get("category_name").unwrap(), "fruits & vegetables");
    }
}
