use anyhow::Result;
use serde_json::Value;

use super::base::Preprocessor;

/// Naheed-specific data preprocessor
/// Handles HTML scraped product data from Naheed store
pub struct NaheedPreprocessor;

impl NaheedPreprocessor {
    pub fn new() -> Self {
        Self
    }

    /// Clean and normalize price text from Naheed
    fn clean_price(&self, price_text: &str) -> Result<String, anyhow::Error> {
        // Remove common price prefixes and suffixes specific to Naheed
        let cleaned = price_text
            .replace("Rs.", "")
            .replace("Rs", "")
            .replace("PKR", "")
            .replace("₨", "")
            .replace(",", "")
            .replace("/-", "")
            .trim()
            .to_string();

        // Extract numeric value
        let numeric_part: String = cleaned
            .chars()
            .filter(|c| c.is_numeric() || *c == '.')
            .collect();

        if numeric_part.is_empty() {
            return Err(anyhow::anyhow!("No numeric value found in price: {}", price_text));
        }

        // Validate it's a valid number
        match numeric_part.parse::<f64>() {
            Ok(price_value) => {
                if price_value <= 0.0 {
                    return Err(anyhow::anyhow!("Invalid price value: {}", price_value));
                }
                Ok(price_value.to_string())
            }
            Err(_) => Err(anyhow::anyhow!("Failed to parse price: {}", numeric_part)),
        }
    }

    /// Extract category name from Naheed product data
    fn extract_category_name(&self, product: &Value) -> String {
        // Try different possible category field names
        if let Some(category) = product.get("category").and_then(|v| v.as_str()) {
            if !category.trim().is_empty() && category.trim() != "N/A" {
                return category.trim().to_string();
            }
        }

        if let Some(category_path) = product.get("category_path").and_then(|v| v.as_str()) {
            if !category_path.trim().is_empty() {
                // Extract the last part of the category path
                let parts: Vec<&str> = category_path.split('/').collect();
                if let Some(last_part) = parts.last() {
                    if !last_part.trim().is_empty() {
                        return last_part.trim().to_string();
                    }
                }
            }
        }

        if let Some(page_name) = product.get("page_name").and_then(|v| v.as_str()) {
            if !page_name.trim().is_empty() {
                return page_name.trim().to_string();
            }
        }

        "General".to_string()
    }

    /// Extract units from product name (kg, g, ml, l, etc.)
    fn extract_units_from_name(&self, name: &str) -> String {
        let name_lower = name.to_lowercase();

        // Common unit patterns for Pakistani grocery stores
        let unit_patterns = [
            ("kg", "kg"),
            ("kilogram", "kg"),
            ("kilo", "kg"),
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
            ("packet", "pack"),
            ("dozen", "dozen"),
            ("bottle", "bottle"),
            ("can", "can"),
            ("box", "box"),
        ];

        for (pattern, unit) in &unit_patterns {
            if name_lower.contains(pattern) {
                return unit.to_string();
            }
        }

        "N/A".to_string()
    }

    /// Generate SKU for Naheed products
    fn generate_sku(&self, product: &Value) -> String {
        // Try to use product_id first
        if let Some(product_id) = product.get("product_id").and_then(|v| v.as_str()) {
            if !product_id.trim().is_empty() {
                return format!("NH-{}", product_id.trim());
            }
        }

        // Fallback: generate from name hash
        if let Some(name) = product.get("name").and_then(|v| v.as_str()) {
            if !name.trim().is_empty() {
                // Simple hash-like generation (in production, use proper hash)
                let hash = name.len() % 10000;
                return format!("NH-{}", hash);
            }
        }

        "NH-UNKNOWN".to_string()
    }

    /// Validate Naheed product data
    fn validate_naheed_product(&self, product: &Value) -> bool {
        // Check required fields
        let name = product.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let price = product.get("price").and_then(|v| v.as_str()).unwrap_or("");

        // Basic validation
        if name.len() < 3 || name.len() > 200 {
            return false;
        }

        if price.is_empty() {
            return false;
        }

        // Check if name contains alphabetic characters
        if !name.chars().any(|c| c.is_alphabetic()) {
            return false;
        }

        // Validate price can be cleaned
        if self.clean_price(price).is_err() {
            return false;
        }

        true
    }
}

impl Preprocessor for NaheedPreprocessor {
    fn can_process(&self, item: &Value) -> bool {
        // Naheed products are identified by source_type = "html" and specific URL patterns
        if let Some(source_type) = item.get("source_type").and_then(|v| v.as_str()) {
            if source_type == "html" {
                // Check if it's from Naheed by looking at URL or other identifiers
                if let Some(url) = item.get("url").and_then(|v| v.as_str()) {
                    return url.contains("naheed.pk") || url.contains("naheed");
                }
                
                // Alternative: check if we have Naheed-specific fields or patterns
                // This could be enhanced based on actual Naheed data structure
                return true; // For now, process all HTML data
            }
        }
        false
    }

    fn process(&self, item: &Value) -> Result<Value> {
        let mut processed = item.clone();

        if let Value::Object(ref mut map) = processed {
            // Validate the product first
            if !self.validate_naheed_product(item) {
                return Err(anyhow::anyhow!("Naheed product failed validation"));
            }

            // Clean and set pricing
            if let Some(price_str) = item.get("price").and_then(|v| v.as_str()) {
                let cleaned_price = self.clean_price(price_str)?;
                map.insert("cost_price".to_string(), Value::String(cleaned_price.clone()));
                map.insert("mrp".to_string(), Value::String(cleaned_price));
            }

            // Set product ID if not present
            if !map.contains_key("product_id") {
                if let Some(existing_id) = item.get("product_id").and_then(|v| v.as_str()) {
                    map.insert("product_id".to_string(), Value::String(existing_id.to_string()));
                } else {
                    // Generate a product ID from name or other fields
                    let generated_id = format!("naheed_{}", map.len());
                    map.insert("product_id".to_string(), Value::String(generated_id));
                }
            }

            // Extract and set category
            let category_name = self.extract_category_name(item);
            map.insert("category_name".to_string(), Value::String(category_name));

            // Generate and set SKU
            let sku = self.generate_sku(item);
            map.insert("sku".to_string(), Value::String(sku));

            // Extract units of mass from name
            if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                let units = self.extract_units_from_name(name);
                map.insert("units_of_mass".to_string(), Value::String(units));
            }

            // Set default discount percentage (HTML sources typically don't have discount info)
            map.insert("sku_percent_off".to_string(), Value::String("0.00".to_string()));

            // Add Naheed-specific metadata
            map.insert("extraction_method".to_string(), Value::String("html_scraping".to_string()));
            map.insert("source_store".to_string(), Value::String("naheed".to_string()));

            // Remove complex fields that might cause flattening issues
            map.remove("raw_html");
        }

        Ok(processed)
    }

    fn name(&self) -> &'static str {
        "Naheed"
    }
}

impl Default for NaheedPreprocessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_can_process_naheed_data() {
        let preprocessor = NaheedPreprocessor::new();

        // Test Naheed HTML data
        let naheed_product = json!({
            "name": "Basmati Rice 5kg",
            "price": "Rs. 1,500",
            "product_id": "NH001",
            "category": "Rice & Grains",
            "url": "https://naheed.pk/products/basmati-rice",
            "source_type": "html"
        });

        assert!(preprocessor.can_process(&naheed_product));

        // Test non-HTML data
        let api_product = json!({
            "name": "Test Product",
            "price": "100",
            "source_type": "api"
        });

        assert!(!preprocessor.can_process(&api_product));
    }

    #[test]
    fn test_price_cleaning() {
        let preprocessor = NaheedPreprocessor::new();

        assert_eq!(preprocessor.clean_price("Rs. 1,500").unwrap(), "1500");
        assert_eq!(preprocessor.clean_price("PKR 2,000/-").unwrap(), "2000");
        assert_eq!(preprocessor.clean_price("₨ 99.50").unwrap(), "99.5");
        assert_eq!(preprocessor.clean_price("1200").unwrap(), "1200");

        assert!(preprocessor.clean_price("invalid").is_err());
        assert!(preprocessor.clean_price("Rs. 0").is_err());
    }

    #[test]
    fn test_units_extraction() {
        let preprocessor = NaheedPreprocessor::new();

        assert_eq!(preprocessor.extract_units_from_name("Basmati Rice 5kg"), "kg");
        assert_eq!(preprocessor.extract_units_from_name("Milk 1 liter"), "l");
        assert_eq!(preprocessor.extract_units_from_name("Eggs 1 dozen"), "dozen");
        assert_eq!(preprocessor.extract_units_from_name("Simple Product"), "N/A");
    }

    #[test]
    fn test_naheed_processing() {
        let preprocessor = NaheedPreprocessor::new();

        let naheed_product = json!({
            "name": "Basmati Rice 5kg",
            "price": "Rs. 1,500",
            "product_id": "NH001",
            "category": "Rice & Grains",
            "url": "https://naheed.pk/products/basmati-rice",
            "source_type": "html",
            "raw_html": "<div>Some HTML</div>"
        });

        let result = preprocessor.process(&naheed_product).unwrap();

        assert_eq!(result["name"], "Basmati Rice 5kg");
        assert_eq!(result["cost_price"], "1500");
        assert_eq!(result["mrp"], "1500");
        assert_eq!(result["category_name"], "Rice & Grains");
        assert_eq!(result["sku"], "NH-NH001");
        assert_eq!(result["units_of_mass"], "kg");
        assert_eq!(result["sku_percent_off"], "0.00");
        assert_eq!(result["source_store"], "naheed");

        // raw_html should be removed
        assert!(result.get("raw_html").is_none());
    }
}
