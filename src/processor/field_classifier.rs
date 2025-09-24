use anyhow::Result;
use std::collections::HashMap;

pub struct FieldClassifier {
    field_mappings: HashMap<String, String>,
}

impl FieldClassifier {
    pub fn new() -> Self {
        let mut field_mappings = HashMap::new();

        // Initialize with common field name patterns
        field_mappings.insert("cost_price".to_string(), "cost_price".to_string());
        field_mappings.insert("mrp".to_string(), "mrp".to_string());
        field_mappings.insert("name".to_string(), "name".to_string());
        field_mappings.insert("sku".to_string(), "sku".to_string());
        field_mappings.insert("sku_percent_off".to_string(), "discount".to_string());
        field_mappings.insert("category_name".to_string(), "category".to_string());

        // Dealcart-specific field mappings
        field_mappings.insert("id".to_string(), "product_id".to_string());
        field_mappings.insert("dcImsMrp".to_string(), "mrp".to_string());
        field_mappings.insert("discountedPrice".to_string(), "cost_price".to_string());
        field_mappings.insert("productCategory".to_string(), "category".to_string());

        // Pandamart-specific field mappings
        field_mappings.insert("productID".to_string(), "product_id".to_string());
        field_mappings.insert("originalPrice".to_string(), "mrp".to_string());
        field_mappings.insert("price".to_string(), "cost_price".to_string());
        field_mappings.insert("category_section".to_string(), "category".to_string());

        // Add common variations
        field_mappings.insert("price".to_string(), "cost_price".to_string());
        field_mappings.insert("product_price".to_string(), "mrp".to_string());
        field_mappings.insert("special_price".to_string(), "cost_price".to_string());
        field_mappings.insert("selling_price".to_string(), "cost_price".to_string());
        field_mappings.insert("product_name".to_string(), "name".to_string());
        field_mappings.insert("item_name".to_string(), "name".to_string());
        field_mappings.insert("title".to_string(), "name".to_string());
        field_mappings.insert("product_id".to_string(), "product_id".to_string());
        field_mappings.insert("item_id".to_string(), "product_id".to_string());
        field_mappings.insert("id".to_string(), "product_id".to_string());
        field_mappings.insert("discount".to_string(), "discount".to_string());
        field_mappings.insert("discount_percent".to_string(), "discount".to_string());
        field_mappings.insert("percent_off".to_string(), "discount".to_string());
        field_mappings.insert("category".to_string(), "category".to_string());
        field_mappings.insert("product_category".to_string(), "category".to_string());
        field_mappings.insert("item_category".to_string(), "category".to_string());

        FieldClassifier { field_mappings }
    }

    pub fn classify_field(&self, field_name: &str, sample_values: &[String]) -> Result<String> {
        let normalized_field = self.normalize_field_name(field_name);

        // Try rule-based classification first with exact matches
        for (pattern, canonical) in &self.field_mappings {
            let normalized_pattern = self.normalize_field_name(pattern);
            if normalized_field == normalized_pattern {
                return Ok(canonical.clone());
            }
        }

        // Try fuzzy matching with contains
        for (pattern, canonical) in &self.field_mappings {
            let normalized_pattern = self.normalize_field_name(pattern);
            if normalized_field.contains(&normalized_pattern)
                || normalized_pattern.contains(&normalized_field)
            {
                return Ok(canonical.clone());
            }
        }

        // Content-based classification as fallback
        if !sample_values.is_empty() {
            let classification = self.classify_by_content(field_name, sample_values);
            if classification != field_name {
                return Ok(classification);
            }
        }

        // If all else fails, return the original field name
        Ok(field_name.to_string())
    }

    fn normalize_field_name(&self, name: &str) -> String {
        name.to_lowercase()
            .replace("_", "")
            .replace("-", "")
            .replace(" ", "")
    }

    fn classify_by_content(&self, field_name: &str, sample_values: &[String]) -> String {
        let field_lower = field_name.to_lowercase();

        // Check field name patterns first (order matters!)
        if field_lower.contains("sku") && !field_lower.contains("percent") && !field_lower.contains("off") {
            return "sku".to_string();
        }

        if field_lower.contains("price")
            || field_lower.contains("cost")
            || field_lower.contains("mrp")
        {
            return "cost_price".to_string();
        }

        if field_lower.contains("name")
            || field_lower.contains("title")
            || (field_lower.contains("product") && !field_lower.contains("id"))
        {
            return "name".to_string();
        }

        if field_lower.contains("id") || field_lower.contains("product_id") {
            return "product_id".to_string();
        }

        if field_lower.contains("discount")
            || field_lower.contains("off")
            || field_lower.contains("percent")
        {
            return "discount".to_string();
        }

        if field_lower.contains("category")
            || field_lower.contains("type")
            || field_lower.contains("class")
        {
            return "category".to_string();
        }

        // Analyze sample values
        let sample_str = sample_values
            .first()
            .unwrap_or(&String::new())
            .to_lowercase();

        if self.looks_like_price(&sample_str) {
            return "cost_price".to_string();
        }

        if self.looks_like_discount(&sample_str) {
            return "discount".to_string();
        }

        if self.looks_like_name(&sample_str) {
            return "name".to_string();
        }

        if self.looks_like_category(&sample_str) {
            return "category".to_string();
        }

        field_name.to_string()
    }

    fn looks_like_price(&self, value: &str) -> bool {
        // Check for currency symbols or decimal patterns
        value.contains("$")
            || value.contains("₹")
            || value.contains("€")
            || value.contains("£")
            || (value.contains(".") && value.chars().filter(|c| c.is_digit(10)).count() > 0)
            || value.parse::<f64>().is_ok()
    }

    fn looks_like_name(&self, value: &str) -> bool {
        // Names typically have multiple words or significant alphabetic content
        value.len() > 3
            && value.chars().any(|c| c.is_alphabetic())
            && value.split_whitespace().count() >= 2
    }

    fn looks_like_discount(&self, value: &str) -> bool {
        value.contains("%")
            || value.contains("off")
            || value.contains("discount")
            || (value.parse::<f64>().is_ok() && value.parse::<f64>().unwrap() <= 100.0)
    }

    fn looks_like_category(&self, value: &str) -> bool {
        // Categories are usually short, alphabetic strings
        value.len() < 50
            && value
                .chars()
                .all(|c| c.is_alphabetic() || c.is_whitespace())
            && !value.contains(".")
    }

    #[allow(dead_code)]
    pub fn add_field_mapping(&mut self, from: String, to: String) {
        self.field_mappings.insert(from, to);
    }

    pub fn map_to_canonical_schema(&self, df: &mut polars::prelude::DataFrame) -> Result<()> {
        use polars::prelude::*;

        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        for col_name in column_names {
            if let Ok(series) = df.column(&col_name) {
                let sample_values: Vec<String> = match series.dtype() {
                    DataType::String => series
                        .str()
                        .unwrap()
                        .into_no_null_iter()
                        .take(5)
                        .map(|s| s.to_string())
                        .collect(),
                    _ => {
                        // Convert other types to string for analysis
                        (0..std::cmp::min(5, series.len()))
                            .map(|i| format!("{:?}", series.get(i).unwrap()))
                            .collect()
                    }
                };

                if let Ok(canonical_name) = self.classify_field(&col_name, &sample_values) {
                    if canonical_name != col_name {
                        let _ = df.rename(&col_name, canonical_name.into());
                    }
                }
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_canonical_fields(&self) -> Vec<String> {
        let mut canonical_fields: Vec<String> = self.field_mappings.values().cloned().collect();
        canonical_fields.sort();
        canonical_fields.dedup();
        canonical_fields
    }

    #[allow(dead_code)]
    pub fn is_canonical_field(&self, field_name: &str) -> bool {
        self.field_mappings.values().any(|v| v == field_name)
    }
}

impl Default for FieldClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_classification() {
        let classifier = FieldClassifier::new();

        // Test exact matches
        assert_eq!(
            classifier.classify_field("cost_price", &[]).unwrap(),
            "cost_price"
        );

        // Test variations
        assert_eq!(
            classifier.classify_field("product_price", &[]).unwrap(),
            "mrp"
        );

        assert_eq!(
            classifier.classify_field("special_price", &[]).unwrap(),
            "cost_price"
        );

        // Test content-based classification
        assert_eq!(
            classifier
                .classify_field("unknown_field", &["$19.99".to_string()])
                .unwrap(),
            "cost_price"
        );

        assert_eq!(
            classifier
                .classify_field("mystery_column", &["50%".to_string()])
                .unwrap(),
            "discount"
        );
    }

    #[test]
    fn test_normalization() {
        let classifier = FieldClassifier::new();

        assert_eq!(classifier.normalize_field_name("cost_price"), "costprice");
        assert_eq!(classifier.normalize_field_name("Cost-Price"), "costprice");
        assert_eq!(classifier.normalize_field_name("COST PRICE"), "costprice");
    }

    #[test]
    fn test_content_detection() {
        let classifier = FieldClassifier::new();

        assert!(classifier.looks_like_price("$19.99"));
        assert!(classifier.looks_like_price("19.99"));
        assert!(classifier.looks_like_discount("50%"));
        assert!(classifier.looks_like_name("Apple iPhone 13"));
        assert!(classifier.looks_like_category("Electronics"));
    }

    #[test]
    fn test_price_fallback_mappings() {
        let classifier = FieldClassifier::new();

        // Test that product_price maps to mrp (maximum retail price)
        assert_eq!(
            classifier.classify_field("product_price", &["390.00".to_string()]).unwrap(),
            "mrp"
        );

        // Test that special_price maps to cost_price (actual selling price)
        assert_eq!(
            classifier.classify_field("special_price", &["234.00".to_string()]).unwrap(),
            "cost_price"
        );

        // Test normalization works with these fields
        assert_eq!(
            classifier.classify_field("Product_Price", &[]).unwrap(),
            "mrp"
        );

        assert_eq!(
            classifier.classify_field("Special-Price", &[]).unwrap(),
            "cost_price"
        );
    }

    #[test]
    fn test_sku_and_discount_mapping() {
        let classifier = FieldClassifier::new();

        // Test that sku maps to sku (not discount)
        assert_eq!(
            classifier.classify_field("sku", &["TEST123".to_string()]).unwrap(),
            "sku"
        );

        // Test that sku_percent_off maps to discount
        assert_eq!(
            classifier.classify_field("sku_percent_off", &["25% off".to_string()]).unwrap(),
            "discount"
        );

        // Test that product_id maps to product_id
        assert_eq!(
            classifier.classify_field("product_id", &["12345".to_string()]).unwrap(),
            "product_id"
        );

        // Test normalization works
        assert_eq!(
            classifier.classify_field("SKU", &[]).unwrap(),
            "sku"
        );

        assert_eq!(
            classifier.classify_field("Product-ID", &[]).unwrap(),
            "product_id"
        );
    }
}
