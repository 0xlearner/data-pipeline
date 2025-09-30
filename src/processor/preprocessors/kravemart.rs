use anyhow::Result;
use serde_json::Value;

use super::base::Preprocessor;

/// KraveMart-specific data preprocessor
/// Handles category extraction and cleanup for KraveMart products
pub struct KraveMartPreprocessor;

impl KraveMartPreprocessor {
    pub fn new() -> Self {
        Self
    }
    
    /// Extract category name from KraveMart product's categories array
    /// Priority: 1) cat_search_elastic contains ";", 2) cat_search_elastic non-empty, 3) first category
    fn extract_category_name(&self, product: &Value) -> String {
        if let Some(categories) = product.get("categories").and_then(|c| c.as_array()) {


            // First priority: Find a category where cat_search_elastic contains ";"
            for category in categories {
                if let (Some(category_name), Some(cat_search_elastic)) = (
                    category.get("category_name").and_then(|n| n.as_str()),
                    category.get("cat_search_elastic").and_then(|e| e.as_str()),
                ) {
                    if !cat_search_elastic.is_empty() && cat_search_elastic.contains(";") {
                        // Found a category with ";" in cat_search_elastic - highest priority
                        return category_name.to_string();
                    }
                }
            }

            // Second priority: Find categories where cat_search_elastic is non-empty (but no ";")
            // Collect all such categories and prefer more specific ones
            let mut non_semicolon_categories = Vec::new();
            for category in categories {
                if let (Some(category_name), Some(cat_search_elastic)) = (
                    category.get("category_name").and_then(|n| n.as_str()),
                    category.get("cat_search_elastic").and_then(|e| e.as_str()),
                ) {
                    if !cat_search_elastic.is_empty() && !cat_search_elastic.contains(";") {
                        non_semicolon_categories.push((category_name, cat_search_elastic));
                    }
                }
            }



            // If we have multiple non-semicolon categories, prefer more specific ones
            if !non_semicolon_categories.is_empty() {
                // Define generic/broad terms that should be avoided if better options exist
                let generic_terms = [
                    "made in pakistan", "grocery", "monthly stock up", "food items",
                    "food & grocery", "household", "personal care", "beauty", "health"
                ];

                // Define preferred specific terms that indicate good categories
                let preferred_terms = [
                    "tea", "coffee", "black tea", "green tea", "sugar", "milk", "cheese",
                    "chocolate", "biscuit", "snack", "juice", "water", "oil", "ghee",
                    "rice", "flour", "spice", "sauce", "shampoo", "soap", "cream"
                ];

                // Strategy 1: Look for categories with preferred specific terms
                for (category_name, cat_search_elastic) in &non_semicolon_categories {
                    let elastic_lower = cat_search_elastic.to_lowercase();
                    if preferred_terms.iter().any(|&term| elastic_lower.contains(term)) {
                        return category_name.to_string();
                    }
                }

                // Strategy 2: Avoid generic terms, take the first non-generic
                for (category_name, cat_search_elastic) in &non_semicolon_categories {
                    let elastic_lower = cat_search_elastic.to_lowercase();
                    if !generic_terms.iter().any(|&generic| elastic_lower.contains(generic)) {
                        return category_name.to_string();
                    }
                }

                // Strategy 3: If all are generic, take the last one (often more specific than first)
                let (category_name, _cat_search_elastic) = &non_semicolon_categories[non_semicolon_categories.len() - 1];
                return category_name.to_string();
            }

            // Fallback: use the first category's name if available
            if let Some(first_category) = categories.first() {
                if let Some(category_name) = first_category.get("category_name").and_then(|n| n.as_str()) {
                    return category_name.to_string();
                }
            }
        }

        // Final fallback
        "Unknown Category".to_string()
    }
}

impl Preprocessor for KraveMartPreprocessor {
    fn can_process(&self, item: &Value) -> bool {
        // KraveMart products have a categories array with cat_search_elastic field
        if let Some(categories) = item.get("categories").and_then(|c| c.as_array()) {
            // Check if any category has cat_search_elastic field (KraveMart specific)
            return categories.iter().any(|cat| cat.get("cat_search_elastic").is_some());
        }
        false
    }
    
    fn process(&self, item: &Value) -> Result<Value> {
        let mut processed = item.clone();
        
        // Extract clean category name using KraveMart logic
        let category_name = self.extract_category_name(item);
        
        // Remove the complex categories array and add clean category_name
        if let Value::Object(ref mut map) = processed {
            map.remove("categories"); // Remove complex array to prevent flattening issues
            map.insert("category_name".to_string(), Value::String(category_name));
        }
        
        Ok(processed)
    }
    
    fn name(&self) -> &'static str {
        "KraveMart"
    }
}

impl Default for KraveMartPreprocessor {
    fn default() -> Self {
        Self::new()
    }
}
