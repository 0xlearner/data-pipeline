use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

/// Trait for extracting fields from preprocessed JSON data
pub trait FieldExtractor: Send + Sync {
    /// Extract fields from a JSON item into a flat HashMap
    fn extract_fields(&self, item: &Value) -> Result<HashMap<String, String>>;

    /// Get the name of this field extractor for logging
    fn name(&self) -> &'static str;
}
