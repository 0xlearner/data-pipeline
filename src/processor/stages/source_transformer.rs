use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

use crate::extractor::ScrapedProduct;

/// Trait for transforming source-specific raw data into standardized JSON format
/// 
/// Each data source (Pandamart, Naheed, HTML scraping, etc.) implements this trait
/// to convert their specific data structure into a unified JSON format that can
/// be processed by the standard pipeline stages.
pub trait SourceTransformer: Send + Sync {
    /// Get the source type this transformer handles
    fn source_type(&self) -> SourceType;
    
    /// Get the transformer name/identifier
    fn name(&self) -> &str;
    
    /// Transform raw source data to standardized JSON format
    fn transform(&self, raw_data: RawSourceData) -> Result<TransformationResult>;
    
    /// Get field mappings specific to this source
    fn get_field_mappings(&self) -> HashMap<String, String>;
    
    /// Validate that the raw data is compatible with this transformer
    fn can_transform(&self, raw_data: &RawSourceData) -> bool;
    
    /// Get transformation configuration for this source
    fn get_config(&self) -> TransformerConfig {
        TransformerConfig::default()
    }
}

/// Types of data sources
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceType {
    /// Generic JSON API source
    JsonApi,
    /// Pandamart GraphQL API
    Pandamart,
    /// Naheed store API
    Naheed,
    /// HTML web scraping
    HtmlScraping,
    /// CSV file import
    Csv,
    /// Custom source type
    Custom(String),
}

/// Raw data from different sources
#[derive(Debug)]
pub enum RawSourceData {
    /// JSON data from API responses
    Json(Vec<Value>),
    /// Scraped products from HTML
    Html(Vec<ScrapedProduct>),
    /// Raw text data
    Text(String),
    /// CSV data
    Csv(String),
    /// Binary data
    Binary(Vec<u8>),
}

/// Result of source transformation
#[derive(Debug)]
pub struct TransformationResult {
    /// Standardized JSON data
    pub data: Vec<Value>,
    /// Transformation metrics
    pub metrics: TransformationMetrics,
    /// Source-specific metadata
    pub metadata: HashMap<String, String>,
    /// Any warnings during transformation
    pub warnings: Vec<String>,
}

/// Metrics from source transformation
#[derive(Debug, Clone)]
pub struct TransformationMetrics {
    /// Number of items successfully transformed
    pub items_transformed: usize,
    /// Number of items that failed transformation
    pub items_failed: usize,
    /// Time taken for transformation in milliseconds
    pub transformation_time_ms: u64,
    /// Source-specific metrics
    pub source_metrics: HashMap<String, f64>,
}

/// Configuration for source transformers
#[derive(Debug, Clone)]
pub struct TransformerConfig {
    /// Whether to skip invalid items or fail completely
    pub skip_invalid_items: bool,
    /// Maximum number of items to process (0 = no limit)
    pub max_items: usize,
    /// Custom field mappings to override defaults
    pub custom_field_mappings: HashMap<String, String>,
    /// Source-specific configuration
    pub source_config: HashMap<String, Value>,
}

impl Default for TransformerConfig {
    fn default() -> Self {
        Self {
            skip_invalid_items: true,
            max_items: 0,
            custom_field_mappings: HashMap::new(),
            source_config: HashMap::new(),
        }
    }
}

impl SourceType {
    /// Get a string representation of the source type
    pub fn as_str(&self) -> &str {
        match self {
            SourceType::JsonApi => "json_api",
            SourceType::Pandamart => "pandamart",
            SourceType::Naheed => "naheed",
            SourceType::HtmlScraping => "html_scraping",
            SourceType::Csv => "csv",
            SourceType::Custom(name) => name,
        }
    }
    
    /// Create a source type from string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json_api" => SourceType::JsonApi,
            "pandamart" => SourceType::Pandamart,
            "naheed" => SourceType::Naheed,
            "html_scraping" => SourceType::HtmlScraping,
            "csv" => SourceType::Csv,
            _ => SourceType::Custom(s.to_string()),
        }
    }
}

impl RawSourceData {
    /// Get the type of raw data
    pub fn data_type(&self) -> &str {
        match self {
            RawSourceData::Json(_) => "json",
            RawSourceData::Html(_) => "html",
            RawSourceData::Text(_) => "text",
            RawSourceData::Csv(_) => "csv",
            RawSourceData::Binary(_) => "binary",
        }
    }
    
    /// Check if the raw data is empty
    pub fn is_empty(&self) -> bool {
        match self {
            RawSourceData::Json(data) => data.is_empty(),
            RawSourceData::Html(data) => data.is_empty(),
            RawSourceData::Text(data) => data.is_empty(),
            RawSourceData::Csv(data) => data.is_empty(),
            RawSourceData::Binary(data) => data.is_empty(),
        }
    }
    
    /// Get the size/count of items
    pub fn size(&self) -> usize {
        match self {
            RawSourceData::Json(data) => data.len(),
            RawSourceData::Html(data) => data.len(),
            RawSourceData::Text(data) => data.len(),
            RawSourceData::Csv(data) => data.lines().count(),
            RawSourceData::Binary(data) => data.len(),
        }
    }
}

impl TransformationResult {
    /// Create a successful transformation result
    pub fn success(
        data: Vec<Value>,
        items_transformed: usize,
        transformation_time_ms: u64,
    ) -> Self {
        Self {
            data,
            metrics: TransformationMetrics {
                items_transformed,
                items_failed: 0,
                transformation_time_ms,
                source_metrics: HashMap::new(),
            },
            metadata: HashMap::new(),
            warnings: Vec::new(),
        }
    }
    
    /// Create a partial success result with some failures
    pub fn partial_success(
        data: Vec<Value>,
        items_transformed: usize,
        items_failed: usize,
        transformation_time_ms: u64,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            data,
            metrics: TransformationMetrics {
                items_transformed,
                items_failed,
                transformation_time_ms,
                source_metrics: HashMap::new(),
            },
            metadata: HashMap::new(),
            warnings,
        }
    }
    
    /// Add metadata to the result
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
    
    /// Add source-specific metric
    pub fn with_source_metric(mut self, key: String, value: f64) -> Self {
        self.metrics.source_metrics.insert(key, value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_source_type_conversion() {
        assert_eq!(SourceType::from_str("pandamart"), SourceType::Pandamart);
        assert_eq!(SourceType::Pandamart.as_str(), "pandamart");
        
        let custom = SourceType::from_str("my_custom_source");
        assert_eq!(custom, SourceType::Custom("my_custom_source".to_string()));
    }

    #[test]
    fn test_raw_source_data() {
        let json_data = RawSourceData::Json(vec![json!({"test": "value"})]);
        assert_eq!(json_data.data_type(), "json");
        assert_eq!(json_data.size(), 1);
        assert!(!json_data.is_empty());

        let empty_data = RawSourceData::Json(vec![]);
        assert!(empty_data.is_empty());
    }

    #[test]
    fn test_transformation_result() {
        let result = TransformationResult::success(
            vec![json!({"test": "value"})],
            1,
            100,
        );
        
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.metrics.items_transformed, 1);
        assert_eq!(result.metrics.items_failed, 0);
        assert_eq!(result.metrics.transformation_time_ms, 100);
    }
}
