use anyhow::Result;
use polars::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;

/// Core trait for modular processing stages
/// 
/// Each stage represents a single transformation step that can be composed
/// into a processing pipeline. Stages are designed to be:
/// - **Composable**: Can be chained together in different orders
/// - **Reusable**: Same stage can be used across different source types
/// - **Testable**: Each stage can be tested independently
/// - **Configurable**: Stages can be configured for different behaviors
pub trait ProcessingStage: Send + Sync {
    /// Get the stage name/identifier
    fn name(&self) -> &str;
    
    /// Get stage metadata (type, description, etc.)
    fn metadata(&self) -> StageMetadata;
    
    /// Process data through this stage
    fn process(&self, input: ProcessingData) -> Result<StageResult>;
    
    /// Check if this stage can handle the given input type
    fn can_process(&self, input: &ProcessingData) -> bool;
    
    /// Get the expected output type for a given input type
    fn output_type(&self, input_type: &ProcessingDataType) -> Result<ProcessingDataType>;
    
    /// Validate stage configuration (optional)
    fn validate_config(&self) -> Result<()> {
        Ok(())
    }
}

/// Data that flows between processing stages
#[derive(Debug, Clone)]
pub enum ProcessingData {
    /// Raw JSON data from API sources
    Json(Vec<Value>),
    /// Structured DataFrame for tabular operations
    DataFrame(DataFrame),
    /// Key-value metadata and metrics
    Metadata(HashMap<String, String>),
    /// Raw text data (for future HTML processing)
    Text(String),
}

/// Types of processing data
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessingDataType {
    Json,
    DataFrame,
    Metadata,
    Text,
}

/// Result of a processing stage
#[derive(Debug)]
pub struct StageResult {
    /// The processed data
    pub data: ProcessingData,
    /// Processing metrics
    pub metrics: StageMetrics,
    /// Any warnings or non-fatal issues
    pub warnings: Vec<String>,
}

/// Metrics collected during stage processing
#[derive(Debug, Clone)]
pub struct StageMetrics {
    /// Time taken to process
    pub processing_time_ms: u64,
    /// Number of items processed
    pub items_processed: usize,
    /// Number of items that failed processing
    pub items_failed: usize,
    /// Memory usage in MB (optional)
    pub memory_usage_mb: Option<f64>,
}

/// Metadata about a processing stage
#[derive(Debug, Clone)]
pub struct StageMetadata {
    /// Stage type/category
    pub stage_type: StageType,
    /// Human-readable description
    pub description: String,
    /// Version of the stage implementation
    pub version: String,
    /// Input types this stage can handle
    pub supported_inputs: Vec<ProcessingDataType>,
    /// Output types this stage can produce
    pub supported_outputs: Vec<ProcessingDataType>,
}

/// Categories of processing stages
#[derive(Debug, Clone, PartialEq)]
pub enum StageType {
    /// Transforms data structure (JSON to DataFrame, etc.)
    Transformer,
    /// Cleans and normalizes data
    Normalizer,
    /// Classifies and maps fields
    Classifier,
    /// Validates data quality
    Validator,
    /// Enriches data with additional information
    Enricher,
}

impl ProcessingData {
    /// Get the type of this processing data
    pub fn data_type(&self) -> ProcessingDataType {
        match self {
            ProcessingData::Json(_) => ProcessingDataType::Json,
            ProcessingData::DataFrame(_) => ProcessingDataType::DataFrame,
            ProcessingData::Metadata(_) => ProcessingDataType::Metadata,
            ProcessingData::Text(_) => ProcessingDataType::Text,
        }
    }
    
    /// Check if this data is empty
    pub fn is_empty(&self) -> bool {
        match self {
            ProcessingData::Json(data) => data.is_empty(),
            ProcessingData::DataFrame(df) => df.height() == 0,
            ProcessingData::Metadata(map) => map.is_empty(),
            ProcessingData::Text(text) => text.is_empty(),
        }
    }
    
    /// Get the size/count of items in this data
    pub fn size(&self) -> usize {
        match self {
            ProcessingData::Json(data) => data.len(),
            ProcessingData::DataFrame(df) => df.height(),
            ProcessingData::Metadata(map) => map.len(),
            ProcessingData::Text(text) => text.len(),
        }
    }
}

impl StageResult {
    /// Create a new successful stage result
    pub fn success(data: ProcessingData, processing_time: u64, items_processed: usize) -> Self {
        Self {
            data,
            metrics: StageMetrics {
                processing_time_ms: processing_time,
                items_processed,
                items_failed: 0,
                memory_usage_mb: None,
            },
            warnings: Vec::new(),
        }
    }
    
    /// Create a stage result with some failures
    pub fn partial_success(
        data: ProcessingData,
        processing_time: u64,
        items_processed: usize,
        items_failed: usize,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            data,
            metrics: StageMetrics {
                processing_time_ms: processing_time,
                items_processed,
                items_failed,
                memory_usage_mb: None,
            },
            warnings,
        }
    }
    
    /// Add a warning to the result
    pub fn with_warning(mut self, warning: String) -> Self {
        self.warnings.push(warning);
        self
    }
    
    /// Set memory usage metric
    pub fn with_memory_usage(mut self, memory_mb: f64) -> Self {
        self.metrics.memory_usage_mb = Some(memory_mb);
        self
    }
}

/// Helper trait for timing stage operations
pub trait StageTimer {
    fn time_operation<F, R>(&self, operation: F) -> (R, u64)
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = operation();
        let duration = start.elapsed().as_millis() as u64;
        (result, duration)
    }
}

// Implement StageTimer for all ProcessingStage implementations
impl<T: ProcessingStage> StageTimer for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_processing_data_types() {
        let json_data = ProcessingData::Json(vec![json!({"test": "value"})]);
        assert_eq!(json_data.data_type(), ProcessingDataType::Json);
        assert_eq!(json_data.size(), 1);
        assert!(!json_data.is_empty());

        let empty_json = ProcessingData::Json(vec![]);
        assert!(empty_json.is_empty());
    }

    #[test]
    fn test_stage_result_creation() {
        let data = ProcessingData::Json(vec![json!({"test": "value"})]);
        let result = StageResult::success(data, 100, 1);
        
        assert_eq!(result.metrics.processing_time_ms, 100);
        assert_eq!(result.metrics.items_processed, 1);
        assert_eq!(result.metrics.items_failed, 0);
        assert!(result.warnings.is_empty());
    }
}
