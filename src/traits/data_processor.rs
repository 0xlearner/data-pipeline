use anyhow::Result;
use async_trait::async_trait;
use polars::prelude::*;
use serde_json::Value;
use std::collections::HashMap;

/// Core trait for data processors in the pipeline
/// 
/// Data processors transform data from one format to another or apply
/// various transformations like cleaning, normalization, validation, etc.
#[async_trait]
pub trait DataProcessor: Send + Sync {
    /// Get the processor name/identifier
    fn name(&self) -> &str;
    
    /// Get the processor type
    fn processor_type(&self) -> ProcessorType;
    
    /// Process data and return the result
    async fn process(&self, input: ProcessorInput) -> Result<ProcessorOutput>;
    
    /// Check if this processor can handle the given input type
    fn can_process(&self, input_type: &DataType) -> bool;
    
    /// Get the expected output type for a given input type
    fn output_type(&self, input_type: &DataType) -> Result<DataType>;
    
    /// Get processor metadata
    fn metadata(&self) -> ProcessorMetadata;
}

/// Processor type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessorType {
    Flattener,
    Classifier,
    Normalizer,
    Validator,
    Transformer,
    Aggregator,
    Filter,
    Enricher,
}

/// Data types supported by processors
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Json,
    DataFrame,
    Html,
    Text,
    Binary,
    Csv,
    Parquet,
}

/// Input data for processors
#[derive(Debug)]
pub enum ProcessorInput {
    Json(Vec<Value>),
    DataFrame(DataFrame),
    Html(String),
    Text(String),
    Binary(Vec<u8>),
    KeyValue(HashMap<String, Value>),
}

/// Output data from processors
#[derive(Debug)]
pub enum ProcessorOutput {
    Json(Vec<Value>),
    DataFrame(DataFrame),
    Html(String),
    Text(String),
    Binary(Vec<u8>),
    KeyValue(HashMap<String, Value>),
    ValidationResult(ValidationResult),
}

/// Processor metadata
#[derive(Debug, Clone)]
pub struct ProcessorMetadata {
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub supported_input_types: Vec<DataType>,
    pub supported_output_types: Vec<DataType>,
    pub configuration_schema: Option<Value>,
    pub performance_metrics: Option<PerformanceMetrics>,
}

/// Performance metrics for processors
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub average_processing_time_ms: f64,
    pub throughput_items_per_second: f64,
    pub memory_usage_mb: f64,
    pub success_rate: f64,
}

/// Trait for configurable processors
pub trait ConfigurableProcessor {
    type Config;
    
    /// Create a new processor from configuration
    fn from_config(config: Self::Config) -> Result<Self>
    where
        Self: Sized;
    
    /// Update processor configuration
    fn update_config(&mut self, config: Self::Config) -> Result<()>;
    
    /// Get current configuration
    fn get_config(&self) -> &Self::Config;
    
    /// Validate configuration
    fn validate_config(config: &Self::Config) -> Result<()>;
}

/// Trait for processors that support batch processing
#[async_trait]
pub trait BatchProcessor: DataProcessor {
    /// Process data in batches
    async fn process_batch(&self, inputs: Vec<ProcessorInput>, batch_size: usize) -> Result<Vec<ProcessorOutput>>;
    
    /// Get optimal batch size for this processor
    fn optimal_batch_size(&self) -> usize;
    
    /// Check if batch processing is more efficient than single item processing
    fn prefers_batch_processing(&self) -> bool;
}

/// Trait for processors that support streaming
#[async_trait]
pub trait StreamingProcessor: DataProcessor {
    /// Start processing a stream of data
    async fn start_stream(&self) -> Result<Box<dyn ProcessorStream>>;
    
    /// Check if streaming is supported
    fn supports_streaming(&self) -> bool;
}

/// Processor stream interface
#[async_trait]
pub trait ProcessorStream: Send + Sync {
    /// Process the next input item
    async fn process_next(&mut self, input: ProcessorInput) -> Result<Option<ProcessorOutput>>;
    
    /// Flush any pending outputs
    async fn flush(&mut self) -> Result<Vec<ProcessorOutput>>;
    
    /// Close the stream
    async fn close(&mut self) -> Result<()>;
}

/// Trait for data validators
#[async_trait]
pub trait DataValidator: DataProcessor {
    /// Validate data and return validation result
    async fn validate(&self, input: ProcessorInput) -> Result<ValidationResult>;
    
    /// Get validation rules
    fn get_validation_rules(&self) -> Vec<ValidationRule>;
    
    /// Check if data passes all validation rules
    async fn is_valid(&self, input: ProcessorInput) -> Result<bool> {
        let result = self.validate(input).await?;
        Ok(result.is_valid())
    }
}

/// Validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationWarning>,
    pub metrics: ValidationMetrics,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.is_valid && self.errors.is_empty()
    }
    
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Validation error
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub rule_name: String,
    pub message: String,
    pub field: Option<String>,
    pub row_index: Option<usize>,
    pub severity: ErrorSeverity,
}

/// Validation warning
#[derive(Debug, Clone)]
pub struct ValidationWarning {
    pub rule_name: String,
    pub message: String,
    pub field: Option<String>,
    pub row_index: Option<usize>,
}

/// Error severity levels
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorSeverity {
    Critical,
    High,
    Medium,
    Low,
}

/// Validation metrics
#[derive(Debug, Clone)]
pub struct ValidationMetrics {
    pub total_records: usize,
    pub valid_records: usize,
    pub invalid_records: usize,
    pub records_with_warnings: usize,
    pub validation_time_ms: u64,
}

/// Validation rule definition
#[derive(Debug, Clone)]
pub struct ValidationRule {
    pub name: String,
    pub description: String,
    pub rule_type: ValidationRuleType,
    pub severity: ErrorSeverity,
    pub enabled: bool,
}

/// Types of validation rules
#[derive(Debug, Clone)]
pub enum ValidationRuleType {
    Required,
    DataType,
    Range { min: f64, max: f64 },
    Length { min: usize, max: usize },
    Pattern(String),
    Custom(String),
    Uniqueness,
    Referential,
}

/// Trait for data transformers
#[async_trait]
pub trait DataTransformer: DataProcessor {
    /// Transform data from one format to another
    async fn transform(&self, input: ProcessorInput, target_type: DataType) -> Result<ProcessorOutput>;
    
    /// Get supported transformation paths
    fn get_transformation_paths(&self) -> Vec<TransformationPath>;
    
    /// Check if transformation is supported
    fn supports_transformation(&self, from: &DataType, to: &DataType) -> bool;
}

/// Transformation path definition
#[derive(Debug, Clone)]
pub struct TransformationPath {
    pub from: DataType,
    pub to: DataType,
    pub cost: u32, // Relative cost of transformation
    pub quality_loss: f32, // 0.0 = no loss, 1.0 = complete loss
}

/// Processor chain for composing multiple processors
pub struct ProcessorChain {
    processors: Vec<Box<dyn DataProcessor>>,
    name: String,
}

impl ProcessorChain {
    /// Create a new processor chain
    pub fn new(name: String) -> Self {
        Self {
            processors: Vec::new(),
            name,
        }
    }
    
    /// Add a processor to the chain
    pub fn add_processor(&mut self, processor: Box<dyn DataProcessor>) -> Result<()> {
        // Validate that the processor can be added to the chain
        if let Some(last_processor) = self.processors.last() {
            let last_output_type = last_processor.output_type(&DataType::Json)?; // Simplified
            if !processor.can_process(&last_output_type) {
                return Err(anyhow::anyhow!(
                    "Processor {} cannot process output from {}",
                    processor.name(),
                    last_processor.name()
                ));
            }
        }
        
        self.processors.push(processor);
        Ok(())
    }
    
    /// Get the number of processors in the chain
    pub fn len(&self) -> usize {
        self.processors.len()
    }
    
    /// Check if the chain is empty
    pub fn is_empty(&self) -> bool {
        self.processors.is_empty()
    }
}

#[async_trait]
impl DataProcessor for ProcessorChain {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Transformer // Chain is a composite transformer
    }
    
    async fn process(&self, mut input: ProcessorInput) -> Result<ProcessorOutput> {
        for processor in &self.processors {
            let output = processor.process(input).await?;
            input = match output {
                ProcessorOutput::Json(data) => ProcessorInput::Json(data),
                ProcessorOutput::DataFrame(df) => ProcessorInput::DataFrame(df),
                ProcessorOutput::Html(html) => ProcessorInput::Html(html),
                ProcessorOutput::Text(text) => ProcessorInput::Text(text),
                ProcessorOutput::Binary(data) => ProcessorInput::Binary(data),
                ProcessorOutput::KeyValue(kv) => ProcessorInput::KeyValue(kv),
                ProcessorOutput::ValidationResult(_) => {
                    return Err(anyhow::anyhow!("Cannot chain validation results"));
                }
            };
        }
        
        // Convert final input back to output
        match input {
            ProcessorInput::Json(data) => Ok(ProcessorOutput::Json(data)),
            ProcessorInput::DataFrame(df) => Ok(ProcessorOutput::DataFrame(df)),
            ProcessorInput::Html(html) => Ok(ProcessorOutput::Html(html)),
            ProcessorInput::Text(text) => Ok(ProcessorOutput::Text(text)),
            ProcessorInput::Binary(data) => Ok(ProcessorOutput::Binary(data)),
            ProcessorInput::KeyValue(kv) => Ok(ProcessorOutput::KeyValue(kv)),
        }
    }
    
    fn can_process(&self, input_type: &DataType) -> bool {
        self.processors.first()
            .map(|p| p.can_process(input_type))
            .unwrap_or(false)
    }
    
    fn output_type(&self, input_type: &DataType) -> Result<DataType> {
        let mut current_type = input_type.clone();
        for processor in &self.processors {
            current_type = processor.output_type(&current_type)?;
        }
        Ok(current_type)
    }
    
    fn metadata(&self) -> ProcessorMetadata {
        ProcessorMetadata {
            name: self.name.clone(),
            description: Some("Processor chain".to_string()),
            version: None,
            supported_input_types: self.processors.first()
                .map(|p| p.metadata().supported_input_types)
                .unwrap_or_default(),
            supported_output_types: self.processors.last()
                .map(|p| p.metadata().supported_output_types)
                .unwrap_or_default(),
            configuration_schema: None,
            performance_metrics: None,
        }
    }
}
