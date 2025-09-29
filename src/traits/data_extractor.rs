use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Core trait for data extractors
/// 
/// Data extractors are responsible for extracting structured data from various
/// raw formats (JSON, HTML, XML, etc.) and converting them to a standardized format.
#[async_trait]
pub trait DataExtractor: Send + Sync {
    /// Get the extractor name/identifier
    fn name(&self) -> &str;
    
    /// Get the extractor type
    fn extractor_type(&self) -> ExtractorType;
    
    /// Extract data from raw input
    async fn extract(&self, input: ExtractorInput) -> Result<ExtractorOutput>;
    
    /// Check if this extractor can handle the given input type
    fn can_extract(&self, input_type: &InputType) -> bool;
    
    /// Get extractor metadata
    fn metadata(&self) -> ExtractorMetadata;
    
    /// Validate extraction configuration
    fn validate_config(&self) -> Result<()>;
}

/// Extractor type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ExtractorType {
    Json,
    Html,
    Xml,
    Csv,
    Text,
    Binary,
    Api,
    Database,
}

/// Input types for extractors
#[derive(Debug, Clone, PartialEq)]
pub enum InputType {
    Json,
    Html,
    Xml,
    Csv,
    Text,
    Binary,
    Url,
}

/// Input data for extractors
#[derive(Debug, Clone)]
pub enum ExtractorInput {
    Json(Value),
    Html(String),
    Xml(String),
    Csv(String),
    Text(String),
    Binary(Vec<u8>),
    Url(String),
}

/// Output data from extractors
#[derive(Debug, Clone)]
pub enum ExtractorOutput {
    Records(Vec<ExtractedRecord>),
    KeyValue(HashMap<String, Value>),
    Structured(Value),
    Raw(Vec<u8>),
}

/// Extracted record with metadata
#[derive(Debug, Clone)]
pub struct ExtractedRecord {
    pub id: Option<String>,
    pub data: HashMap<String, Value>,
    pub metadata: RecordMetadata,
    pub confidence: f64, // 0.0 to 1.0
}

/// Record metadata
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub source_location: Option<String>,
    pub extraction_timestamp: chrono::DateTime<chrono::Utc>,
    pub extractor_version: String,
    pub quality_score: f64,
    pub tags: Vec<String>,
}

/// Extractor metadata
#[derive(Debug, Clone)]
pub struct ExtractorMetadata {
    pub name: String,
    pub description: Option<String>,
    pub version: String,
    pub supported_input_types: Vec<InputType>,
    pub supported_output_formats: Vec<OutputFormat>,
    pub configuration_schema: Option<Value>,
    pub performance_metrics: Option<ExtractionMetrics>,
}

/// Output formats supported by extractors
#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Records,
    KeyValue,
    Structured,
    Raw,
}

/// Extraction performance metrics
#[derive(Debug, Clone)]
pub struct ExtractionMetrics {
    pub average_extraction_time_ms: f64,
    pub throughput_records_per_second: f64,
    pub accuracy_rate: f64,
    pub memory_usage_mb: f64,
}

/// Trait for configurable extractors
pub trait ConfigurableExtractor {
    type Config;
    
    /// Create a new extractor from configuration
    fn from_config(config: Self::Config) -> Result<Self>
    where
        Self: Sized;
    
    /// Update extractor configuration
    fn update_config(&mut self, config: Self::Config) -> Result<()>;
    
    /// Get current configuration
    fn get_config(&self) -> &Self::Config;
}

/// Trait for extractors that support schema inference
#[async_trait]
pub trait SchemaInferenceExtractor: DataExtractor {
    /// Infer schema from sample data
    async fn infer_schema(&self, sample: ExtractorInput) -> Result<DataSchema>;
    
    /// Validate data against inferred schema
    async fn validate_against_schema(&self, input: ExtractorInput, schema: &DataSchema) -> Result<SchemaValidationResult>;
}

/// Data schema definition
#[derive(Debug, Clone)]
pub struct DataSchema {
    pub fields: Vec<FieldSchema>,
    pub constraints: Vec<SchemaConstraint>,
    pub metadata: HashMap<String, Value>,
}

/// Field schema definition
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub description: Option<String>,
    pub constraints: Vec<FieldConstraint>,
}

/// Field types
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    Array(Box<FieldType>),
    Object(Vec<FieldSchema>),
    Null,
}

/// Field constraints
#[derive(Debug, Clone, PartialEq)]
pub enum FieldConstraint {
    MinLength(usize),
    MaxLength(usize),
    Pattern(String),
    MinValue(f64),
    MaxValue(f64),
    Enum(Vec<String>),
    Unique,
}

/// Schema constraints
#[derive(Debug, Clone)]
pub enum SchemaConstraint {
    UniqueKey(Vec<String>),
    ForeignKey { fields: Vec<String>, reference_table: String, reference_fields: Vec<String> },
    Check(String),
}

/// Schema validation result
#[derive(Debug, Clone)]
pub struct SchemaValidationResult {
    pub is_valid: bool,
    pub errors: Vec<SchemaValidationError>,
    pub warnings: Vec<SchemaValidationWarning>,
}

/// Schema validation error
#[derive(Debug, Clone)]
pub struct SchemaValidationError {
    pub field: String,
    pub message: String,
    pub error_type: SchemaErrorType,
}

/// Schema validation warning
#[derive(Debug, Clone)]
pub struct SchemaValidationWarning {
    pub field: String,
    pub message: String,
}

/// Schema error types
#[derive(Debug, Clone)]
pub enum SchemaErrorType {
    TypeMismatch,
    ConstraintViolation,
    MissingField,
    UnexpectedField,
    InvalidFormat,
}

/// Trait for extractors that support pattern-based extraction
#[async_trait]
pub trait PatternExtractor: DataExtractor {
    /// Extract data using predefined patterns
    async fn extract_with_patterns(&self, input: ExtractorInput, patterns: &[ExtractionPattern]) -> Result<ExtractorOutput>;
    
    /// Get available extraction patterns
    fn get_available_patterns(&self) -> Vec<ExtractionPattern>;
    
    /// Create a custom extraction pattern
    fn create_pattern(&self, name: String, definition: PatternDefinition) -> Result<ExtractionPattern>;
}

/// Extraction pattern definition
#[derive(Debug, Clone)]
pub struct ExtractionPattern {
    pub name: String,
    pub description: Option<String>,
    pub pattern_type: PatternType,
    pub definition: PatternDefinition,
    pub confidence_threshold: f64,
}

/// Pattern types
#[derive(Debug, Clone)]
pub enum PatternType {
    Regex,
    XPath,
    CssSelector,
    JsonPath,
    Custom,
}

/// Pattern definition
#[derive(Debug, Clone)]
pub enum PatternDefinition {
    Regex(String),
    XPath(String),
    CssSelector(String),
    JsonPath(String),
    Custom(HashMap<String, Value>),
}

/// Trait for extractors that support machine learning
#[async_trait]
pub trait MLExtractor: DataExtractor {
    /// Train the extractor on sample data
    async fn train(&mut self, training_data: Vec<TrainingExample>) -> Result<TrainingResult>;
    
    /// Evaluate extractor performance
    async fn evaluate(&self, test_data: Vec<TrainingExample>) -> Result<EvaluationResult>;
    
    /// Get model information
    fn get_model_info(&self) -> ModelInfo;
    
    /// Save trained model
    async fn save_model(&self, path: &str) -> Result<()>;
    
    /// Load trained model
    async fn load_model(&mut self, path: &str) -> Result<()>;
}

/// Training example for ML extractors
#[derive(Debug, Clone)]
pub struct TrainingExample {
    pub input: ExtractorInput,
    pub expected_output: ExtractorOutput,
    pub weight: f64,
}

/// Training result
#[derive(Debug, Clone)]
pub struct TrainingResult {
    pub success: bool,
    pub training_accuracy: f64,
    pub validation_accuracy: f64,
    pub training_time: std::time::Duration,
    pub model_size_bytes: usize,
    pub iterations: u32,
}

/// Evaluation result
#[derive(Debug, Clone)]
pub struct EvaluationResult {
    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,
    pub confusion_matrix: Vec<Vec<u32>>,
    pub evaluation_time: std::time::Duration,
}

/// Model information
#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub model_type: String,
    pub version: String,
    pub training_date: chrono::DateTime<chrono::Utc>,
    pub feature_count: usize,
    pub accuracy: f64,
    pub size_bytes: usize,
}

/// Extractor registry for managing multiple extractors
pub struct ExtractorRegistry {
    extractors: HashMap<String, Box<dyn DataExtractor>>,
}

impl ExtractorRegistry {
    /// Create a new extractor registry
    pub fn new() -> Self {
        Self {
            extractors: HashMap::new(),
        }
    }
    
    /// Register an extractor
    pub fn register(&mut self, name: String, extractor: Box<dyn DataExtractor>) {
        self.extractors.insert(name, extractor);
    }
    
    /// Get an extractor by name
    pub fn get(&self, name: &str) -> Option<&dyn DataExtractor> {
        self.extractors.get(name).map(|e| e.as_ref())
    }
    
    /// List all registered extractors
    pub fn list_extractors(&self) -> Vec<&str> {
        self.extractors.keys().map(|s| s.as_str()).collect()
    }
    
    /// Find extractors that can handle a specific input type
    pub fn find_compatible_extractors(&self, input_type: &InputType) -> Vec<&str> {
        self.extractors
            .iter()
            .filter(|(_, extractor)| extractor.can_extract(input_type))
            .map(|(name, _)| name.as_str())
            .collect()
    }
    
    /// Remove an extractor
    pub fn remove(&mut self, name: &str) -> Option<Box<dyn DataExtractor>> {
        self.extractors.remove(name)
    }
}

impl Default for ExtractorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
