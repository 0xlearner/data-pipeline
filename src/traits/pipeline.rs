use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

// Removed unused imports

/// Core trait for data processing pipelines
///
/// A pipeline orchestrates the flow of data from sources through processors
/// to final storage or output destinations.
#[async_trait]
pub trait Pipeline: Send + Sync {
    /// Get the pipeline name/identifier
    fn name(&self) -> &str;

    /// Execute the pipeline with the given context
    async fn execute(&self, context: PipelineContext) -> Result<PipelineResult>;

    /// Execute the pipeline for a specific source
    async fn execute_for_source(
        &self,
        source_name: &str,
        context: PipelineContext,
    ) -> Result<PipelineResult>;

    /// Get pipeline metadata
    fn metadata(&self) -> PipelineMetadata;

    /// Validate pipeline configuration
    fn validate(&self) -> Result<()>;

    /// Get pipeline health status
    async fn health_check(&self) -> Result<PipelineHealth>;
}

/// Pipeline execution context
#[derive(Debug, Clone)]
pub struct PipelineContext {
    pub execution_id: String,
    pub source_filters: Vec<String>,
    pub processor_config: HashMap<String, serde_json::Value>,
    pub output_config: OutputConfig,
    pub execution_mode: ExecutionMode,
    pub retry_config: RetryConfig,
    pub timeout: Option<Duration>,
    pub metadata: HashMap<String, String>,
}

/// Pipeline execution modes
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionMode {
    /// Process all data in a single batch
    Batch,
    /// Process data in streaming fashion
    Streaming,
    /// Process data incrementally (only new/changed data)
    Incremental,
    /// Dry run mode (validate without executing)
    DryRun,
}

/// Output configuration
#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub destinations: Vec<OutputDestination>,
    pub format: OutputFormat,
    pub compression: Option<CompressionType>,
    pub partitioning: Option<PartitioningConfig>,
}

/// Output destinations
#[derive(Debug, Clone)]
pub enum OutputDestination {
    Storage {
        path: String,
    },
    Database {
        connection_string: String,
        table: String,
    },
    Api {
        endpoint: String,
        headers: HashMap<String, String>,
    },
    File {
        path: String,
    },
    Memory,
}

/// Output formats
#[derive(Debug, Clone)]
pub enum OutputFormat {
    Json,
    Parquet,
    Csv,
    Avro,
    Delta,
}

/// Compression types
#[derive(Debug, Clone)]
pub enum CompressionType {
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Partitioning configuration
#[derive(Debug, Clone)]
pub struct PartitioningConfig {
    pub columns: Vec<String>,
    pub strategy: PartitioningStrategy,
}

/// Partitioning strategies
#[derive(Debug, Clone)]
pub enum PartitioningStrategy {
    Hash,
    Range,
    List,
    Time,
}

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
    pub retry_on_errors: Vec<String>,
}

/// Pipeline execution result
#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub execution_id: String,
    pub pipeline_name: String,
    pub status: ExecutionStatus,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    pub duration: Option<Duration>,
    pub sources_processed: Vec<SourceResult>,
    pub total_records_processed: u64,
    pub total_records_output: u64,
    pub errors: Vec<PipelineError>,
    pub warnings: Vec<PipelineWarning>,
    pub metrics: PipelineMetrics,
    pub output_locations: Vec<String>,
}

/// Execution status
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    PartiallyCompleted,
}

/// Source processing result
#[derive(Debug, Clone)]
pub struct SourceResult {
    pub source_name: String,
    pub status: ExecutionStatus,
    pub records_processed: u64,
    pub records_output: u64,
    pub duration: Duration,
    pub errors: Vec<PipelineError>,
}

/// Pipeline error
#[derive(Debug, Clone)]
pub struct PipelineError {
    pub error_type: ErrorType,
    pub message: String,
    pub source: Option<String>,
    pub processor: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub recoverable: bool,
}

/// Error types
#[derive(Debug, Clone)]
pub enum ErrorType {
    SourceError,
    ProcessorError,
    ValidationError,
    StorageError,
    ConfigurationError,
    TimeoutError,
    ResourceError,
}

/// Pipeline warning
#[derive(Debug, Clone)]
pub struct PipelineWarning {
    pub message: String,
    pub source: Option<String>,
    pub processor: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Pipeline metrics
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    pub throughput_records_per_second: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub network_io_mb: f64,
    pub disk_io_mb: f64,
    pub cache_hit_rate: f64,
    pub error_rate: f64,
}

/// Pipeline metadata
#[derive(Debug, Clone)]
pub struct PipelineMetadata {
    pub name: String,
    pub description: Option<String>,
    pub version: String,
    pub author: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub tags: Vec<String>,
    pub supported_sources: Vec<String>,
    pub supported_processors: Vec<String>,
    pub dependencies: Vec<String>,
}

/// Pipeline health status
#[derive(Debug, Clone)]
pub struct PipelineHealth {
    pub is_healthy: bool,
    pub status: HealthStatus,
    pub checks: Vec<HealthCheck>,
    pub last_execution: Option<chrono::DateTime<chrono::Utc>>,
    pub uptime: Duration,
}

/// Health status levels
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Individual health check
#[derive(Debug, Clone)]
pub struct HealthCheck {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
    pub duration: Duration,
}

/// Trait for configurable pipelines
pub trait ConfigurablePipeline {
    type Config;

    /// Create a new pipeline from configuration
    fn from_config(config: Self::Config) -> Result<Self>
    where
        Self: Sized;

    /// Update pipeline configuration
    fn update_config(&mut self, config: Self::Config) -> Result<()>;

    /// Get current configuration
    fn get_config(&self) -> &Self::Config;
}

/// Trait for pipelines that support monitoring
#[async_trait]
pub trait MonitorablePipeline: Pipeline {
    /// Get real-time metrics
    async fn get_metrics(&self) -> Result<PipelineMetrics>;

    /// Get execution history
    async fn get_execution_history(&self, limit: Option<usize>) -> Result<Vec<PipelineResult>>;

    /// Subscribe to pipeline events
    async fn subscribe_to_events(&self) -> Result<Box<dyn PipelineEventStream>>;
}

/// Pipeline event stream
#[async_trait]
pub trait PipelineEventStream: Send + Sync {
    /// Get the next event
    async fn next_event(&mut self) -> Result<Option<PipelineEvent>>;

    /// Close the event stream
    async fn close(&mut self) -> Result<()>;
}

/// Pipeline events
#[derive(Debug, Clone)]
pub enum PipelineEvent {
    ExecutionStarted {
        execution_id: String,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    ExecutionCompleted {
        execution_id: String,
        result: PipelineResult,
    },
    ExecutionFailed {
        execution_id: String,
        error: PipelineError,
    },
    SourceProcessingStarted {
        execution_id: String,
        source_name: String,
    },
    SourceProcessingCompleted {
        execution_id: String,
        source_result: SourceResult,
    },
    ProcessorStarted {
        execution_id: String,
        processor_name: String,
    },
    ProcessorCompleted {
        execution_id: String,
        processor_name: String,
        duration: Duration,
    },
    MetricsUpdated {
        metrics: PipelineMetrics,
    },
}

/// Default implementations for common pipeline context operations
impl Default for PipelineContext {
    fn default() -> Self {
        Self {
            execution_id: uuid::Uuid::new_v4().to_string(),
            source_filters: Vec::new(),
            processor_config: HashMap::new(),
            output_config: OutputConfig::default(),
            execution_mode: ExecutionMode::Batch,
            retry_config: RetryConfig::default(),
            timeout: None,
            metadata: HashMap::new(),
        }
    }
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            destinations: vec![OutputDestination::Memory],
            format: OutputFormat::Json,
            compression: None,
            partitioning: None,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            retry_on_errors: Vec::new(),
        }
    }
}

impl PipelineResult {
    /// Check if the pipeline execution was successful
    pub fn is_success(&self) -> bool {
        matches!(self.status, ExecutionStatus::Completed)
    }

    /// Check if the pipeline execution had partial success
    pub fn is_partial_success(&self) -> bool {
        matches!(self.status, ExecutionStatus::PartiallyCompleted)
    }

    /// Get the success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.sources_processed.is_empty() {
            return 0.0;
        }

        let successful = self
            .sources_processed
            .iter()
            .filter(|s| s.status == ExecutionStatus::Completed)
            .count();

        successful as f64 / self.sources_processed.len() as f64
    }
}
