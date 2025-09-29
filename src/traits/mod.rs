pub mod data_extractor;
pub mod data_processor;
pub mod data_source;
pub mod pipeline;
pub mod storage;

// Re-export main traits
pub use data_extractor::{DataExtractor, OutputFormat as ExtractorOutputFormat};
pub use data_processor::{DataProcessor, ProcessorInput, ProcessorOutput, ProcessorType};
pub use data_source::{DataSource, RawSourceData, SourceHealth, SourceMetadata, SourceType};
pub use pipeline::{
    ErrorType, ExecutionMode, ExecutionStatus, HealthCheck, HealthStatus, OutputConfig,
    OutputDestination, OutputFormat, Pipeline, PipelineContext, PipelineError, PipelineHealth,
    PipelineResult, RetryConfig,
};
pub use storage::{Storage, StorageData, StorageLocation, StorageMetadata, StorageType};
