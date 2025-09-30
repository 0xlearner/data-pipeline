pub mod processing_stage;
pub mod source_transformer;
pub mod pipeline_builder;
pub mod source_registry;
pub mod transformers;
pub mod implementations;
pub mod registry_factory;
pub mod examples;

#[cfg(test)]
pub mod integration_tests;

// Re-export main traits and types
pub use processing_stage::{ProcessingStage, ProcessingData, StageMetadata, StageResult, ProcessingDataType, StageType};
pub use source_transformer::{SourceTransformer, SourceType, TransformationResult, RawSourceData, TransformerConfig};
pub use pipeline_builder::{PipelineBuilder, ProcessingPipeline, PipelineConfig, PipelineResult};
pub use source_registry::{SourceRegistry, SourcePipelineConfig, PipelineFactory};
pub use transformers::{HtmlTransformer, PandamartTransformer, JsonTransformer};
pub use implementations::{JsonFlattenerStage, FieldClassifierStage, RuleNormalizerStage};
pub use registry_factory::RegistryFactory;
pub use examples::ProcessingExamples;
