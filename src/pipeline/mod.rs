pub mod orchestrator;
pub mod unified_pipeline;
pub mod adapters;
pub mod modular_pipeline;

pub use orchestrator::PipelineOrchestrator;
pub use unified_pipeline::{UnifiedPipeline, PipelineContext, SourceType, RawData};
pub use adapters::{ApiPipelineAdapter, HtmlPipelineAdapter, StoragePipelineAdapter, PipelineFactory, BatchPipelineExecutor};
pub use modular_pipeline::{ModularPipeline, ModularPipelineContext, ModularPipelineResult, ModularRawData};