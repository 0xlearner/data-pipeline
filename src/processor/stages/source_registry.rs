use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;

use super::source_transformer::{SourceType, SourceTransformer};
use super::pipeline_builder::{PipelineBuilder, ProcessingPipeline, PipelineConfig};
use super::processing_stage::ProcessingStage;

/// Registry for managing source-specific processing pipelines
/// 
/// The registry automatically selects the appropriate pipeline and transformer
/// for each data source, making it easy to add new sources without changing
/// the main processing logic.
pub struct SourceRegistry {
    /// Registered transformers by source type
    transformers: HashMap<SourceType, Box<dyn SourceTransformer>>,
    /// Registered pipeline configurations by source type
    pipeline_configs: HashMap<SourceType, SourcePipelineConfig>,
    /// Shared processing stages that can be reused across pipelines
    shared_stages: HashMap<String, Arc<dyn ProcessingStage>>,
}

/// Configuration for a source-specific pipeline
#[derive(Debug, Clone)]
pub struct SourcePipelineConfig {
    /// Source type this configuration applies to
    pub source_type: SourceType,
    /// Names of stages to include in the pipeline (in order)
    pub stage_names: Vec<String>,
    /// Pipeline-specific configuration
    pub pipeline_config: PipelineConfig,
    /// Whether this source requires a transformer
    pub requires_transformer: bool,
}

/// Factory for creating source-specific pipelines
pub struct PipelineFactory {
    registry: Arc<SourceRegistry>,
}

impl SourceRegistry {
    /// Create a new source registry
    pub fn new() -> Self {
        Self {
            transformers: HashMap::new(),
            pipeline_configs: HashMap::new(),
            shared_stages: HashMap::new(),
        }
    }
    
    /// Register a source transformer
    pub fn register_transformer(&mut self, transformer: Box<dyn SourceTransformer>) {
        let source_type = transformer.source_type();
        self.transformers.insert(source_type, transformer);
    }
    
    /// Register a shared processing stage
    pub fn register_shared_stage(&mut self, name: String, stage: Arc<dyn ProcessingStage>) {
        self.shared_stages.insert(name, stage);
    }
    
    /// Register a pipeline configuration for a source type
    pub fn register_pipeline_config(&mut self, config: SourcePipelineConfig) {
        self.pipeline_configs.insert(config.source_type.clone(), config);
    }
    
    /// Get a transformer for a source type
    pub fn get_transformer(&self, source_type: &SourceType) -> Option<&dyn SourceTransformer> {
        self.transformers.get(source_type).map(|t| t.as_ref())
    }
    
    /// Get pipeline configuration for a source type
    pub fn get_pipeline_config(&self, source_type: &SourceType) -> Option<&SourcePipelineConfig> {
        self.pipeline_configs.get(source_type)
    }
    
    /// Create a pipeline for a specific source type
    pub fn create_pipeline(&self, source_type: &SourceType) -> Result<ProcessingPipeline> {
        let config = self.get_pipeline_config(source_type)
            .ok_or_else(|| anyhow!("No pipeline configuration found for source type: {:?}", source_type))?;
        
        let mut builder = PipelineBuilder::new(
            config.pipeline_config.name.clone(),
            source_type.clone(),
        )
        .with_config(config.pipeline_config.clone());
        
        // Add stages in the specified order
        for stage_name in &config.stage_names {
            let stage = self.shared_stages.get(stage_name)
                .ok_or_else(|| anyhow!("Stage '{}' not found in registry", stage_name))?;
            
            // Clone the Arc to create a new Box<dyn ProcessingStage>
            builder = builder.add_stage(Box::new(StageWrapper(stage.clone())));
        }
        
        // Add transformer if required
        if config.requires_transformer {
            if let Some(_transformer) = self.transformers.get(source_type) {
                // Create a cloned transformer for the pipeline
                // Note: This requires implementing Clone for transformers or using Arc
                return Err(anyhow!("Transformer cloning not yet implemented"));
            } else {
                return Err(anyhow!("Transformer required but not found for source type: {:?}", source_type));
            }
        }
        
        Ok(builder.build())
    }
    
    /// Get all registered source types
    pub fn get_registered_sources(&self) -> Vec<SourceType> {
        self.pipeline_configs.keys().cloned().collect()
    }
    
    /// Check if a source type is registered
    pub fn is_source_registered(&self, source_type: &SourceType) -> bool {
        self.pipeline_configs.contains_key(source_type)
    }
    
    /// Create a default registry with standard configurations
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        
        // Register default pipeline configurations
        registry.register_pipeline_config(SourcePipelineConfig {
            source_type: SourceType::JsonApi,
            stage_names: vec![
                "json_flattener".to_string(),
                "field_classifier".to_string(),
                "rule_normalizer".to_string(),
            ],
            pipeline_config: PipelineConfig {
                name: "standard_json".to_string(),
                source_type: SourceType::JsonApi,
                fail_fast: false,
                max_processing_time_ms: 0,
                custom_config: HashMap::new(),
            },
            requires_transformer: false,
        });
        
        registry.register_pipeline_config(SourcePipelineConfig {
            source_type: SourceType::HtmlScraping,
            stage_names: vec![
                "json_flattener".to_string(),
                "field_classifier".to_string(),
                "rule_normalizer".to_string(),
            ],
            pipeline_config: PipelineConfig {
                name: "html_scraping".to_string(),
                source_type: SourceType::HtmlScraping,
                fail_fast: false,
                max_processing_time_ms: 0,
                custom_config: HashMap::new(),
            },
            requires_transformer: true,
        });
        
        registry.register_pipeline_config(SourcePipelineConfig {
            source_type: SourceType::Pandamart,
            stage_names: vec![
                "json_flattener".to_string(),
                "field_classifier".to_string(),
                "rule_normalizer".to_string(),
            ],
            pipeline_config: PipelineConfig {
                name: "pandamart".to_string(),
                source_type: SourceType::Pandamart,
                fail_fast: false,
                max_processing_time_ms: 0,
                custom_config: HashMap::new(),
            },
            requires_transformer: true,
        });
        
        registry
    }
}

/// Wrapper to convert Arc<dyn ProcessingStage> to Box<dyn ProcessingStage>
struct StageWrapper(Arc<dyn ProcessingStage>);

impl ProcessingStage for StageWrapper {
    fn name(&self) -> &str {
        self.0.name()
    }
    
    fn metadata(&self) -> super::processing_stage::StageMetadata {
        self.0.metadata()
    }
    
    fn process(&self, input: super::processing_stage::ProcessingData) -> Result<super::processing_stage::StageResult> {
        self.0.process(input)
    }
    
    fn can_process(&self, input: &super::processing_stage::ProcessingData) -> bool {
        self.0.can_process(input)
    }
    
    fn output_type(&self, input_type: &super::processing_stage::ProcessingDataType) -> Result<super::processing_stage::ProcessingDataType> {
        self.0.output_type(input_type)
    }
    
    fn validate_config(&self) -> Result<()> {
        self.0.validate_config()
    }
}

impl PipelineFactory {
    /// Create a new pipeline factory
    pub fn new(registry: Arc<SourceRegistry>) -> Self {
        Self { registry }
    }
    
    /// Create a pipeline for a source type
    pub fn create_for_source(&self, source_type: &SourceType) -> Result<ProcessingPipeline> {
        self.registry.create_pipeline(source_type)
    }
    
    /// Get available source types
    pub fn available_sources(&self) -> Vec<SourceType> {
        self.registry.get_registered_sources()
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_registry_creation() {
        let registry = SourceRegistry::new();
        assert_eq!(registry.get_registered_sources().len(), 0);
    }

    #[test]
    fn test_default_registry() {
        let registry = SourceRegistry::with_defaults();
        let sources = registry.get_registered_sources();
        
        assert!(sources.contains(&SourceType::JsonApi));
        assert!(sources.contains(&SourceType::HtmlScraping));
        assert!(sources.contains(&SourceType::Pandamart));
        
        assert!(registry.is_source_registered(&SourceType::JsonApi));
        assert!(!registry.is_source_registered(&SourceType::Naheed));
    }

    #[test]
    fn test_pipeline_config() {
        let config = SourcePipelineConfig {
            source_type: SourceType::JsonApi,
            stage_names: vec!["stage1".to_string(), "stage2".to_string()],
            pipeline_config: PipelineConfig::default(),
            requires_transformer: false,
        };
        
        assert_eq!(config.source_type, SourceType::JsonApi);
        assert_eq!(config.stage_names.len(), 2);
        assert!(!config.requires_transformer);
    }
}
