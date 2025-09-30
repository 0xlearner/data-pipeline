use std::sync::Arc;
use std::collections::HashMap;

use super::source_registry::{SourceRegistry, SourcePipelineConfig};
use super::source_transformer::SourceType;
use super::pipeline_builder::PipelineConfig;
use super::transformers::{HtmlTransformer, PandamartTransformer, JsonTransformer};
use super::implementations::{JsonFlattenerStage, FieldClassifierStage, RuleNormalizerStage};

/// Factory for creating a fully configured source registry
/// 
/// This factory sets up all the default transformers, stages, and pipeline configurations
/// needed for the modular processing system.
pub struct RegistryFactory;

impl RegistryFactory {
    /// Create a complete source registry with all default configurations
    pub fn create_default_registry() -> SourceRegistry {
        let mut registry = SourceRegistry::new();
        
        // Register shared processing stages
        Self::register_shared_stages(&mut registry);
        
        // Register source transformers
        Self::register_transformers(&mut registry);
        
        // Register pipeline configurations
        Self::register_pipeline_configs(&mut registry);
        
        registry
    }
    
    /// Register all shared processing stages
    fn register_shared_stages(registry: &mut SourceRegistry) {
        // JSON Flattener Stage
        let json_flattener = Arc::new(JsonFlattenerStage::new());
        registry.register_shared_stage("json_flattener".to_string(), json_flattener);
        
        // Field Classifier Stage
        let field_classifier = Arc::new(FieldClassifierStage::new());
        registry.register_shared_stage("field_classifier".to_string(), field_classifier);
        
        // Rule Normalizer Stage
        let rule_normalizer = Arc::new(RuleNormalizerStage::new());
        registry.register_shared_stage("rule_normalizer".to_string(), rule_normalizer);
    }
    
    /// Register all source transformers
    fn register_transformers(registry: &mut SourceRegistry) {
        // HTML Transformer
        let html_transformer = Box::new(HtmlTransformer::new());
        registry.register_transformer(html_transformer);
        
        // Pandamart Transformer
        let pandamart_transformer = Box::new(PandamartTransformer::new());
        registry.register_transformer(pandamart_transformer);
        
        // JSON Transformer (pass-through)
        let json_transformer = Box::new(JsonTransformer::new());
        registry.register_transformer(json_transformer);
    }
    
    /// Register all pipeline configurations
    fn register_pipeline_configs(registry: &mut SourceRegistry) {
        // Standard JSON API Pipeline
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
        
        // HTML Scraping Pipeline
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
        
        // Pandamart Pipeline
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
        
        // Naheed Pipeline (placeholder for future implementation)
        registry.register_pipeline_config(SourcePipelineConfig {
            source_type: SourceType::Naheed,
            stage_names: vec![
                "json_flattener".to_string(),
                "field_classifier".to_string(),
                "rule_normalizer".to_string(),
            ],
            pipeline_config: PipelineConfig {
                name: "naheed".to_string(),
                source_type: SourceType::Naheed,
                fail_fast: false,
                max_processing_time_ms: 0,
                custom_config: HashMap::new(),
            },
            requires_transformer: true,
        });
    }
    
    /// Create a custom registry with specific configurations
    pub fn create_custom_registry(
        transformers: Vec<Box<dyn super::SourceTransformer>>,
        stages: Vec<(String, Arc<dyn super::ProcessingStage>)>,
        configs: Vec<SourcePipelineConfig>,
    ) -> SourceRegistry {
        let mut registry = SourceRegistry::new();
        
        // Register custom transformers
        for transformer in transformers {
            registry.register_transformer(transformer);
        }
        
        // Register custom stages
        for (name, stage) in stages {
            registry.register_shared_stage(name, stage);
        }
        
        // Register custom pipeline configurations
        for config in configs {
            registry.register_pipeline_config(config);
        }
        
        registry
    }
    
    /// Create a minimal registry for testing
    pub fn create_test_registry() -> SourceRegistry {
        let mut registry = SourceRegistry::new();
        
        // Register minimal stages for testing
        let json_flattener = Arc::new(JsonFlattenerStage::new());
        registry.register_shared_stage("json_flattener".to_string(), json_flattener);
        
        // Register minimal transformer
        let json_transformer = Box::new(JsonTransformer::new());
        registry.register_transformer(json_transformer);
        
        // Register minimal pipeline config
        registry.register_pipeline_config(SourcePipelineConfig {
            source_type: SourceType::JsonApi,
            stage_names: vec!["json_flattener".to_string()],
            pipeline_config: PipelineConfig {
                name: "test_json".to_string(),
                source_type: SourceType::JsonApi,
                fail_fast: true,
                max_processing_time_ms: 5000,
                custom_config: HashMap::new(),
            },
            requires_transformer: false,
        });
        
        registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_registry_creation() {
        let registry = RegistryFactory::create_default_registry();
        
        // Check that all expected source types are registered
        assert!(registry.is_source_registered(&SourceType::JsonApi));
        assert!(registry.is_source_registered(&SourceType::HtmlScraping));
        assert!(registry.is_source_registered(&SourceType::Pandamart));
        assert!(registry.is_source_registered(&SourceType::Naheed));
        
        // Check that transformers are registered
        assert!(registry.get_transformer(&SourceType::HtmlScraping).is_some());
        assert!(registry.get_transformer(&SourceType::Pandamart).is_some());
        assert!(registry.get_transformer(&SourceType::JsonApi).is_some());
    }

    #[test]
    fn test_test_registry_creation() {
        let registry = RegistryFactory::create_test_registry();
        
        // Check minimal configuration
        assert!(registry.is_source_registered(&SourceType::JsonApi));
        assert!(!registry.is_source_registered(&SourceType::HtmlScraping));
        
        // Should be able to create a pipeline
        let pipeline_result = registry.create_pipeline(&SourceType::JsonApi);
        assert!(pipeline_result.is_ok());
    }

    #[test]
    fn test_custom_registry_creation() {
        use super::super::{SourceTransformer, ProcessingStage};

        let transformers = vec![Box::new(JsonTransformer::new()) as Box<dyn SourceTransformer>];
        let stages = vec![("test_stage".to_string(), Arc::new(JsonFlattenerStage::new()) as Arc<dyn ProcessingStage>)];
        let configs = vec![SourcePipelineConfig {
            source_type: SourceType::JsonApi,
            stage_names: vec!["test_stage".to_string()],
            pipeline_config: PipelineConfig {
                name: "test".to_string(),
                source_type: SourceType::JsonApi,
                fail_fast: false,
                max_processing_time_ms: 0,
                custom_config: std::collections::HashMap::new(),
            },
            requires_transformer: false,
        }];

        let registry = RegistryFactory::create_custom_registry(transformers, stages, configs);

        assert!(registry.is_source_registered(&SourceType::JsonApi));
        assert!(registry.get_transformer(&SourceType::JsonApi).is_some());
    }
}
