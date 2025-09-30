use anyhow::{Result, anyhow};
use std::collections::HashMap;

use super::processing_stage::{ProcessingStage, ProcessingData, StageMetrics};
use super::source_transformer::{SourceType, SourceTransformer, RawSourceData};

/// Builder for creating processing pipelines
/// 
/// Allows composing different processing stages into a pipeline that can
/// handle specific source types. Pipelines can be built programmatically
/// or from configuration.
pub struct PipelineBuilder {
    stages: Vec<Box<dyn ProcessingStage>>,
    config: PipelineConfig,
}

/// A complete processing pipeline
pub struct ProcessingPipeline {
    stages: Vec<Box<dyn ProcessingStage>>,
    config: PipelineConfig,
    transformer: Option<Box<dyn SourceTransformer>>,
}

/// Configuration for a processing pipeline
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Pipeline name/identifier
    pub name: String,
    /// Source type this pipeline handles
    pub source_type: SourceType,
    /// Whether to stop on first error or continue
    pub fail_fast: bool,
    /// Maximum processing time in milliseconds (0 = no limit)
    pub max_processing_time_ms: u64,
    /// Pipeline-specific configuration
    pub custom_config: HashMap<String, String>,
}

/// Result of pipeline execution
#[derive(Debug)]
pub struct PipelineResult {
    /// Final processed data
    pub data: ProcessingData,
    /// Combined metrics from all stages
    pub metrics: PipelineMetrics,
    /// Results from each stage
    pub stage_results: Vec<StageExecutionResult>,
    /// Overall success status
    pub success: bool,
    /// Any errors that occurred
    pub errors: Vec<String>,
}

/// Metrics for the entire pipeline
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    /// Total processing time
    pub total_time_ms: u64,
    /// Number of stages executed
    pub stages_executed: usize,
    /// Total items processed across all stages
    pub total_items_processed: usize,
    /// Total items failed across all stages
    pub total_items_failed: usize,
    /// Memory usage peak
    pub peak_memory_mb: Option<f64>,
}

/// Result of executing a single stage in the pipeline
#[derive(Debug)]
pub struct StageExecutionResult {
    /// Stage name
    pub stage_name: String,
    /// Whether the stage succeeded
    pub success: bool,
    /// Stage metrics
    pub metrics: StageMetrics,
    /// Warnings from the stage
    pub warnings: Vec<String>,
    /// Error message if stage failed
    pub error: Option<String>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new(name: String, source_type: SourceType) -> Self {
        Self {
            stages: Vec::new(),
            config: PipelineConfig {
                name,
                source_type,
                fail_fast: true,
                max_processing_time_ms: 0,
                custom_config: HashMap::new(),
            },
        }
    }
    
    /// Add a processing stage to the pipeline
    pub fn add_stage(mut self, stage: Box<dyn ProcessingStage>) -> Self {
        self.stages.push(stage);
        self
    }
    
    /// Set pipeline configuration
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }
    
    /// Set fail-fast behavior
    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.config.fail_fast = fail_fast;
        self
    }
    
    /// Set maximum processing time
    pub fn max_time(mut self, max_time_ms: u64) -> Self {
        self.config.max_processing_time_ms = max_time_ms;
        self
    }
    
    /// Add custom configuration
    pub fn with_custom_config(mut self, key: String, value: String) -> Self {
        self.config.custom_config.insert(key, value);
        self
    }
    
    /// Build the pipeline
    pub fn build(self) -> ProcessingPipeline {
        ProcessingPipeline {
            stages: self.stages,
            config: self.config,
            transformer: None,
        }
    }
    
    /// Build the pipeline with a source transformer
    pub fn build_with_transformer(self, transformer: Box<dyn SourceTransformer>) -> ProcessingPipeline {
        ProcessingPipeline {
            stages: self.stages,
            config: self.config,
            transformer: Some(transformer),
        }
    }
    
    /// Create a standard JSON processing pipeline
    pub fn standard_json_pipeline() -> Self {
        Self::new("standard_json".to_string(), SourceType::JsonApi)
            .fail_fast(false)
    }
    
    /// Create a pipeline for HTML scraping
    pub fn html_scraping_pipeline() -> Self {
        Self::new("html_scraping".to_string(), SourceType::HtmlScraping)
            .fail_fast(false)
    }
    
    /// Create a pipeline for Pandamart data
    pub fn pandamart_pipeline() -> Self {
        Self::new("pandamart".to_string(), SourceType::Pandamart)
            .fail_fast(false)
    }
}

impl ProcessingPipeline {
    /// Execute the pipeline with raw source data
    pub fn execute_with_raw_data(&self, raw_data: RawSourceData) -> Result<PipelineResult> {
        // First, transform raw data if transformer is available
        let initial_data = if let Some(transformer) = &self.transformer {
            if !transformer.can_transform(&raw_data) {
                return Err(anyhow!("Transformer cannot handle the provided raw data"));
            }
            
            let transform_result = transformer.transform(raw_data)?;
            ProcessingData::Json(transform_result.data)
        } else {
            // Convert raw data directly to ProcessingData
            match raw_data {
                RawSourceData::Json(data) => ProcessingData::Json(data),
                _ => return Err(anyhow!("No transformer available for non-JSON raw data")),
            }
        };
        
        self.execute(initial_data)
    }
    
    /// Execute the pipeline with already processed data
    pub fn execute(&self, initial_data: ProcessingData) -> Result<PipelineResult> {
        let start_time = std::time::Instant::now();
        let mut stage_results = Vec::new();
        let mut total_items_processed = 0;
        let mut total_items_failed = 0;
        let mut errors = Vec::new();
        let mut current_data = Some(initial_data);

        for (_index, stage) in self.stages.iter().enumerate() {
            let data = match current_data.take() {
                Some(data) => data,
                None => {
                    // No data to process (previous stage failed)
                    break;
                }
            };

            // Check if we can process this data type
            if !stage.can_process(&data) {
                let error = format!(
                    "Stage '{}' cannot process data type '{:?}'",
                    stage.name(),
                    data.data_type()
                );

                stage_results.push(StageExecutionResult {
                    stage_name: stage.name().to_string(),
                    success: false,
                    metrics: StageMetrics {
                        processing_time_ms: 0,
                        items_processed: 0,
                        items_failed: 0,
                        memory_usage_mb: None,
                    },
                    warnings: Vec::new(),
                    error: Some(error.clone()),
                });

                if self.config.fail_fast {
                    errors.push(error);
                    current_data = Some(data); // Keep the data for the result
                    break;
                } else {
                    errors.push(error);
                    current_data = Some(data); // Keep the data for next stage
                    continue;
                }
            }

            // Execute the stage
            match stage.process(data) {
                Ok(result) => {
                    total_items_processed += result.metrics.items_processed;
                    total_items_failed += result.metrics.items_failed;

                    stage_results.push(StageExecutionResult {
                        stage_name: stage.name().to_string(),
                        success: true,
                        metrics: result.metrics,
                        warnings: result.warnings,
                        error: None,
                    });

                    current_data = Some(result.data);
                }
                Err(e) => {
                    let error = format!("Stage '{}' failed: {}", stage.name(), e);

                    stage_results.push(StageExecutionResult {
                        stage_name: stage.name().to_string(),
                        success: false,
                        metrics: StageMetrics {
                            processing_time_ms: 0,
                            items_processed: 0,
                            items_failed: 0,
                            memory_usage_mb: None,
                        },
                        warnings: Vec::new(),
                        error: Some(error.clone()),
                    });

                    errors.push(error);
                    // current_data is None, so we'll break on next iteration
                }
            }
        }

        let total_time = start_time.elapsed().as_millis() as u64;
        let success = errors.is_empty();

        // Use the last valid data or create empty data if none exists
        let final_data = current_data.unwrap_or(ProcessingData::Json(vec![]));

        Ok(PipelineResult {
            data: final_data,
            metrics: PipelineMetrics {
                total_time_ms: total_time,
                stages_executed: stage_results.len(),
                total_items_processed,
                total_items_failed,
                peak_memory_mb: None, // TODO: Implement memory tracking
            },
            stage_results,
            success,
            errors,
        })
    }
    
    /// Get pipeline configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }
    
    /// Get the number of stages in the pipeline
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }
    
    /// Get stage names in order
    pub fn stage_names(&self) -> Vec<String> {
        self.stages.iter().map(|s| s.name().to_string()).collect()
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            source_type: SourceType::JsonApi,
            fail_fast: true,
            max_processing_time_ms: 0,
            custom_config: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_builder() {
        let pipeline = PipelineBuilder::new("test".to_string(), SourceType::JsonApi)
            .fail_fast(false)
            .max_time(5000)
            .with_custom_config("test_key".to_string(), "test_value".to_string())
            .build();
        
        assert_eq!(pipeline.config.name, "test");
        assert_eq!(pipeline.config.source_type, SourceType::JsonApi);
        assert!(!pipeline.config.fail_fast);
        assert_eq!(pipeline.config.max_processing_time_ms, 5000);
        assert_eq!(pipeline.config.custom_config.get("test_key"), Some(&"test_value".to_string()));
    }
}
