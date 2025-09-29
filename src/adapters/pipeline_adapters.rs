use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::pipeline::unified_pipeline::{
    PipelineContext as UnifiedContext, RawData, UnifiedPipeline,
};
use crate::storage::MinioStorage;
use crate::traits::data_source::{DataSource, SourceType};
use crate::traits::pipeline::{
    ExecutionStatus, HealthCheck, HealthStatus, Pipeline, PipelineContext, PipelineHealth,
    PipelineMetadata, PipelineMetrics, PipelineResult, SourceResult,
};

/// Adapter that makes UnifiedPipeline compatible with Pipeline trait
pub struct UnifiedPipelineAdapter {
    pipeline: UnifiedPipeline,
    name: String,
    sources: HashMap<String, Box<dyn DataSource>>,
}

impl UnifiedPipelineAdapter {
    pub fn new(name: String, storage: Arc<MinioStorage>) -> Self {
        Self {
            pipeline: UnifiedPipeline::new(storage),
            name,
            sources: HashMap::new(),
        }
    }

    pub fn add_source(&mut self, name: String, source: Box<dyn DataSource>) {
        self.sources.insert(name, source);
    }

    pub fn remove_source(&mut self, name: &str) -> Option<Box<dyn DataSource>> {
        self.sources.remove(name)
    }

    pub fn list_sources(&self) -> Vec<&str> {
        self.sources.keys().map(|s| s.as_str()).collect()
    }
}

#[async_trait]
impl Pipeline for UnifiedPipelineAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, context: PipelineContext) -> Result<PipelineResult> {
        let start_time = chrono::Utc::now();
        let execution_start = Instant::now();

        let mut sources_processed = Vec::new();
        let mut total_records_processed = 0u64;
        let mut total_records_output = 0u64;
        let mut errors = Vec::new();
        let warnings = Vec::new();

        // Process sources based on filters
        let sources_to_process: Vec<_> = if context.source_filters.is_empty() {
            self.sources.keys().cloned().collect()
        } else {
            context.source_filters.clone()
        };

        for source_name in sources_to_process {
            if let Some(_source) = self.sources.get(&source_name) {
                let source_start = Instant::now();

                match self.execute_for_source(&source_name, context.clone()).await {
                    Ok(result) => {
                        // Extract metrics from the result
                        if let Some(source_result) = result.sources_processed.first() {
                            sources_processed.push(SourceResult {
                                source_name: source_name.clone(),
                                status: ExecutionStatus::Completed,
                                records_processed: source_result.records_processed,
                                records_output: source_result.records_output,
                                duration: source_start.elapsed(),
                                errors: Vec::new(),
                            });

                            total_records_processed += source_result.records_processed;
                            total_records_output += source_result.records_output;
                        }
                    }
                    Err(e) => {
                        sources_processed.push(SourceResult {
                            source_name: source_name.clone(),
                            status: ExecutionStatus::Failed,
                            records_processed: 0,
                            records_output: 0,
                            duration: source_start.elapsed(),
                            errors: vec![crate::traits::pipeline::PipelineError {
                                error_type: crate::traits::pipeline::ErrorType::SourceError,
                                message: e.to_string(),
                                source: Some(source_name.clone()),
                                processor: None,
                                timestamp: chrono::Utc::now(),
                                recoverable: true,
                            }],
                        });

                        errors.push(crate::traits::pipeline::PipelineError {
                            error_type: crate::traits::pipeline::ErrorType::SourceError,
                            message: e.to_string(),
                            source: Some(source_name),
                            processor: None,
                            timestamp: chrono::Utc::now(),
                            recoverable: true,
                        });
                    }
                }
            } else {
                errors.push(crate::traits::pipeline::PipelineError {
                    error_type: crate::traits::pipeline::ErrorType::ConfigurationError,
                    message: format!("Source '{}' not found", source_name),
                    source: Some(source_name),
                    processor: None,
                    timestamp: chrono::Utc::now(),
                    recoverable: false,
                });
            }
        }

        let duration = execution_start.elapsed();
        let status = if errors.is_empty() {
            ExecutionStatus::Completed
        } else if sources_processed
            .iter()
            .any(|s| s.status == ExecutionStatus::Completed)
        {
            ExecutionStatus::PartiallyCompleted
        } else {
            ExecutionStatus::Failed
        };

        // Calculate error rate before moving values
        let error_rate = errors.len() as f64 / sources_processed.len().max(1) as f64;

        Ok(PipelineResult {
            execution_id: context.execution_id,
            pipeline_name: self.name.clone(),
            status,
            start_time,
            end_time: Some(chrono::Utc::now()),
            duration: Some(duration),
            sources_processed,
            total_records_processed,
            total_records_output,
            errors,
            warnings,
            metrics: PipelineMetrics {
                throughput_records_per_second: if duration.as_secs() > 0 {
                    total_records_processed as f64 / duration.as_secs() as f64
                } else {
                    0.0
                },
                memory_usage_mb: 0.0,   // TODO: Implement memory tracking
                cpu_usage_percent: 0.0, // TODO: Implement CPU tracking
                network_io_mb: 0.0,
                disk_io_mb: 0.0,
                cache_hit_rate: 0.0,
                error_rate,
            },
            output_locations: Vec::new(), // TODO: Track output locations
        })
    }

    async fn execute_for_source(
        &self,
        source_name: &str,
        context: PipelineContext,
    ) -> Result<PipelineResult> {
        if let Some(source) = self.sources.get(source_name) {
            // Fetch data from source
            let raw_data = source.fetch_all().await?;

            // Convert to unified pipeline format
            let unified_data = match raw_data {
                crate::traits::data_source::RawSourceData::Json(data) => RawData::Json(data),
                crate::traits::data_source::RawSourceData::Html(data) => RawData::Html(data),
                crate::traits::data_source::RawSourceData::Binary(_) => {
                    return Err(anyhow::anyhow!("Binary data not supported yet"));
                }
            };

            // Create unified pipeline context
            let unified_context = match source.source_type() {
                SourceType::Api => UnifiedContext::for_api(source_name.to_string()),
                SourceType::Html => UnifiedContext::for_html(source_name.to_string()),
                SourceType::Storage => UnifiedContext::for_storage(source_name.to_string()),
                _ => UnifiedContext::for_api(source_name.to_string()), // Default
            };

            // Execute unified pipeline
            let result = self.pipeline.execute(unified_context, unified_data).await?;

            // Convert result to trait format
            Ok(PipelineResult {
                execution_id: context.execution_id,
                pipeline_name: self.name.clone(),
                status: ExecutionStatus::Completed,
                start_time: chrono::Utc::now()
                    - chrono::Duration::from_std(result.duration).unwrap_or_default(),
                end_time: Some(chrono::Utc::now()),
                duration: Some(result.duration),
                sources_processed: vec![SourceResult {
                    source_name: source_name.to_string(),
                    status: ExecutionStatus::Completed,
                    records_processed: result.total_items as u64,
                    records_output: result.processed_items as u64,
                    duration: result.duration,
                    errors: Vec::new(),
                }],
                total_records_processed: result.total_items as u64,
                total_records_output: result.processed_items as u64,
                errors: Vec::new(),
                warnings: Vec::new(),
                metrics: PipelineMetrics {
                    throughput_records_per_second: if result.duration.as_secs() > 0 {
                        result.total_items as f64 / result.duration.as_secs() as f64
                    } else {
                        0.0
                    },
                    memory_usage_mb: 0.0,
                    cpu_usage_percent: 0.0,
                    network_io_mb: 0.0,
                    disk_io_mb: 0.0,
                    cache_hit_rate: 0.0,
                    error_rate: 0.0,
                },
                output_locations: result.processed_storage_key.into_iter().collect(),
            })
        } else {
            Err(anyhow::anyhow!("Source '{}' not found", source_name))
        }
    }

    fn metadata(&self) -> PipelineMetadata {
        PipelineMetadata {
            name: self.name.clone(),
            description: Some(
                "Unified data processing pipeline with trait-based abstractions".to_string(),
            ),
            version: "1.0.0".to_string(),
            author: Some("Data Pipeline Team".to_string()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: vec!["unified".to_string(), "trait-based".to_string()],
            supported_sources: self.sources.keys().cloned().collect(),
            supported_processors: vec![
                "json_flattener".to_string(),
                "field_classifier".to_string(),
                "rule_normalizer".to_string(),
            ],
            dependencies: vec!["polars".to_string(), "serde_json".to_string()],
        }
    }

    fn validate(&self) -> Result<()> {
        if self.sources.is_empty() {
            return Err(anyhow::anyhow!("Pipeline has no configured sources"));
        }

        // Validate each source
        for (name, source) in &self.sources {
            // Basic validation - could be extended
            if source.name().is_empty() {
                return Err(anyhow::anyhow!("Source '{}' has empty name", name));
            }
        }

        Ok(())
    }

    async fn health_check(&self) -> Result<PipelineHealth> {
        let start_time = Instant::now();
        let mut checks = Vec::new();
        let mut is_healthy = true;

        // Check each source
        for (name, source) in &self.sources {
            let check_start = Instant::now();
            match source.health_check().await {
                Ok(health) => {
                    let status = if health.is_healthy {
                        HealthStatus::Healthy
                    } else {
                        HealthStatus::Unhealthy
                    };

                    if !health.is_healthy {
                        is_healthy = false;
                    }

                    checks.push(HealthCheck {
                        name: format!("source_{}", name),
                        status,
                        message: health.error_message,
                        duration: check_start.elapsed(),
                    });
                }
                Err(e) => {
                    is_healthy = false;
                    checks.push(HealthCheck {
                        name: format!("source_{}", name),
                        status: HealthStatus::Unhealthy,
                        message: Some(e.to_string()),
                        duration: check_start.elapsed(),
                    });
                }
            }
        }

        let overall_status = if is_healthy {
            HealthStatus::Healthy
        } else if checks.iter().any(|c| c.status == HealthStatus::Healthy) {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        Ok(PipelineHealth {
            is_healthy,
            status: overall_status,
            checks,
            last_execution: None, // TODO: Track last execution
            uptime: start_time.elapsed(),
        })
    }
}

/// Factory for creating pipeline adapters
pub struct PipelineAdapterFactory;

impl PipelineAdapterFactory {
    pub fn create_unified_pipeline(
        name: String,
        storage: Arc<MinioStorage>,
    ) -> UnifiedPipelineAdapter {
        UnifiedPipelineAdapter::new(name, storage)
    }

    pub fn create_pipeline_with_sources(
        name: String,
        storage: Arc<MinioStorage>,
        sources: Vec<(String, Box<dyn DataSource>)>,
    ) -> UnifiedPipelineAdapter {
        let mut pipeline = Self::create_unified_pipeline(name, storage);

        for (source_name, source) in sources {
            pipeline.add_source(source_name, source);
        }

        pipeline
    }
}
