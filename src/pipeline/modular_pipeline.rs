use anyhow::{Context, Result};
use polars::prelude::*;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use crate::extractor::ScrapedProduct;
use crate::processor::stages::{
    RegistryFactory, PipelineFactory, SourceType, RawSourceData, ProcessingData
};
use crate::storage::MinioStorage;

/// Modular processing pipeline that uses the new stage-based architecture
/// 
/// This pipeline automatically selects the appropriate processing stages and transformers
/// based on the source type, making it easy to add new data sources without changing
/// the main pipeline logic.
pub struct ModularPipeline {
    storage: Arc<MinioStorage>,
    factory: PipelineFactory,
}

/// Context for modular pipeline execution
#[derive(Debug, Clone)]
pub struct ModularPipelineContext {
    pub source_name: String,
    pub source_type: SourceType,
    pub skip_storage: bool,
    pub custom_config: std::collections::HashMap<String, String>,
}

/// Result of modular pipeline execution
#[derive(Debug)]
pub struct ModularPipelineResult {
    pub source_name: String,
    pub source_type: SourceType,
    pub total_items: usize,
    pub processed_items: usize,
    pub duration: std::time::Duration,
    pub raw_storage_key: Option<String>,
    pub processed_storage_key: Option<String>,
    pub stage_results: Vec<String>,
    pub warnings: Vec<String>,
    pub success: bool,
}

impl ModularPipeline {
    /// Create a new modular pipeline instance
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        Self {
            storage,
            factory,
        }
    }
    
    /// Create a modular pipeline with custom registry
    pub fn with_custom_registry(storage: Arc<MinioStorage>, factory: PipelineFactory) -> Self {
        Self {
            storage,
            factory,
        }
    }
    
    /// Execute the modular pipeline for any data source
    pub async fn execute(
        &self,
        context: ModularPipelineContext,
        raw_data: ModularRawData,
    ) -> Result<ModularPipelineResult> {
        let start_time = Instant::now();
        info!(
            "🚀 Starting modular pipeline for {} ({})",
            context.source_name,
            context.source_type.as_str()
        );
        
        // Convert to RawSourceData
        let source_data = match raw_data {
            ModularRawData::Json(data) => {
                let total_items = data.len();
                info!("📄 Processing {} JSON items", total_items);
                RawSourceData::Json(data)
            }
            ModularRawData::Html(products) => {
                let total_items = products.len();
                info!("🔄 Processing {} HTML products", total_items);
                RawSourceData::Html(products)
            }
        };
        
        if source_data.is_empty() {
            warn!("No data to process for {}", context.source_name);
            return Ok(ModularPipelineResult {
                source_name: context.source_name,
                source_type: context.source_type,
                total_items: 0,
                processed_items: 0,
                duration: start_time.elapsed(),
                raw_storage_key: None,
                processed_storage_key: None,
                stage_results: vec!["No data to process".to_string()],
                warnings: vec!["No data to process".to_string()],
                success: false,
            });
        }
        
        let total_items = source_data.size();
        
        // Store raw data (optional)
        let raw_storage_key = if !context.skip_storage {
            Some(self.store_raw_data(&context.source_name, &source_data).await?)
        } else {
            None
        };
        
        // Create and execute pipeline
        let pipeline = self.factory.create_for_source(&context.source_type)
            .context("Failed to create pipeline for source type")?;
        
        info!("🔧 Created pipeline with {} stages", pipeline.stage_count());
        info!("📋 Pipeline stages: {:?}", pipeline.stage_names());
        
        let pipeline_result = pipeline.execute_with_raw_data(source_data)
            .context("Pipeline execution failed")?;
        
        let processed_items = pipeline_result.metrics.total_items_processed;
        let stage_results: Vec<String> = pipeline_result.stage_results
            .iter()
            .map(|r| format!("{}: {}", r.stage_name, if r.success { "✅" } else { "❌" }))
            .collect();
        
        info!("📊 Pipeline completed: {} items processed", processed_items);
        info!("⏱️  Processing time: {}ms", pipeline_result.metrics.total_time_ms);
        
        // Store processed data
        let processed_storage_key = if !context.skip_storage && pipeline_result.success {
            match pipeline_result.data {
                ProcessingData::DataFrame(df) => {
                    Some(self.store_processed_data(&context.source_name, &df).await?)
                }
                _ => {
                    warn!("Pipeline did not produce DataFrame output, skipping storage");
                    None
                }
            }
        } else {
            None
        };
        
        let warnings: Vec<String> = pipeline_result.stage_results
            .iter()
            .flat_map(|r| r.warnings.clone())
            .chain(pipeline_result.errors.clone())
            .collect();
        
        Ok(ModularPipelineResult {
            source_name: context.source_name,
            source_type: context.source_type,
            total_items,
            processed_items,
            duration: start_time.elapsed(),
            raw_storage_key,
            processed_storage_key,
            stage_results,
            warnings,
            success: pipeline_result.success,
        })
    }
    
    /// Store raw data to MinIO
    async fn store_raw_data(&self, source_name: &str, raw_data: &RawSourceData) -> Result<String> {
        match raw_data {
            RawSourceData::Json(data) => {
                let json_string = serde_json::to_string_pretty(data)?;
                self.storage.store_raw_json(source_name, &json_string).await
            }
            RawSourceData::Html(products) => {
                // Store both HTML files and JSON summary
                let mut html_storage_keys = Vec::new();

                // Group products by category_path and page_name for HTML storage
                let mut html_groups: std::collections::HashMap<(String, String, Option<u32>), Vec<&ScrapedProduct>> = std::collections::HashMap::new();

                for product in products {
                    if let (Some(category_path), Some(page_name)) = (&product.category_path, &product.page_name) {
                        let key = (category_path.clone(), page_name.clone(), product.page_number);
                        html_groups.entry(key).or_insert_with(Vec::new).push(product);
                    }
                }

                // Store HTML files for each group
                for ((category_path, page_name, page_number), group_products) in html_groups {
                    // Combine all HTML content for this page
                    let combined_html = group_products.iter()
                        .map(|p| p.raw_html.as_str())
                        .collect::<Vec<_>>()
                        .join("\n<!-- PRODUCT_SEPARATOR -->\n");

                    let html_key = self.storage.store_raw_html(
                        source_name,
                        &category_path,
                        &page_name,
                        page_number,
                        &combined_html,
                    ).await?;

                    html_storage_keys.push(html_key);
                }

                // Also store JSON summary for processing pipeline
                let json_data: Vec<serde_json::Value> = products.iter().map(|p| {
                    serde_json::json!({
                        "name": p.name,
                        "price": p.price,
                        "product_id": p.product_id,
                        "category": p.category,
                        "url": p.url,
                        "raw_html": p.raw_html,
                        "category_path": p.category_path,
                        "page_name": p.page_name,
                        "page_number": p.page_number,
                        "html_storage_keys": html_storage_keys
                    })
                }).collect();
                let json_string = serde_json::to_string_pretty(&json_data)?;
                self.storage.store_raw_json(source_name, &json_string).await
            }
            _ => Err(anyhow::anyhow!("Unsupported raw data type for storage")),
        }
    }
    
    /// Store processed DataFrame to MinIO
    async fn store_processed_data(&self, source_name: &str, df: &DataFrame) -> Result<String> {
        // Convert DataFrame to Parquet
        let mut buf = Vec::new();
        {
            let writer = polars::prelude::ParquetWriter::new(&mut buf);
            writer.finish(&mut df.clone())?;
        }
        
        self.storage.store_parquet(source_name, &buf).await
    }
    
    /// Get available source types
    pub fn available_sources(&self) -> Vec<SourceType> {
        self.factory.available_sources()
    }
    
    /// Check if a source type is supported
    pub fn supports_source(&self, source_type: &SourceType) -> bool {
        self.available_sources().contains(source_type)
    }
}

/// Raw data input for the modular pipeline
#[derive(Debug)]
pub enum ModularRawData {
    Json(Vec<Value>),
    Html(Vec<ScrapedProduct>),
}

impl ModularPipelineContext {
    /// Create a new pipeline context
    pub fn new(source_name: String, source_type: SourceType) -> Self {
        Self {
            source_name,
            source_type,
            skip_storage: false,
            custom_config: std::collections::HashMap::new(),
        }
    }
    
    /// Skip storage operations
    pub fn skip_storage(mut self) -> Self {
        self.skip_storage = true;
        self
    }
    
    /// Add custom configuration
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.custom_config.insert(key, value);
        self
    }
}

impl ModularPipelineResult {
    /// Check if the pipeline execution was successful
    pub fn is_success(&self) -> bool {
        self.success && self.processed_items > 0
    }
    
    /// Get processing rate (items per second)
    pub fn processing_rate(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.processed_items as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }
    
    /// Get success rate (processed / total)
    pub fn success_rate(&self) -> f64 {
        if self.total_items > 0 {
            self.processed_items as f64 / self.total_items as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_modular_pipeline_json() {
        // This test would require a MinIO instance, so we'll skip the actual execution
        // but test the pipeline creation
        let storage = Arc::new(MinioStorage::new("test", "test", "test", "test").unwrap());
        let pipeline = ModularPipeline::new(storage);
        
        assert!(pipeline.supports_source(&SourceType::JsonApi));
        assert!(pipeline.supports_source(&SourceType::HtmlScraping));
        assert!(pipeline.supports_source(&SourceType::Pandamart));
    }

    #[test]
    fn test_modular_pipeline_context() {
        let context = ModularPipelineContext::new(
            "test_source".to_string(),
            SourceType::JsonApi,
        )
        .skip_storage()
        .with_config("test_key".to_string(), "test_value".to_string());
        
        assert_eq!(context.source_name, "test_source");
        assert_eq!(context.source_type, SourceType::JsonApi);
        assert!(context.skip_storage);
        assert_eq!(context.custom_config.get("test_key"), Some(&"test_value".to_string()));
    }
}
