use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};

use crate::traits::{
    ExecutionStatus, PipelineResult as TraitPipelineResult, RawSourceData,
    pipeline::PipelineMetrics,
};

use crate::adapters::{
    ApiSourceAdapter, HtmlSourceAdapter, MinioStorageAdapter, MinioStorageConfig,
    UnifiedPipelineAdapter,
};
use crate::cli::App;
use crate::config::{ApiConfig, HtmlConfig, MinioConfig};
use crate::sources::SourceType;
use crate::storage::MinioStorage;
use crate::traits::pipeline::OutputFormat;
use crate::traits::{
    DataSource, ExecutionMode, OutputConfig, OutputDestination, Pipeline, PipelineContext,
};

/// Main pipeline orchestrator that coordinates the entire data processing workflow
/// Now uses trait-based abstractions for better modularity and testability
pub struct PipelineOrchestrator {
    pipeline: UnifiedPipelineAdapter,
    sources: HashMap<String, SourceDefinition>,
    storage: Arc<dyn crate::traits::Storage>,
    minio_storage: Arc<MinioStorage>,
}

/// Source definition for the pipeline
#[derive(Debug, Clone)]
pub struct SourceDefinition {
    pub name: String,
    pub config_path: String,
    pub source_type: SourceType,
}

/// Pipeline execution options
#[derive(Debug, Clone)]
pub struct PipelineOptions {
    pub from_storage: bool,
    pub specific_source: Option<String>,
    pub batch_size: Option<usize>,
    pub memory_efficient: bool,
}

/// Pipeline execution result
#[derive(Debug)]
pub struct PipelineResult {
    pub total_products: usize,
    pub successful_sources: usize,
    pub failed_sources: usize,
    pub source_results: Vec<SourceResult>,
}

/// Result for individual source processing
#[derive(Debug)]
pub struct SourceResult {
    pub source_name: String,
    pub success: bool,
    pub products_count: usize,
    pub error_message: Option<String>,
    pub processing_time_ms: u64,
}

impl PipelineOrchestrator {
    /// Create a new pipeline orchestrator using trait-based abstractions
    pub async fn new() -> Result<Self> {
        // Load MinIO configuration
        let minio_config = MinioConfig::from_file("config/sources/minio.toml")
            .context("Failed to load MinIO configuration")?;

        info!(
            "Loaded MinIO configuration: {}@{}",
            minio_config.endpoint, minio_config.bucket_name
        );

        // Initialize storage using trait abstraction
        let storage_config = MinioStorageConfig {
            endpoint: minio_config.endpoint.clone(),
            access_key: minio_config
                .access_key
                .clone()
                .unwrap_or_else(|| "minioadmin".to_string()),
            secret_key: minio_config
                .secret_key
                .clone()
                .unwrap_or_else(|| "minioadmin".to_string()),
            bucket_name: minio_config.bucket_name.clone(),
            region: minio_config.region.clone(),
            use_ssl: minio_config.ssl.unwrap_or(false),
        };

        let storage_adapter = MinioStorageAdapter::from_config_async(storage_config).await?;
        let storage: Arc<dyn crate::traits::Storage> = Arc::new(storage_adapter);

        // Ensure storage is ready
        storage
            .ensure_ready()
            .await
            .context("Failed to ensure storage is ready")?;

        // Initialize MinIO storage for the pipeline (legacy compatibility)
        let minio_storage = Arc::new(
            MinioStorage::from_config(&minio_config)
                .context("Failed to initialize MinIO storage")?,
        );

        // Create unified pipeline using trait-based architecture
        let mut pipeline = UnifiedPipelineAdapter::new(
            "main_orchestrator_pipeline".to_string(),
            minio_storage.clone(),
        );

        // Define available sources with their configurations
        let mut sources = HashMap::new();

        let source_definitions = vec![
            (
                "kravemart",
                "config/sources/krave_mart.toml",
                SourceType::Json,
            ),
            (
                "bazaarapp",
                "config/sources/bazaar_app.toml",
                SourceType::Json,
            ),
            ("dealcart", "config/sources/dealcart.toml", SourceType::Json),
            (
                "pandamart",
                "config/sources/pandamart.toml",
                SourceType::Json,
            ),
            ("naheed", "config/sources/naheed.toml", SourceType::Html),
        ];

        // Initialize and register data sources
        for (name, config_path, source_type) in source_definitions {
            let source_def = SourceDefinition {
                name: name.to_string(),
                config_path: config_path.to_string(),
                source_type: source_type.clone(),
            };

            // Try to create and register the source
            if Path::new(config_path).exists() {
                match Self::create_data_source(&source_def).await {
                    Ok(data_source) => {
                        pipeline.add_source(name.to_string(), data_source);
                        info!("Registered data source: {} ({})", name, config_path);
                    }
                    Err(e) => {
                        warn!("Failed to create data source {}: {}", name, e);
                    }
                }
            } else {
                warn!("Config file not found for {}: {}", name, config_path);
            }

            sources.insert(name.to_string(), source_def);
        }

        info!("Initialized pipeline with {} sources", sources.len());

        Ok(Self {
            pipeline,
            sources,
            storage,
            minio_storage,
        })
    }

    /// Create a data source from source definition using trait abstractions
    async fn create_data_source(source_def: &SourceDefinition) -> Result<Box<dyn DataSource>> {
        match source_def.source_type {
            SourceType::Json => {
                let api_config =
                    ApiConfig::from_file(&source_def.config_path).with_context(|| {
                        format!("Failed to load API config from {}", source_def.config_path)
                    })?;
                // Create without storage for now - will be enhanced at runtime for two-stage processing
                let adapter = ApiSourceAdapter::new(api_config).await?;
                Ok(Box::new(adapter))
            }
            SourceType::Html => {
                let html_config =
                    HtmlConfig::from_file(&source_def.config_path).with_context(|| {
                        format!("Failed to load HTML config from {}", source_def.config_path)
                    })?;
                // Create without storage for now - will be enhanced at runtime
                let adapter = HtmlSourceAdapter::new(html_config).await?;
                Ok(Box::new(adapter))
            }
        }
    }

    /// Create a storage-enabled HTML source for two-stage processing
    async fn create_html_source_with_storage(
        &self,
        source_def: &SourceDefinition,
    ) -> Result<Box<dyn DataSource>> {
        let html_config = HtmlConfig::from_file(&source_def.config_path).with_context(|| {
            format!("Failed to load HTML config from {}", source_def.config_path)
        })?;

        let adapter =
            HtmlSourceAdapter::new_with_storage(html_config, self.minio_storage.clone()).await?;
        Ok(Box::new(adapter))
    }

    /// Process HTML source using two-stage approach (fetch → store → scrape)
    async fn process_html_source_two_stage(
        &self,
        source_def: &SourceDefinition,
        context: &PipelineContext,
    ) -> Result<TraitPipelineResult> {
        info!(
            "🔄 Starting two-stage HTML processing for: {}",
            source_def.name
        );

        // Create storage-enabled HTML source
        let html_source = self.create_html_source_with_storage(source_def).await?;

        // Fetch data using the storage-enabled source (this will do fetch → store → scrape)
        let raw_source_data = html_source.fetch_all().await?;

        // Convert RawSourceData to RawData
        let raw_data = match raw_source_data {
            RawSourceData::Html(products) => {
                crate::pipeline::unified_pipeline::RawData::Html(products)
            }
            _ => return Err(anyhow::anyhow!("Expected HTML data from HTML source")),
        };

        // Process the data through the pipeline stages
        let temp_pipeline =
            crate::pipeline::unified_pipeline::UnifiedPipeline::new(self.minio_storage.clone());

        let unified_context = crate::pipeline::unified_pipeline::PipelineContext {
            source_name: source_def.name.clone(),
            source_type: crate::pipeline::unified_pipeline::SourceType::Html,
            batch_size: Some(1000), // Default batch size
            skip_storage: false,
            validate_data: true,
        };

        let result = temp_pipeline.execute(unified_context, raw_data).await?;

        // Convert unified pipeline result to trait pipeline result
        let now = chrono::Utc::now();
        Ok(TraitPipelineResult {
            execution_id: context.execution_id.clone(),
            pipeline_name: "two-stage-html".to_string(),
            status: ExecutionStatus::Completed,
            start_time: now,
            end_time: Some(now),
            duration: Some(std::time::Duration::from_secs(0)),
            sources_processed: vec![],
            total_records_processed: result.processed_items as u64,
            total_records_output: result.processed_items as u64,
            errors: vec![],
            warnings: vec![],
            metrics: PipelineMetrics {
                memory_usage_mb: 0.0,
                cpu_usage_percent: 0.0,
                disk_io_mb: 0.0,
                network_io_mb: 0.0,
                cache_hit_rate: 0.0,
                error_rate: 0.0,
                throughput_records_per_second: 0.0,
            },
            output_locations: vec![],
        })
    }

    /// Process API source using two-stage approach (fetch → store → extract)
    async fn process_api_source(
        &self,
        source_def: &SourceDefinition,
        context: &PipelineContext,
    ) -> Result<TraitPipelineResult> {
        info!(
            "🔄 Starting two-stage API processing for: {}",
            source_def.name
        );

        // Load API config
        let api_config = ApiConfig::from_file(&source_def.config_path).with_context(|| {
            format!("Failed to load API config from {}", source_def.config_path)
        })?;

        // Stage 1: Fetch and store raw API responses (no extraction)
        info!("📥 Stage 1: Fetching and storing raw API responses");
        let fetcher = crate::fetcher::ApiFetcher::new_with_storage(
            api_config.clone(),
            self.minio_storage.clone(),
        )
        .await?;
        let _fetched_responses = fetcher.fetch_and_store_only().await?;
        info!("✅ Stage 1 complete: Raw API responses stored");

        // Stage 2: Load stored raw data and extract products
        info!("🔄 Stage 2: Loading stored data and extracting products");
        let stored_raw_data = self
            .minio_storage
            .load_all_raw_data(&source_def.name)
            .await?;

        // Extract products using the API extractor
        let extractor = crate::extractor::ApiExtractor::new(api_config);
        let mut all_products = Vec::new();

        for raw_response in &stored_raw_data {
            let products = extractor.extract_products(raw_response)?;
            all_products.extend(products);
        }

        info!(
            "✅ Stage 2 complete: Extracted {} products",
            all_products.len()
        );

        // Convert to RawData for pipeline processing
        let raw_data = crate::pipeline::unified_pipeline::RawData::Json(all_products);

        // Process the data through the pipeline stages
        let temp_pipeline =
            crate::pipeline::unified_pipeline::UnifiedPipeline::new(self.minio_storage.clone());

        let unified_context = crate::pipeline::unified_pipeline::PipelineContext {
            source_name: source_def.name.clone(),
            source_type: crate::pipeline::unified_pipeline::SourceType::Api,
            batch_size: Some(1000), // Default batch size
            skip_storage: false,
            validate_data: true,
        };

        let result = temp_pipeline.execute(unified_context, raw_data).await?;

        // Convert unified pipeline result to trait pipeline result
        let now = chrono::Utc::now();
        Ok(TraitPipelineResult {
            execution_id: context.execution_id.clone(),
            pipeline_name: "two-stage-api".to_string(),
            status: ExecutionStatus::Completed,
            start_time: now,
            end_time: Some(now),
            duration: Some(std::time::Duration::from_secs(0)),
            sources_processed: vec![],
            total_records_processed: result.processed_items as u64,
            total_records_output: result.processed_items as u64,
            errors: vec![],
            warnings: vec![],
            metrics: PipelineMetrics {
                memory_usage_mb: 0.0,
                cpu_usage_percent: 0.0,
                disk_io_mb: 0.0,
                network_io_mb: 0.0,
                cache_hit_rate: 0.0,
                error_rate: 0.0,
                throughput_records_per_second: 0.0,
            },
            output_locations: vec![],
        })
    }

    /// Run the complete pipeline with the given options using trait-based architecture
    pub async fn run(&self, options: &PipelineOptions) -> Result<PipelineResult> {
        info!(
            "🚀 Starting Multi-Source Data Pipeline ({})",
            if options.from_storage {
                "Processing from Storage"
            } else {
                "Fetching New Data"
            }
        );

        if let Some(ref source) = options.specific_source {
            info!("🎯 Target: Specific source '{}'", source);
        } else {
            info!("🎯 Target: All sources");
        }

        // Filter sources based on options
        let sources_to_process = self.filter_sources(options)?;

        if sources_to_process.is_empty() {
            return Err(anyhow::anyhow!("No sources to process"));
        }

        info!(
            "Processing {} sources using trait-based pipeline with concurrency support",
            sources_to_process.len()
        );

        // Process each source using the unified pipeline
        // Note: Concurrency is implemented at the HTTP request level within each source
        let mut source_results = Vec::new();
        let mut total_products = 0;
        let mut successful_sources = 0;

        for source_def in sources_to_process {
            let start_time = Instant::now();
            info!(
                "\n=== Processing Source: {} ({:?}) ===",
                source_def.name, source_def.source_type
            );

            // Check if config file exists
            if !Path::new(&source_def.config_path).exists() {
                warn!(
                    "Config file not found for {}: {}",
                    source_def.name, source_def.config_path
                );
                source_results.push(SourceResult {
                    source_name: source_def.name.clone(),
                    success: false,
                    products_count: 0,
                    error_message: Some("Config file not found".to_string()),
                    processing_time_ms: start_time.elapsed().as_millis() as u64,
                });
                continue;
            }

            // Create pipeline context for this source
            let context = self.create_pipeline_context(&source_def, options)?;

            // Execute using trait-based pipeline
            let result = if options.from_storage {
                self.process_source_from_storage_trait(&source_def.name, &context)
                    .await
            } else if source_def.source_type == SourceType::Html {
                // For HTML sources, use two-stage processing (fetch → store → scrape)
                self.process_html_source_two_stage(&source_def, &context)
                    .await
            } else if source_def.source_type == SourceType::Json {
                // For API sources, use two-stage processing (fetch → store → extract)
                self.process_api_source(&source_def, &context).await
            } else {
                self.pipeline.execute(context).await
            };

            let processing_time = start_time.elapsed().as_millis() as u64;

            match result {
                Ok(pipeline_result) => {
                    let products_count = pipeline_result.total_records_processed as usize;

                    // Check if we should consider this a success or failure
                    if products_count == 0 {
                        warn!(
                            "❌ Failed to process {} - No products fetched in {}ms",
                            source_def.name, processing_time
                        );
                        source_results.push(SourceResult {
                            source_name: source_def.name.clone(),
                            success: false,
                            products_count: 0,
                            error_message: Some(
                                "No products fetched - possible API/scraping issue".to_string(),
                            ),
                            processing_time_ms: processing_time,
                        });
                    } else {
                        info!(
                            "✅ Successfully processed {} with {} products in {}ms",
                            source_def.name, products_count, processing_time
                        );
                        source_results.push(SourceResult {
                            source_name: source_def.name.clone(),
                            success: true,
                            products_count,
                            error_message: None,
                            processing_time_ms: processing_time,
                        });
                        total_products += products_count;
                        successful_sources += 1;
                    }
                }
                Err(e) => {
                    error!("❌ Failed to process {}: {}", source_def.name, e);
                    source_results.push(SourceResult {
                        source_name: source_def.name.clone(),
                        success: false,
                        products_count: 0,
                        error_message: Some(e.to_string()),
                        processing_time_ms: processing_time,
                    });
                }
            }
        }

        let failed_sources = source_results.len() - successful_sources;
        let mode_str = if options.from_storage {
            "from Storage"
        } else {
            "from APIs"
        };

        info!("\n=== Multi-Source Pipeline Summary ({}) ===", mode_str);
        info!(
            "✅ Successfully processed {} out of {} sources",
            successful_sources,
            source_results.len()
        );
        info!("📊 Total products processed: {}", total_products);

        if failed_sources > 0 {
            warn!("⚠️ {} sources failed to process", failed_sources);
        }

        if successful_sources > 0 {
            info!(
                "🎉 Multi-source pipeline {} completed successfully!",
                mode_str
            );
        } else {
            warn!("⚠️ No sources were processed successfully {}", mode_str);
        }

        Ok(PipelineResult {
            total_products,
            successful_sources,
            failed_sources,
            source_results,
        })
    }

    /// Create pipeline context for a source
    fn create_pipeline_context(
        &self,
        source_def: &SourceDefinition,
        options: &PipelineOptions,
    ) -> Result<PipelineContext> {
        let execution_mode = if options.memory_efficient {
            ExecutionMode::Batch // Use batch mode for memory efficiency
        } else {
            ExecutionMode::Streaming // Use streaming for standard processing
        };

        let output_config = OutputConfig {
            destinations: vec![OutputDestination::Storage {
                path: format!("processed/{}", source_def.name),
            }],
            format: OutputFormat::Parquet,
            compression: None,
            partitioning: None,
        };

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source_name".to_string(), source_def.name.clone());
        metadata.insert(
            "source_type".to_string(),
            format!("{:?}", source_def.source_type),
        );
        if let Some(batch_size) = options.batch_size {
            metadata.insert("batch_size".to_string(), batch_size.to_string());
        }

        Ok(PipelineContext {
            execution_id: format!("{}_{}", source_def.name, chrono::Utc::now().timestamp()),
            source_filters: vec![source_def.name.clone()],
            processor_config: std::collections::HashMap::new(),
            output_config,
            execution_mode,
            retry_config: crate::traits::RetryConfig {
                max_retries: 3,
                initial_delay: std::time::Duration::from_secs(1),
                max_delay: std::time::Duration::from_secs(60),
                backoff_multiplier: 2.0,
                retry_on_errors: vec!["NetworkError".to_string(), "TimeoutError".to_string()],
            },
            timeout: Some(std::time::Duration::from_secs(300)), // 5 minutes
            metadata,
        })
    }

    /// Process source from storage using trait-based approach
    async fn process_source_from_storage_trait(
        &self,
        source_name: &str,
        _context: &PipelineContext,
    ) -> Result<crate::traits::PipelineResult> {
        info!(
            "Loading raw data from storage for {} using trait-based approach",
            source_name
        );

        // This would be implemented when we have storage-based data sources
        // For now, return an error indicating this feature needs implementation
        Err(anyhow::anyhow!(
            "Storage-based processing with trait architecture not yet implemented"
        ))
    }

    /// Filter sources based on pipeline options
    fn filter_sources(&self, options: &PipelineOptions) -> Result<Vec<SourceDefinition>> {
        let mut sources_to_process = Vec::new();

        if let Some(ref target_source) = options.specific_source {
            // Process only the specified source
            if let Some(source_def) = self.sources.get(target_source) {
                sources_to_process.push(source_def.clone());
            } else {
                let available_sources: Vec<_> = self.sources.keys().collect();
                return Err(anyhow::anyhow!(
                    "Source '{}' not found. Available sources: {}",
                    target_source,
                    available_sources
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        } else {
            // Process all sources
            sources_to_process = self.sources.values().cloned().collect();
        }

        Ok(sources_to_process)
    }

    /// Get available source names
    pub fn get_available_sources(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }

    /// Check if a source exists
    pub fn source_exists(&self, source_name: &str) -> bool {
        self.sources.contains_key(source_name)
    }

    /// Get source definition by name
    pub fn get_source(&self, source_name: &str) -> Option<&SourceDefinition> {
        self.sources.get(source_name)
    }

    /// Get pipeline health status
    pub async fn health_check(&self) -> Result<bool> {
        // Check storage health
        let storage_health = self.storage.health_check().await?;

        // Check pipeline health
        let pipeline_health = self.pipeline.health_check().await?;

        // Use string comparison to avoid ambiguous imports
        Ok(format!("{:?}", storage_health.status) == "Healthy"
            && format!("{:?}", pipeline_health.status) == "Healthy")
    }
}

impl From<&App> for PipelineOptions {
    fn from(app: &App) -> Self {
        Self {
            from_storage: app.from_storage,
            specific_source: app.specific_source.clone(),
            batch_size: app.batch_size,
            memory_efficient: false, // Default to false, can be set explicitly
        }
    }
}

impl PipelineResult {
    /// Check if the pipeline execution was successful
    pub fn is_success(&self) -> bool {
        self.failed_sources == 0
    }

    /// Get the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.source_results.is_empty() {
            0.0
        } else {
            (self.successful_sources as f64 / self.source_results.len() as f64) * 100.0
        }
    }

    /// Get failed source names
    pub fn failed_source_names(&self) -> Vec<&str> {
        self.source_results
            .iter()
            .filter(|result| !result.success)
            .map(|result| result.source_name.as_str())
            .collect()
    }
}
