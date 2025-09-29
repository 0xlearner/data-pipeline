use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{error, info};

use crate::config::{ApiConfig, HtmlConfig};
use crate::fetcher::{ApiFetcher, HtmlFetcher};
use crate::pipeline::unified_pipeline::{
    PipelineContext, PipelineResult, RawData, SourceType, UnifiedPipeline,
};
use crate::storage::MinioStorage;

/// Pipeline adapter for API sources
/// Integrates ApiFetcher with UnifiedPipeline
pub struct ApiPipelineAdapter {
    pipeline: UnifiedPipeline,
}

/// Pipeline adapter for HTML sources
/// Integrates HtmlFetcher with UnifiedPipeline
pub struct HtmlPipelineAdapter {
    pipeline: UnifiedPipeline,
}

/// Pipeline adapter for storage sources
/// Loads data from storage and processes through UnifiedPipeline
pub struct StoragePipelineAdapter {
    pipeline: UnifiedPipeline,
    storage: Arc<MinioStorage>,
}

impl ApiPipelineAdapter {
    /// Create a new API pipeline adapter
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        Self {
            pipeline: UnifiedPipeline::new(storage),
        }
    }

    /// Execute the complete API pipeline: fetch -> extract -> process -> store
    pub async fn execute(&self, config: ApiConfig) -> Result<PipelineResult> {
        let source_name = config.api.name.clone();
        info!("🔗 Starting API pipeline for: {}", source_name);

        // Stage 1: Fetch data using ApiFetcher
        let fetcher = ApiFetcher::new_async(config)
            .await
            .context("Failed to create API fetcher")?;

        let raw_data = fetcher
            .fetch_all_categories()
            .await
            .context("Failed to fetch data from API")?;

        info!(
            "📥 Fetched {} items from API: {}",
            raw_data.len(),
            source_name
        );

        // Stage 2: Execute unified pipeline
        let context = PipelineContext::for_api(source_name);
        self.pipeline
            .execute(context, RawData::Json(raw_data))
            .await
    }

    /// Execute API pipeline with custom context
    pub async fn execute_with_context(
        &self,
        config: ApiConfig,
        context: PipelineContext,
    ) -> Result<PipelineResult> {
        let source_name = config.api.name.clone();
        info!(
            "🔗 Starting API pipeline for: {} with custom context",
            source_name
        );

        // Stage 1: Fetch data using ApiFetcher
        let fetcher = ApiFetcher::new_async(config)
            .await
            .context("Failed to create API fetcher")?;

        let raw_data = fetcher
            .fetch_all_categories()
            .await
            .context("Failed to fetch data from API")?;

        info!(
            "📥 Fetched {} items from API: {}",
            raw_data.len(),
            source_name
        );

        // Stage 2: Execute unified pipeline
        self.pipeline
            .execute(context, RawData::Json(raw_data))
            .await
    }
}

impl HtmlPipelineAdapter {
    /// Create a new HTML pipeline adapter
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        Self {
            pipeline: UnifiedPipeline::new(storage),
        }
    }

    /// Execute the complete HTML pipeline: scrape -> extract -> process -> store
    pub async fn execute(&self, config: HtmlConfig) -> Result<PipelineResult> {
        let source_name = config.site.name.clone();
        info!("🔗 Starting HTML pipeline for: {}", source_name);

        // Stage 1: Scrape data using HtmlFetcher
        let fetcher = HtmlFetcher::new(config).context("Failed to create HTML fetcher")?;

        let scraped_data = fetcher
            .fetch_all_categories()
            .await
            .context("Failed to scrape data from HTML")?;

        info!(
            "📥 Scraped {} items from HTML: {}",
            scraped_data.len(),
            source_name
        );

        // Stage 2: Execute unified pipeline
        let context = PipelineContext::for_html(source_name);
        self.pipeline
            .execute(context, RawData::Html(scraped_data))
            .await
    }

    /// Execute HTML pipeline with custom context
    pub async fn execute_with_context(
        &self,
        config: HtmlConfig,
        context: PipelineContext,
    ) -> Result<PipelineResult> {
        let source_name = config.site.name.clone();
        info!(
            "🔗 Starting HTML pipeline for: {} with custom context",
            source_name
        );

        // Stage 1: Scrape data using HtmlFetcher
        let fetcher = HtmlFetcher::new(config).context("Failed to create HTML fetcher")?;

        let scraped_data = fetcher
            .fetch_all_categories()
            .await
            .context("Failed to scrape data from HTML")?;

        info!(
            "📥 Scraped {} items from HTML: {}",
            scraped_data.len(),
            source_name
        );

        // Stage 2: Execute unified pipeline
        self.pipeline
            .execute(context, RawData::Html(scraped_data))
            .await
    }
}

impl StoragePipelineAdapter {
    /// Create a new storage pipeline adapter
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        let pipeline = UnifiedPipeline::new(storage.clone());
        Self { pipeline, storage }
    }

    /// Execute the storage pipeline: load -> process -> store
    pub async fn execute(&self, source_name: &str) -> Result<PipelineResult> {
        info!("🔗 Starting storage pipeline for: {}", source_name);

        // Stage 1: Load data from storage
        let raw_data = self
            .storage
            .load_latest_raw_data(source_name)
            .await
            .context("Failed to load data from storage")?;

        info!(
            "📥 Loaded {} items from storage: {}",
            raw_data.len(),
            source_name
        );

        // Stage 2: Execute unified pipeline
        let context = PipelineContext::for_storage(format!("{}_from_storage", source_name));
        self.pipeline
            .execute(context, RawData::Json(raw_data))
            .await
    }

    /// Execute storage pipeline with custom context
    pub async fn execute_with_context(
        &self,
        source_name: &str,
        context: PipelineContext,
    ) -> Result<PipelineResult> {
        info!(
            "🔗 Starting storage pipeline for: {} with custom context",
            source_name
        );

        // Stage 1: Load data from storage
        let raw_data = self
            .storage
            .load_latest_raw_data(source_name)
            .await
            .context("Failed to load data from storage")?;

        info!(
            "📥 Loaded {} items from storage: {}",
            raw_data.len(),
            source_name
        );

        // Stage 2: Execute unified pipeline
        self.pipeline
            .execute(context, RawData::Json(raw_data))
            .await
    }
}

/// Unified pipeline factory for creating appropriate adapters
#[derive(Clone)]
pub struct PipelineFactory {
    storage: Arc<MinioStorage>,
}

impl PipelineFactory {
    /// Create a new pipeline factory
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        Self { storage }
    }

    /// Create an API pipeline adapter
    pub fn create_api_adapter(&self) -> ApiPipelineAdapter {
        ApiPipelineAdapter::new(self.storage.clone())
    }

    /// Create an HTML pipeline adapter
    pub fn create_html_adapter(&self) -> HtmlPipelineAdapter {
        HtmlPipelineAdapter::new(self.storage.clone())
    }

    /// Create a storage pipeline adapter
    pub fn create_storage_adapter(&self) -> StoragePipelineAdapter {
        StoragePipelineAdapter::new(self.storage.clone())
    }

    /// Execute pipeline for any source type with automatic adapter selection
    pub async fn execute_for_source(
        &self,
        source_type: SourceType,
        source_name: &str,
    ) -> Result<PipelineResult> {
        match source_type {
            SourceType::Api => {
                // Load API config and execute
                let config_path = format!("src/config/sources/{}.toml", source_name);
                let config =
                    ApiConfig::from_file(&config_path).context("Failed to load API config")?;
                self.create_api_adapter().execute(config).await
            }
            SourceType::Html => {
                // Load HTML config and execute
                let config_path = format!("src/config/sources/{}.toml", source_name);
                let config =
                    HtmlConfig::from_file(&config_path).context("Failed to load HTML config")?;
                self.create_html_adapter().execute(config).await
            }
            SourceType::Storage => {
                // Execute storage pipeline
                self.create_storage_adapter().execute(source_name).await
            }
        }
    }
}

/// Batch pipeline executor for processing multiple sources
pub struct BatchPipelineExecutor {
    factory: PipelineFactory,
}

impl BatchPipelineExecutor {
    /// Create a new batch pipeline executor
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        Self {
            factory: PipelineFactory::new(storage),
        }
    }

    /// Execute pipelines for multiple sources concurrently
    pub async fn execute_batch(
        &self,
        sources: Vec<(SourceType, String)>,
    ) -> Vec<Result<PipelineResult>> {
        let mut handles = Vec::new();

        for (source_type, source_name) in sources {
            let factory = self.factory.clone();
            let handle =
                tokio::spawn(
                    async move { factory.execute_for_source(source_type, &source_name).await },
                );
            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    error!("Task failed: {}", e);
                    results.push(Err(anyhow::anyhow!("Task execution failed: {}", e)));
                }
            }
        }

        results
    }

    /// Execute pipelines for multiple sources sequentially
    pub async fn execute_sequential(
        &self,
        sources: Vec<(SourceType, String)>,
    ) -> Vec<Result<PipelineResult>> {
        let mut results = Vec::new();

        for (source_type, source_name) in sources {
            let result = self
                .factory
                .execute_for_source(source_type, &source_name)
                .await;
            results.push(result);
        }

        results
    }
}
