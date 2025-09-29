use anyhow::{Context, Result};
use polars::prelude::*;
use std::sync::Arc;
use tracing::{info, warn};

use crate::config::loader::SourceConfig;
use crate::fetcher::{ApiFetcher, HtmlFetcher};
use crate::processor::{FieldClassifier, HtmlProcessor, JsonFlattener, RuleNormalizer};
use crate::storage::MinioStorage;

/// Pipeline executor responsible for processing individual sources
pub struct PipelineExecutor {
    storage: Arc<MinioStorage>,
    flattener: JsonFlattener,
    classifier: FieldClassifier,
    normalizer: RuleNormalizer,
    html_processor: HtmlProcessor,
}

impl PipelineExecutor {
    /// Create a new pipeline executor
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        Self {
            storage,
            flattener: JsonFlattener::new(),
            classifier: FieldClassifier::new(),
            normalizer: RuleNormalizer,
            html_processor: HtmlProcessor::new(),
        }
    }

    /// Process a source from storage (previously fetched data)
    pub async fn process_from_storage(
        &self,
        source_name: &str,
        batch_size: Option<usize>,
    ) -> Result<usize> {
        info!("Loading raw data from storage for {}", source_name);

        // Load raw data from storage
        let raw_data = self
            .storage
            .load_latest_raw_data(source_name)
            .await
            .with_context(|| format!("Failed to load raw data for {} from storage", source_name))?;

        let products_count = raw_data.len();
        info!(
            "Loaded {} products from storage for {}",
            products_count, source_name
        );

        if products_count == 0 {
            warn!("No products found in storage for {}", source_name);
            return Ok(0);
        }

        // Process the data through the pipeline
        self.process_data(source_name, &raw_data, batch_size).await
    }

    /// Process a source by fetching from the original API/website
    pub async fn process_from_source(
        &self,
        source_name: &str,
        source_config: &SourceConfig,
        batch_size: Option<usize>,
    ) -> Result<usize> {
        info!("Fetching data from source: {}", source_name);

        // Fetch data based on source type
        let raw_data = match source_config {
            SourceConfig::Json(api_config) => {
                let fetcher = ApiFetcher::new_async(api_config.clone()).await?;
                fetcher.fetch_all_categories().await?
            }
            SourceConfig::Html(html_config) => {
                let html_fetcher = HtmlFetcher::new(html_config.clone())?;
                let scraped_products = html_fetcher.fetch_all_categories().await?;

                // Convert scraped products to JSON format
                let mut json_products = Vec::new();
                for product in scraped_products {
                    match self.html_processor.convert_to_json(&product) {
                        Ok(json_product) => json_products.push(json_product),
                        Err(e) => {
                            warn!("Failed to convert HTML product to JSON: {}", e);
                            continue;
                        }
                    }
                }
                json_products
            }
        };

        let products_count = raw_data.len();
        info!("Fetched {} products from {}", products_count, source_name);

        if products_count == 0 {
            warn!("No products fetched from {}", source_name);
            return Ok(0);
        }

        // Store raw data
        let raw_json = serde_json::to_string(&raw_data)?;
        let raw_key = self.storage.store_raw_json(source_name, &raw_json).await?;
        info!("Stored raw data at: {}", raw_key);

        // Process the data through the pipeline
        self.process_data(source_name, &raw_data, batch_size).await
    }

    /// Process raw data through the complete pipeline
    async fn process_data(
        &self,
        source_name: &str,
        raw_data: &[serde_json::Value],
        batch_size: Option<usize>,
    ) -> Result<usize> {
        let total_products = raw_data.len();

        // Determine batch size for memory efficiency
        let batch_size = batch_size.unwrap_or_else(|| {
            if total_products > 10000 {
                2000
            } else if total_products > 5000 {
                1000
            } else {
                total_products
            }
        });

        info!(
            "Processing {} products in batches of {} for memory efficiency",
            total_products, batch_size
        );

        // Process data based on size
        let df = if batch_size >= total_products {
            // Small dataset - use standard processing
            info!("Using standard processing for small dataset");
            self.flattener.flatten_to_dataframe(raw_data)?
        } else {
            // Large dataset - use batched processing
            info!("Using batched processing for large dataset");
            let batches = self
                .storage
                .stream_latest_raw_data_batched(source_name, batch_size)
                .await?;
            self.flattener.flatten_to_dataframe_batched(batches)?
        };

        info!("Flattened to DataFrame with {} rows", df.height());

        // Apply processing pipeline
        let mut processed_df = df;

        // Apply ML classification
        self.classifier.map_to_canonical_schema(&mut processed_df)?;
        info!("Applied field classification");

        // Apply rule-based normalization
        self.normalizer.normalize_dataframe(&mut processed_df)?;
        info!("Applied normalization rules");

        // Convert to Parquet
        info!("Converting to Parquet format");
        let mut buf = Vec::new();
        {
            let writer = ParquetWriter::new(&mut buf);
            writer.finish(&mut processed_df)?;
        }

        // Store processed data
        let clean_key = self.storage.store_parquet(source_name, &buf).await?;
        info!("Stored processed data at: {}", clean_key);

        Ok(total_products)
    }

    /// Get processing statistics for a source
    pub async fn get_processing_stats(&self, source_name: &str) -> Result<ProcessingStats> {
        // Load raw data to get count
        let raw_data = self.storage.load_latest_raw_data(source_name).await?;

        // For now, assume processed count equals raw count (we'll improve this later)
        let processed_count = raw_data.len();

        Ok(ProcessingStats {
            source_name: source_name.to_string(),
            raw_count: raw_data.len(),
            processed_count,
            success_rate: if raw_data.is_empty() {
                0.0
            } else {
                100.0 // Assume 100% success for now
            },
        })
    }
}

/// Processing statistics for a source
#[derive(Debug)]
pub struct ProcessingStats {
    pub source_name: String,
    pub raw_count: usize,
    pub processed_count: usize,
    pub success_rate: f64,
}

impl ProcessingStats {
    /// Check if processing was successful (no data loss)
    pub fn is_successful(&self) -> bool {
        self.success_rate >= 95.0 // Allow for some minor data loss
    }

    /// Get the number of lost records
    pub fn lost_records(&self) -> usize {
        if self.raw_count > self.processed_count {
            self.raw_count - self.processed_count
        } else {
            0
        }
    }
}
