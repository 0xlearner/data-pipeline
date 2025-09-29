use anyhow::{Context, Result};
use polars::prelude::*;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use crate::extractor::ScrapedProduct;
use crate::processor::{FieldClassifier, HtmlProcessor, JsonFlattener, RuleNormalizer};
use crate::storage::MinioStorage;

/// Unified processing pipeline that standardizes data flow regardless of source type
///
/// Pipeline stages:
/// 1. **Fetch** - Get raw data from source (API/HTML)
/// 2. **Extract** - Extract structured data from raw response
/// 3. **Transform** - Convert to unified JSON format
/// 4. **Flatten** - Convert JSON to tabular format (DataFrame)
/// 5. **Classify** - Apply ML field classification
/// 6. **Normalize** - Apply rule-based normalization
/// 7. **Validate** - Validate data quality
/// 8. **Store** - Save to storage (raw + processed)
pub struct UnifiedPipeline {
    storage: Arc<MinioStorage>,
    flattener: JsonFlattener,
    classifier: FieldClassifier,
    normalizer: RuleNormalizer,
    html_processor: HtmlProcessor,
}

/// Pipeline execution context containing metadata and options
#[derive(Debug, Clone)]
pub struct PipelineContext {
    pub source_name: String,
    pub source_type: SourceType,
    pub batch_size: Option<usize>,
    pub skip_storage: bool,
    pub validate_data: bool,
}

/// Source type enumeration for pipeline routing
#[derive(Debug, Clone, PartialEq)]
pub enum SourceType {
    Api,
    Html,
    Storage,
}

/// Pipeline execution result with metrics
#[derive(Debug)]
pub struct PipelineResult {
    pub source_name: String,
    pub total_items: usize,
    pub processed_items: usize,
    pub duration: std::time::Duration,
    pub raw_storage_key: Option<String>,
    pub processed_storage_key: Option<String>,
    pub validation_errors: Vec<String>,
}

/// Raw data input for the pipeline
#[derive(Debug)]
pub enum RawData {
    Json(Vec<Value>),
    Html(Vec<ScrapedProduct>),
}

impl UnifiedPipeline {
    /// Create a new unified pipeline instance
    pub fn new(storage: Arc<MinioStorage>) -> Self {
        Self {
            storage,
            flattener: JsonFlattener::new(),
            classifier: FieldClassifier::new(),
            normalizer: RuleNormalizer,
            html_processor: HtmlProcessor::new(),
        }
    }

    /// Execute the complete pipeline for any data source
    pub async fn execute(
        &self,
        context: PipelineContext,
        raw_data: RawData,
    ) -> Result<PipelineResult> {
        let start_time = Instant::now();
        info!(
            "🚀 Starting unified pipeline for {} ({})",
            context.source_name,
            format!("{:?}", context.source_type)
        );

        // Stage 1: Transform raw data to unified JSON format
        let json_data = self.transform_to_json(raw_data, &context).await?;
        let total_items = json_data.len();

        if total_items == 0 {
            warn!("No data to process for {}", context.source_name);
            return Ok(PipelineResult {
                source_name: context.source_name,
                total_items: 0,
                processed_items: 0,
                duration: start_time.elapsed(),
                raw_storage_key: None,
                processed_storage_key: None,
                validation_errors: vec!["No data to process".to_string()],
            });
        }

        info!(
            "📊 Stage 1: Transformed {} items to JSON format",
            total_items
        );

        // Stage 2: Store raw data (optional)
        let raw_storage_key = if !context.skip_storage {
            Some(
                self.store_raw_data(&context.source_name, &json_data)
                    .await?,
            )
        } else {
            None
        };

        // Stage 3: Process data through pipeline
        let processed_items = self.process_data(&context, &json_data).await?;

        // Stage 4: Store processed data (optional)
        let processed_storage_key = if !context.skip_storage {
            // This will be set by process_data method
            Some(format!("processed/{}/data.parquet", context.source_name))
        } else {
            None
        };

        let duration = start_time.elapsed();
        info!(
            "✅ Pipeline completed for {} in {:?}",
            context.source_name, duration
        );

        Ok(PipelineResult {
            source_name: context.source_name,
            total_items,
            processed_items,
            duration,
            raw_storage_key,
            processed_storage_key,
            validation_errors: vec![], // TODO: Implement validation
        })
    }

    /// Transform raw data to unified JSON format
    async fn transform_to_json(
        &self,
        raw_data: RawData,
        _context: &PipelineContext,
    ) -> Result<Vec<Value>> {
        match raw_data {
            RawData::Json(json_data) => {
                info!("📄 JSON data already in correct format");
                Ok(json_data)
            }
            RawData::Html(scraped_products) => {
                info!(
                    "🔄 Converting {} HTML products to JSON format",
                    scraped_products.len()
                );
                self.html_processor
                    .process_scraped_products(scraped_products)
            }
        }
    }

    /// Store raw data in storage
    async fn store_raw_data(&self, source_name: &str, data: &[Value]) -> Result<String> {
        let raw_json =
            serde_json::to_string(data).context("Failed to serialize raw data to JSON")?;

        let storage_key = self
            .storage
            .store_raw_json(source_name, &raw_json)
            .await
            .context("Failed to store raw data")?;

        info!("💾 Stage 2: Stored raw data at: {}", storage_key);
        Ok(storage_key)
    }

    /// Process data through the standardized pipeline stages
    async fn process_data(&self, context: &PipelineContext, data: &[Value]) -> Result<usize> {
        let total_items = data.len();
        info!(
            "🔄 Stage 3: Processing {} items through pipeline",
            total_items
        );

        // Stage 3.1: Flatten JSON to DataFrame
        let mut df = self
            .flattener
            .flatten_to_dataframe(data)
            .context("Failed to flatten JSON to DataFrame")?;
        info!(
            "📊 Stage 3.1: Flattened to DataFrame with {} rows, {} columns",
            df.height(),
            df.width()
        );

        // Stage 3.2: Apply field classification
        self.classifier
            .map_to_canonical_schema(&mut df)
            .context("Failed to apply field classification")?;
        info!("🏷️  Stage 3.2: Applied field classification");

        // Stage 3.3: Apply rule-based normalization
        self.normalizer
            .normalize_dataframe(&mut df)
            .context("Failed to apply normalization rules")?;
        info!("🔧 Stage 3.3: Applied normalization rules");

        // Stage 3.4: Validate data (optional)
        if context.validate_data {
            self.validate_dataframe(&df)?;
            info!("✅ Stage 3.4: Data validation passed");
        }

        // Stage 3.5: Store processed data
        if !context.skip_storage {
            self.store_processed_data(&context.source_name, &df).await?;
        }

        Ok(total_items)
    }

    /// Store processed data as Parquet
    async fn store_processed_data(&self, source_name: &str, df: &DataFrame) -> Result<String> {
        info!("💾 Stage 3.5: Converting to Parquet format");
        let mut buf = Vec::new();
        {
            let writer = ParquetWriter::new(&mut buf);
            writer
                .finish(&mut df.clone())
                .context("Failed to write DataFrame to Parquet")?;
        }

        let storage_key = self
            .storage
            .store_parquet(source_name, &buf)
            .await
            .context("Failed to store processed data")?;

        info!("💾 Stage 3.5: Stored processed data at: {}", storage_key);
        Ok(storage_key)
    }

    /// Validate DataFrame quality
    fn validate_dataframe(&self, df: &DataFrame) -> Result<()> {
        // Basic validation checks
        if df.height() == 0 {
            return Err(anyhow::anyhow!("DataFrame is empty after processing"));
        }

        // Check for required columns
        let required_columns = ["name", "cost_price"];
        for col in required_columns {
            if df.column(col).is_err() {
                warn!("Missing required column: {}", col);
            }
        }

        // Check for data quality issues
        if let Ok(name_col) = df.column("name") {
            let null_count = name_col.null_count();
            if null_count > 0 {
                warn!("Found {} null values in name column", null_count);
            }
        }

        Ok(())
    }
}

impl Default for PipelineContext {
    fn default() -> Self {
        Self {
            source_name: "unknown".to_string(),
            source_type: SourceType::Api,
            batch_size: None,
            skip_storage: false,
            validate_data: true,
        }
    }
}

impl PipelineContext {
    /// Create a new pipeline context for API source
    pub fn for_api(source_name: String) -> Self {
        Self {
            source_name,
            source_type: SourceType::Api,
            ..Default::default()
        }
    }

    /// Create a new pipeline context for HTML source
    pub fn for_html(source_name: String) -> Self {
        Self {
            source_name,
            source_type: SourceType::Html,
            ..Default::default()
        }
    }

    /// Create a new pipeline context for storage source
    pub fn for_storage(source_name: String) -> Self {
        Self {
            source_name,
            source_type: SourceType::Storage,
            ..Default::default()
        }
    }

    /// Set batch size for processing
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Skip storage operations (for testing)
    pub fn skip_storage(mut self) -> Self {
        self.skip_storage = true;
        self
    }

    /// Disable data validation
    pub fn skip_validation(mut self) -> Self {
        self.validate_data = false;
        self
    }
}
