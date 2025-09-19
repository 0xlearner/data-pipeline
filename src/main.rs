use anyhow::{Context, Result};
use config::{ApiConfig, MinioConfig};
use dotenv;
use fetcher::HttpFetcher;
use polars::prelude::*;
use processor::{FieldClassifier, JsonFlattener, RuleNormalizer};
use storage::MinioStorage;
use tracing::info;
use tracing_subscriber;

mod config;
mod fetcher;
mod models;
mod processor;
mod storage;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file (if it exists)
    dotenv::dotenv().ok();

    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("Starting data pipeline");

    // Load API configuration
    let api_config = ApiConfig::from_file("src/configs/krave_mart.toml")
        .context("Failed to load Krave Mart configuration")?;

    // Load MinIO configuration with helpful error messages
    let minio_config = MinioConfig::from_file("src/configs/minio.toml")
        .context("Failed to load MinIO configuration")
        .with_context(|| {
            "MinIO configuration file not found. Please run: ./scripts/setup-minio.sh or copy src/configs/minio.toml.example to src/configs/minio.toml"
        })?;

    info!(
        "Loaded MinIO config - Endpoint: {}, Bucket: {}",
        minio_config.endpoint, minio_config.bucket_name
    );

    // Initialize components
    let fetcher = HttpFetcher::new(api_config.clone())?;
    let storage = MinioStorage::from_config(&minio_config)
        .context("Failed to initialize MinIO storage")
        .with_context(|| {
            "Please ensure MinIO server is running and environment variables are set. Run: ./scripts/setup-minio.sh for setup assistance"
        })?;

    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;

    // Ensure bucket exists
    storage.ensure_bucket().await?;

    // Fetch data from all categories
    info!("Fetching data from {} API", api_config.api.name);
    let raw_data = fetcher.fetch_all_categories().await?;

    info!("Fetched {} total products", raw_data.len());

    // Store raw JSON
    let raw_json = serde_json::to_string(&raw_data)?;
    let raw_key = storage
        .store_raw_json(&api_config.api.name, &raw_json)
        .await?;
    info!("Stored raw data at: {}", raw_key);

    // Process data
    info!("Processing data");
    let mut df = flattener.flatten_to_dataframe(&raw_data)?;

    // Apply ML classification
    classifier.map_to_canonical_schema(&mut df)?;

    // Apply rule-based normalization
    normalizer.normalize_dataframe(&mut df)?;

    // Convert to Parquet
    info!("Converting to Parquet format");
    let mut buf = Vec::new();
    {
        let writer = ParquetWriter::new(&mut buf);
        writer.finish(&mut df)?;
    }

    // Store processed data
    let clean_key = storage.store_parquet(&api_config.api.name, &buf).await?;
    info!("Stored processed data at: {}", clean_key);

    info!("Pipeline completed successfully");
    Ok(())
}
