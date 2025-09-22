use anyhow::{Context, Result};
use config::{ApiConfig, MinioConfig};
use dotenv;
use fetcher::UnifiedFetcher;
use polars::prelude::*;
use processor::{FieldClassifier, JsonFlattener, RuleNormalizer};
use storage::MinioStorage;
use tracing::{info, warn, error};
use tracing_subscriber;
use std::path::Path;
use std::env;

mod config;
mod fetcher;
mod models;
mod processor;
mod storage;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables
    dotenv::dotenv().ok();

    // Check if we should process from storage instead of APIs
    let from_storage = env::args().any(|arg| arg == "--from-storage" || arg == "-s");

    if from_storage {
        info!("🚀 Starting Multi-Source Data Pipeline (Processing from S3/MinIO Storage)");
    } else {
        info!("🚀 Starting Multi-Source Data Pipeline (Fetching from APIs)");
    }

    // Define all available sources
    let sources = vec![
        ("krave_mart", "src/configs/krave_mart.toml"),
        ("bazaar_app", "src/configs/bazaar_app.toml"),
    ];

    // Load MinIO configuration (shared across all sources)
    let minio_config = MinioConfig::from_file("src/configs/minio.toml")
        .context("Failed to load MinIO configuration")?;

    info!(
        "Loaded MinIO configuration: {}@{}",
        minio_config.endpoint, minio_config.bucket_name
    );

    // Initialize shared components
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

    // Process each source
    let mut total_products = 0;
    let mut successful_sources = 0;

    if from_storage {
        // Process from storage mode
        for (source_name, _) in &sources {
            info!("\n=== Processing Source from Storage: {} ===", source_name);

            match process_source_from_storage(
                source_name,
                &storage,
                &flattener,
                &classifier,
                &normalizer,
            ).await {
                Ok(products_count) => {
                    info!("✅ Successfully processed {} with {} products from storage", source_name, products_count);
                    total_products += products_count;
                    successful_sources += 1;
                }
                Err(e) => {
                    error!("❌ Failed to process {} from storage: {}", source_name, e);
                    // Continue with other sources even if one fails
                }
            }
        }
    } else {
        // Process from APIs mode (original behavior)
        for (source_name, config_path) in &sources {
            info!("\n=== Processing Source from API: {} ===", source_name);

            // Check if config file exists
            if !Path::new(config_path).exists() {
                warn!("Config file not found for {}: {}", source_name, config_path);
                continue;
            }

            match process_single_source(
                source_name,
                config_path,
                &storage,
                &flattener,
                &classifier,
                &normalizer,
            ).await {
                Ok(products_count) => {
                    info!("✅ Successfully processed {} with {} products from API", source_name, products_count);
                    total_products += products_count;
                    successful_sources += 1;
                }
                Err(e) => {
                    error!("❌ Failed to process {} from API: {}", source_name, e);
                    // Continue with other sources even if one fails
                }
            }
        }
    }

    let mode_str = if from_storage { "from Storage" } else { "from APIs" };
    info!("\n=== Multi-Source Pipeline Summary ({}) ===", mode_str);
    info!("✅ Successfully processed {} out of {} sources", successful_sources, sources.len());
    info!("📊 Total products processed: {}", total_products);

    if successful_sources > 0 {
        info!("🎉 Multi-source pipeline {} completed successfully!", mode_str);
    } else {
        warn!("⚠️ No sources were processed successfully {}", mode_str);
    }

    Ok(())
}

async fn process_single_source(
    source_name: &str,
    config_path: &str,
    storage: &MinioStorage,
    flattener: &JsonFlattener,
    classifier: &FieldClassifier,
    normalizer: &RuleNormalizer,
) -> Result<usize> {
    // Load source-specific configuration
    let api_config = ApiConfig::from_file(config_path)
        .with_context(|| format!("Failed to load config for {}", source_name))?;

    info!("Loaded config for {}: {} ({})", source_name, api_config.api.name, api_config.request.method);

    // Initialize fetcher for this source
    let fetcher = UnifiedFetcher::new(api_config.clone())?;

    // Fetch data from all categories
    info!("Fetching data from {} API", api_config.api.name);
    let raw_data = fetcher.fetch_all_categories().await?;
    let products_count = raw_data.len();

    info!("Fetched {} total products from {}", products_count, source_name);

    if products_count == 0 {
        warn!("No products fetched from {}", source_name);
        return Ok(0);
    }

    // Store raw JSON
    let raw_json = serde_json::to_string(&raw_data)?;
    let raw_key = storage
        .store_raw_json(&api_config.api.name, &raw_json)
        .await?;
    info!("Stored raw data at: {}", raw_key);

    // Load raw data back from S3 for processing (ensuring consistency)
    info!("Loading raw data from S3 for processing");
    let raw_data_from_storage = storage.load_latest_raw_data(&api_config.api.name).await
        .with_context(|| format!("Failed to load raw data back from S3 for {}", api_config.api.name))?;

    // Process data through pipeline using S3-sourced data
    info!("Processing {} products through pipeline (from S3)", raw_data_from_storage.len());
    let mut df = flattener.flatten_to_dataframe(&raw_data_from_storage)?;
    info!("Flattened to DataFrame with {} rows", df.height());

    // Apply ML classification
    classifier.map_to_canonical_schema(&mut df)?;
    info!("Applied field classification");

    // Apply rule-based normalization
    normalizer.normalize_dataframe(&mut df)?;
    info!("Applied normalization rules");

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

    Ok(products_count)
}

async fn process_source_from_storage(
    source_name: &str,
    storage: &MinioStorage,
    flattener: &JsonFlattener,
    classifier: &FieldClassifier,
    normalizer: &RuleNormalizer,
) -> Result<usize> {
    info!("Loading raw data from storage for {}", source_name);

    // Load raw data from S3/MinIO storage
    let raw_data = storage.load_latest_raw_data(source_name).await
        .with_context(|| format!("Failed to load raw data for {} from storage", source_name))?;

    let products_count = raw_data.len();
    info!("Loaded {} products from storage for {}", products_count, source_name);

    if products_count == 0 {
        warn!("No products found in storage for {}", source_name);
        return Ok(0);
    }

    // Process data through pipeline
    info!("Processing {} products through pipeline", products_count);
    let mut df = flattener.flatten_to_dataframe(&raw_data)?;
    info!("Flattened to DataFrame with {} rows", df.height());

    // Apply ML classification
    classifier.map_to_canonical_schema(&mut df)?;
    info!("Applied field classification");

    // Apply rule-based normalization
    normalizer.normalize_dataframe(&mut df)?;
    info!("Applied normalization rules");

    // Convert to Parquet
    info!("Converting to Parquet format");
    let mut buf = Vec::new();
    {
        let writer = ParquetWriter::new(&mut buf);
        writer.finish(&mut df)?;
    }

    // Store processed data with storage suffix to distinguish from API-sourced data
    let processed_key = storage.store_parquet(&format!("{}_from_storage", source_name), &buf).await?;
    info!("Stored processed data at: {}", processed_key);

    Ok(products_count)
}
