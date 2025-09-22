use anyhow::{Context, Result};
use config::MinioConfig;
use dotenv;
use polars::prelude::*;
use processor::{FieldClassifier, JsonFlattener, RuleNormalizer};
use storage::MinioStorage;
use tracing::{info, warn, error};
use tracing_subscriber;
use std::path::Path;

mod config {
    pub use data_pipeline::config::*;
}
mod processor {
    pub use data_pipeline::processor::*;
}
mod storage {
    pub use data_pipeline::storage::*;
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables
    dotenv::dotenv().ok();

    info!("🚀 Starting Multi-Source Data Pipeline (Processing from S3/MinIO Storage)");

    // Define all available sources
    let sources = vec![
        "krave_mart",
        "bazaar_app",
    ];

    // Load MinIO configuration
    let minio_config = MinioConfig::from_file("src/configs/minio.toml")
        .context("Failed to load MinIO configuration")?;

    info!(
        "Loaded MinIO configuration: {}@{}",
        minio_config.endpoint, minio_config.bucket_name
    );

    // Initialize storage
    let storage = MinioStorage::from_config(&minio_config)
        .context("Failed to initialize MinIO storage")
        .with_context(|| {
            "Please ensure MinIO server is running and environment variables are set. Run: ./scripts/setup-minio.sh for setup assistance"
        })?;

    // Initialize processing components
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;

    // Ensure bucket exists
    storage.ensure_bucket().await?;

    // Process each source from storage
    let mut total_products = 0;
    let mut successful_sources = 0;

    for source_name in &sources {
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

    info!("\n=== Multi-Source Pipeline Summary (from Storage) ===");
    info!("✅ Successfully processed {} out of {} sources", successful_sources, sources.len());
    info!("📊 Total products processed: {}", total_products);
    
    if successful_sources > 0 {
        info!("🎉 Multi-source pipeline from storage completed successfully!");
    } else {
        warn!("⚠️ No sources were processed successfully from storage");
    }

    Ok(())
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
