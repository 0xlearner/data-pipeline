use anyhow::{Context, Result};
use config::MinioConfig;
use dotenv;
use processor::{FieldClassifier, JsonFlattener, RuleNormalizer};
use storage::MinioStorage;
use tracing::{info, warn};
use tracing_subscriber;
use std::time::Instant;

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

    info!("🧪 Testing Memory-Efficient Data Pipeline");

    // Load MinIO configuration
    let minio_config = MinioConfig::from_file("src/configs/minio.toml")
        .context("Failed to load MinIO configuration")?;

    // Initialize storage
    let storage = MinioStorage::from_config(&minio_config)
        .context("Failed to initialize MinIO storage")?;

    // Initialize processing components
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;

    // Test sources with different sizes
    let test_sources = vec![
        ("krave_mart", "Small dataset"),
        ("bazaar_app", "Medium dataset"), 
        ("dealcart", "Large dataset"),
    ];

    for (source_name, description) in &test_sources {
        info!("\n=== Testing {} ({}) ===", source_name, description);
        
        let start_time = Instant::now();
        
        match test_memory_efficient_processing(
            source_name,
            &storage,
            &flattener,
            &classifier,
            &normalizer,
        ).await {
            Ok((products_count, processing_method)) => {
                let duration = start_time.elapsed();
                info!("✅ {} processed successfully:", source_name);
                info!("   📊 Products: {}", products_count);
                info!("   ⚡ Method: {}", processing_method);
                info!("   ⏱️  Duration: {:.2}s", duration.as_secs_f64());
                info!("   💾 Memory: {} products per batch", 
                      if processing_method.contains("batched") { "5000-2000" } else { "all" });
            }
            Err(e) => {
                warn!("❌ Failed to process {}: {}", source_name, e);
            }
        }
    }

    info!("\n🎉 Memory-efficient pipeline testing completed!");
    Ok(())
}

async fn test_memory_efficient_processing(
    source_name: &str,
    storage: &MinioStorage,
    flattener: &JsonFlattener,
    classifier: &FieldClassifier,
    normalizer: &RuleNormalizer,
) -> Result<(usize, String)> {
    // Get metadata first to determine processing approach
    let (file_path, total_products) = storage.get_latest_raw_data_info(source_name).await
        .with_context(|| format!("Failed to get raw data info for {} from storage", source_name))?;

    info!("📁 File: {}", file_path);
    info!("📊 Total products: {}", total_products);

    if total_products == 0 {
        return Ok((0, "No data".to_string()));
    }

    // Determine batch size based on dataset size
    let (batch_size, processing_method) = if total_products > 50000 {
        (5000, "Large dataset - batched processing (5K per batch)")
    } else if total_products > 10000 {
        (2000, "Medium dataset - batched processing (2K per batch)")
    } else {
        (total_products, "Small dataset - standard processing")
    };

    info!("🔧 Processing method: {}", processing_method);

    let processing_start = Instant::now();
    
    let df = if batch_size >= total_products {
        // Small dataset - use original method
        info!("📥 Loading all data at once...");
        let raw_data = storage.load_latest_raw_data(source_name).await?;
        flattener.flatten_to_dataframe(&raw_data)?
    } else {
        // Large dataset - use batched processing
        info!("📥 Streaming data in batches of {}...", batch_size);
        let batches = storage.stream_latest_raw_data_batched(source_name, batch_size).await?;
        flattener.flatten_to_dataframe_batched(batches)?
    };

    let flattening_duration = processing_start.elapsed();
    info!("⚡ Flattening completed in {:.2}s: {} rows", 
          flattening_duration.as_secs_f64(), df.height());

    // Apply processing pipeline (simulate full pipeline)
    let mut processed_df = df;
    
    let classification_start = Instant::now();
    classifier.map_to_canonical_schema(&mut processed_df)?;
    let classification_duration = classification_start.elapsed();
    info!("🏷️  Classification completed in {:.2}s", classification_duration.as_secs_f64());

    let normalization_start = Instant::now();
    normalizer.normalize_dataframe(&mut processed_df)?;
    let normalization_duration = normalization_start.elapsed();
    info!("🧹 Normalization completed in {:.2}s", normalization_duration.as_secs_f64());

    let total_processing_time = processing_start.elapsed();
    info!("⏱️  Total processing time: {:.2}s", total_processing_time.as_secs_f64());
    info!("📈 Processing rate: {:.0} products/second", 
          total_products as f64 / total_processing_time.as_secs_f64());

    Ok((total_products, processing_method.to_string()))
}
