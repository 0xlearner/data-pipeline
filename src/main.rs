use anyhow::{Context, Result};
use config::{ApiConfig, HtmlConfig, MinioConfig};
use dotenv;
use fetcher::{UnifiedFetcher, HtmlFetcher};
use polars::prelude::*;
use processor::{FieldClassifier, JsonFlattener, HtmlProcessor, RuleNormalizer};
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

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let from_storage = args.iter().any(|arg| arg == "--from-storage" || arg == "-s");

    // Check for specific source argument
    let specific_source = args.iter()
        .position(|arg| arg == "--source")
        .and_then(|pos| args.get(pos + 1))
        .map(|s| s.as_str());

    if from_storage {
        info!("🚀 Starting Multi-Source Data Pipeline (Processing from S3/MinIO Storage)");
    } else {
        info!("🚀 Starting Multi-Source Data Pipeline (Fetching from APIs)");
    }

    if let Some(source) = specific_source {
        info!("🎯 Processing specific source: {}", source);
    }

    // Define all available sources with their types
    let sources = vec![
        ("krave_mart", "src/configs/krave_mart.toml", "json"),
        ("bazaar_app", "src/configs/bazaar_app.toml", "json"),
        ("dealcart", "src/configs/dealcart.toml", "json"),
        ("pandamart", "src/configs/pandamart.toml", "json"),
        ("naheed", "src/configs/naheed.toml", "html"),
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

    // Filter sources based on specific source argument
    let sources_to_process: Vec<_> = if let Some(target_source) = specific_source {
        let filtered: Vec<_> = sources.iter()
            .filter(|(name, _, _)| *name == target_source)
            .cloned()
            .collect();

        if filtered.is_empty() {
            error!("❌ Source '{}' not found. Available sources: {}",
                target_source,
                sources.iter().map(|(name, _, _)| *name).collect::<Vec<_>>().join(", ")
            );
            return Ok(());
        }
        filtered
    } else {
        sources
    };

    if from_storage {
        // Process from storage mode
        for (source_name, _, _) in &sources_to_process {
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
        // Process from APIs/HTML sources mode
        for (source_name, config_path, source_type) in &sources_to_process {
            info!("\n=== Processing Source from {}: {} ===", source_type.to_uppercase(), source_name);

            // Check if config file exists
            if !Path::new(config_path).exists() {
                warn!("Config file not found for {}: {}", source_name, config_path);
                continue;
            }

            let products_count = match source_type.as_ref() {
                "json" => {
                    // Process JSON API source
                    match process_json_source(
                        source_name,
                        config_path,
                        &storage,
                        &flattener,
                        &classifier,
                        &normalizer,
                    ).await {
                        Ok(count) => count,
                        Err(e) => {
                            error!("❌ Failed to process JSON source {}: {}", source_name, e);
                            continue;
                        }
                    }
                }
                "html" => {
                    // Process HTML scraping source
                    match process_html_source(
                        source_name,
                        config_path,
                        &storage,
                        &flattener,
                        &classifier,
                        &normalizer,
                    ).await {
                        Ok(count) => count,
                        Err(e) => {
                            error!("❌ Failed to process HTML source {}: {}", source_name, e);
                            continue;
                        }
                    }
                }
                _ => {
                    warn!("Unknown source type '{}' for {}", source_type, source_name);
                    continue;
                }
            };

            info!("✅ Successfully processed {} with {} products", source_name, products_count);
            total_products += products_count;
            successful_sources += 1;
        }
    }

    let mode_str = if from_storage { "from Storage" } else { "from APIs" };
    info!("\n=== Multi-Source Pipeline Summary ({}) ===", mode_str);
    info!("✅ Successfully processed {} out of {} sources", successful_sources, sources_to_process.len());
    info!("📊 Total products processed: {}", total_products);

    if successful_sources > 0 {
        info!("🎉 Multi-source pipeline {} completed successfully!", mode_str);
    } else {
        warn!("⚠️ No sources were processed successfully {}", mode_str);
    }

    Ok(())
}

async fn process_json_source(
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

    // Get metadata first to determine processing approach
    let (file_path, total_products) = storage.get_latest_raw_data_info(&api_config.api.name).await
        .with_context(|| format!("Failed to get raw data info for {} from storage", api_config.api.name))?;

    info!("Found {} products in {} for processing", total_products, file_path);

    // Determine batch size based on dataset size
    let batch_size = if total_products <= 500 {
        total_products  // Very small datasets: process all at once
    } else if total_products <= 5000 {
        500  // Small-medium datasets: 500 per batch
    } else if total_products <= 50000 {
        2000  // Medium datasets: 2K per batch
    } else {
        5000  // Large datasets: 5K per batch
    };

    info!("Processing {} products in batches of {} for memory efficiency", total_products, batch_size);

    let df = if batch_size >= total_products {
        // Small dataset - use original method
        info!("Using standard processing for small dataset");
        let raw_data_from_storage = storage.load_latest_raw_data(&api_config.api.name).await?;
        flattener.flatten_to_dataframe(&raw_data_from_storage)?
    } else {
        // Large dataset - use batched processing
        info!("Using batched processing for large dataset");
        let batches = storage.stream_latest_raw_data_batched(&api_config.api.name, batch_size).await?;
        flattener.flatten_to_dataframe_batched(batches)?
    };

    info!("Flattened to DataFrame with {} rows", df.height());

    // Apply processing pipeline
    let mut processed_df = df;

    // Apply ML classification
    classifier.map_to_canonical_schema(&mut processed_df)?;
    info!("Applied field classification");

    // Apply rule-based normalization
    normalizer.normalize_dataframe(&mut processed_df)?;
    info!("Applied normalization rules");

    // Convert to Parquet
    info!("Converting to Parquet format");
    let mut buf = Vec::new();
    {
        let writer = ParquetWriter::new(&mut buf);
        writer.finish(&mut processed_df)?;
    }

    // Store processed data
    let clean_key = storage.store_parquet(&api_config.api.name, &buf).await?;
    info!("Stored processed data at: {}", clean_key);

    Ok(products_count)
}

/// Process HTML-based source (web scraping)
async fn process_html_source(
    source_name: &str,
    config_path: &str,
    storage: &MinioStorage,
    flattener: &JsonFlattener,
    classifier: &FieldClassifier,
    normalizer: &RuleNormalizer,
) -> Result<usize> {
    info!("Loading HTML config for {}: {}", source_name, config_path);

    // Load HTML configuration
    let html_config = HtmlConfig::from_file(config_path)
        .with_context(|| format!("Failed to load HTML config from {}", config_path))?;

    info!("Loaded HTML config for {}: {}", source_name, html_config.site.name);

    // Store site name before moving config
    let site_name = html_config.site.name.clone();

    // Initialize HTML fetcher
    let html_fetcher = HtmlFetcher::new(html_config)?;

    // Scrape data from all categories
    info!("Scraping data from {} website", site_name);
    let scraped_products = html_fetcher.fetch_all_categories().await?;
    let products_count = scraped_products.len();

    info!("Scraped {} total products from {}", products_count, source_name);

    if products_count == 0 {
        warn!("No products scraped from {}", source_name);
        return Ok(0);
    }

    // Convert scraped products to JSON format for unified processing
    let html_processor = HtmlProcessor::new();
    let json_products = html_processor.process_scraped_products(scraped_products)?;

    // Store raw JSON (converted from HTML)
    let raw_json = serde_json::to_string(&json_products)?;
    let raw_key = storage
        .store_raw_json(&site_name, &raw_json)
        .await?;
    info!("Stored raw HTML data (as JSON) at: {}", raw_key);

    // Process through unified pipeline (same as JSON sources)
    let total_products = json_products.len();
    let batch_size = if total_products > 10000 { 1000 } else { total_products };

    info!("Processing {} products in batches of {} for memory efficiency", total_products, batch_size);

    let df = if batch_size >= total_products {
        // Small dataset - use original method
        info!("Using standard processing for small dataset");
        flattener.flatten_to_dataframe(&json_products)?
    } else {
        // Large dataset - use batched processing
        info!("Using batched processing for large dataset");
        // For HTML sources, we'll process in memory since data is already loaded
        flattener.flatten_to_dataframe(&json_products)?
    };

    info!("Flattened to DataFrame with {} rows", df.height());

    // Apply processing pipeline
    let mut processed_df = df;

    // Apply ML classification
    classifier.map_to_canonical_schema(&mut processed_df)?;
    info!("Applied field classification");

    // Apply rule-based normalization
    normalizer.normalize_dataframe(&mut processed_df)?;
    info!("Applied normalization rules");

    // Convert to Parquet
    info!("Converting to Parquet format");
    let mut buf = Vec::new();
    {
        let writer = ParquetWriter::new(&mut buf);
        writer.finish(&mut processed_df)?;
    }

    // Store processed data
    let clean_key = storage.store_parquet(&site_name, &buf).await?;
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

    // Get metadata first to determine if we need batching
    let (file_path, total_products) = storage.get_latest_raw_data_info(source_name).await
        .with_context(|| format!("Failed to get raw data info for {} from storage", source_name))?;

    info!("Found {} products in {} for processing", total_products, file_path);

    if total_products == 0 {
        warn!("No products found in storage for {}", source_name);
        return Ok(0);
    }

    // Determine batch size based on dataset size
    let batch_size = if total_products <= 500 {
        total_products  // Very small datasets: process all at once
    } else if total_products <= 5000 {
        500  // Small-medium datasets: 500 per batch
    } else if total_products <= 50000 {
        2000  // Medium datasets: 2K per batch
    } else {
        5000  // Large datasets: 5K per batch
    };

    info!("Processing {} products in batches of {} for memory efficiency", total_products, batch_size);

    let df = if batch_size >= total_products {
        // Small dataset - use original method
        info!("Using standard processing for small dataset");
        let raw_data = storage.load_latest_raw_data(source_name).await?;
        flattener.flatten_to_dataframe(&raw_data)?
    } else {
        // Large dataset - use batched processing
        info!("Using batched processing for large dataset");
        let batches = storage.stream_latest_raw_data_batched(source_name, batch_size).await?;
        flattener.flatten_to_dataframe_batched(batches)?
    };

    info!("Flattened to DataFrame with {} rows", df.height());

    // Apply processing pipeline
    let mut processed_df = df;

    // Apply ML classification
    classifier.map_to_canonical_schema(&mut processed_df)?;
    info!("Applied field classification");

    // Apply rule-based normalization
    normalizer.normalize_dataframe(&mut processed_df)?;
    info!("Applied normalization rules");

    // Convert to Parquet
    info!("Converting to Parquet format");
    let mut buf = Vec::new();
    {
        let writer = ParquetWriter::new(&mut buf);
        writer.finish(&mut processed_df)?;
    }

    // Store processed data with storage suffix to distinguish from API-sourced data
    let processed_key = storage.store_parquet(&format!("{}_from_storage", source_name), &buf).await?;
    info!("Stored processed data at: {}", processed_key);

    Ok(total_products)
}
