use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use data_pipeline::cli::{ConfigArgs, ConfigCli};
use data_pipeline::config::{ApiConfig, MinioConfig};
use data_pipeline::fetcher::ApiFetcher;
use data_pipeline::infrastructure::LoggingManager;
use data_pipeline::processor::{FieldClassifier, JsonFlattener, RuleNormalizer};
use data_pipeline::storage::MinioStorage;
use dotenv;
use polars::prelude::*;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::time::Instant;
use tracing::{error, info, warn};

/// Unified pipeline tools for data processing, testing, and utilities
#[derive(Parser)]
#[command(name = "pipeline-tools")]
#[command(about = "A unified tool for data pipeline operations")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the main data pipeline
    Pipeline {
        /// Run from storage instead of fetching new data
        #[arg(long)]
        from_storage: bool,
        /// Process specific source only
        #[arg(long)]
        source: Option<String>,
        /// Batch size for processing
        #[arg(long, default_value = "1000")]
        batch_size: usize,
        /// Enable memory-efficient mode
        #[arg(long)]
        memory_efficient: bool,
    },
    /// Test pipeline functionality
    Test {
        /// Test type to run
        #[command(subcommand)]
        test_type: TestCommands,
    },
    /// Utility commands
    Util {
        #[command(subcommand)]
        util_type: UtilCommands,
    },
    /// Configuration management commands
    Config(ConfigArgs),
}

#[derive(Subcommand)]
enum TestCommands {
    /// Test unified pipeline with specific source
    Unified {
        /// Source to test (krave_mart, bazaar_app, pandamart, etc.)
        source: String,
    },
    /// Test memory-efficient pipeline
    Memory,
    /// Test pagination robustness
    Pagination {
        /// Source to test pagination with
        source: String,
    },
    /// Test data cleaning functionality
    Cleaning {
        /// Source to test cleaning with
        source: String,
    },
    /// Test Pandamart configuration
    Pandamart,
}

#[derive(Subcommand)]
enum UtilCommands {
    /// Compare API data between files
    Compare {
        /// First file to compare
        file1: String,
        /// Second file to compare (optional)
        file2: Option<String>,
    },
    /// Count products in data files
    Count {
        /// File or source to count products from
        input: String,
    },
    /// Debug column issues in data
    DebugColumns {
        /// Source to debug
        source: String,
    },
    /// Verify column fixes
    VerifyColumns {
        /// Source to verify
        source: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    LoggingManager::for_development()?;

    // Load environment variables
    dotenv::dotenv().ok();

    match cli.command {
        Commands::Pipeline {
            from_storage,
            source,
            batch_size,
            memory_efficient,
        } => run_pipeline(from_storage, source, batch_size, memory_efficient).await,
        Commands::Test { test_type } => run_test(test_type).await,
        Commands::Util { util_type } => run_utility(util_type).await,
        Commands::Config(config_args) => ConfigCli::execute(config_args).await,
    }
}

async fn run_pipeline(
    from_storage: bool,
    source: Option<String>,
    batch_size: usize,
    memory_efficient: bool,
) -> Result<()> {
    if from_storage {
        info!("🚀 Starting Data Pipeline (Processing from Storage)");
        run_from_storage_pipeline(source, memory_efficient).await
    } else {
        info!("🚀 Starting Data Pipeline (Fetching New Data)");
        run_fetch_pipeline(source, batch_size, memory_efficient).await
    }
}

async fn run_from_storage_pipeline(
    source_filter: Option<String>,
    memory_efficient: bool,
) -> Result<()> {
    // Load MinIO configuration
    let minio_config = MinioConfig::from_file("minio_config.toml")
        .context("Failed to load MinIO configuration from file")?;

    // Initialize storage
    let storage = MinioStorage::from_config(&minio_config)?;

    // Define available sources
    let sources = vec!["krave_mart", "bazaar_app", "pandamart", "naheed"];

    let sources_to_process: Vec<&str> = if let Some(ref filter) = source_filter {
        vec![filter.as_str()]
    } else {
        sources
    };

    for source_name in sources_to_process {
        info!("📦 Processing {} from storage", source_name);

        match storage.load_latest_raw_data(source_name).await {
            Ok(raw_data) => {
                info!(
                    "✅ Loaded raw data for {}: {} records",
                    source_name,
                    raw_data.len()
                );

                if memory_efficient {
                    process_data_memory_efficient(source_name, &raw_data, &storage).await?;
                } else {
                    process_data_standard(source_name, &raw_data, &storage).await?;
                }
            }
            Err(e) => {
                warn!("⚠️ Failed to load data for {}: {}", source_name, e);
                continue;
            }
        }
    }

    Ok(())
}

async fn run_fetch_pipeline(
    source_filter: Option<String>,
    _batch_size: usize,
    memory_efficient: bool,
) -> Result<()> {
    // This would use the new pipeline orchestrator when fully integrated
    info!("Fetch pipeline not yet fully integrated with new architecture");
    info!("Use the existing main binary for now: cargo run --bin data-pipeline");

    if memory_efficient {
        info!("Memory-efficient mode would be enabled");
    }

    if let Some(source) = source_filter {
        info!("Would process only source: {}", source);
    }

    Ok(())
}

async fn process_data_memory_efficient(
    _source_name: &str,
    raw_data: &[Value],
    _storage: &MinioStorage,
) -> Result<()> {
    let start_time = Instant::now();

    // Process in smaller chunks to reduce memory usage
    let chunk_size = 100;
    let chunks: Vec<_> = raw_data.chunks(chunk_size).collect();

    info!(
        "Processing {} records in {} chunks of {}",
        raw_data.len(),
        chunks.len(),
        chunk_size
    );

    for (i, chunk) in chunks.iter().enumerate() {
        info!("Processing chunk {}/{}", i + 1, chunks.len());

        // Process chunk
        let flattener = JsonFlattener::new();
        let classifier = FieldClassifier::new();
        let normalizer = RuleNormalizer;

        match flattener.flatten_to_dataframe(chunk) {
            Ok(mut df) => {
                // Apply processing
                classifier.map_to_canonical_schema(&mut df)?;
                normalizer.normalize_dataframe(&mut df)?;

                info!(
                    "Processed chunk with {} rows, {} columns",
                    df.height(),
                    df.width()
                );
            }
            Err(e) => {
                warn!("Failed to process chunk {}: {}", i + 1, e);
            }
        }
    }

    let duration = start_time.elapsed();
    info!("✅ Memory-efficient processing completed in {:?}", duration);

    Ok(())
}

async fn process_data_standard(
    source_name: &str,
    raw_data: &[Value],
    storage: &MinioStorage,
) -> Result<()> {
    let start_time = Instant::now();

    if raw_data.is_empty() {
        warn!("No data found for {}", source_name);
        return Ok(());
    }

    // Process data
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;

    let mut df = flattener.flatten_to_dataframe(raw_data)?;
    classifier.map_to_canonical_schema(&mut df)?;
    normalizer.normalize_dataframe(&mut df)?;

    info!(
        "✅ Processed {} with {} rows, {} columns",
        source_name,
        df.height(),
        df.width()
    );

    // Store processed data
    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer).finish(&mut df)?;

    match storage.store_parquet(source_name, &buffer).await {
        Ok(path) => info!("💾 Stored processed data to: {}", path),
        Err(e) => warn!("Failed to store processed data: {}", e),
    }

    let duration = start_time.elapsed();
    info!("✅ Standard processing completed in {:?}", duration);

    Ok(())
}

async fn run_test(test_type: TestCommands) -> Result<()> {
    match test_type {
        TestCommands::Unified { source } => test_unified_pipeline(source).await,
        TestCommands::Memory => test_memory_efficient().await,
        TestCommands::Pagination { source } => test_pagination(source).await,
        TestCommands::Cleaning { source } => test_data_cleaning(source).await,
        TestCommands::Pandamart => test_pandamart_config().await,
    }
}

async fn test_unified_pipeline(source: String) -> Result<()> {
    info!("=== UNIFIED PIPELINE TEST: {} ===", source);

    let config_path = match source.as_str() {
        "krave_mart" => "src/config/sources/krave_mart.toml",
        "bazaar_app" => "src/config/sources/bazaar_app.toml",
        "pandamart" => "src/config/sources/pandamart.toml",
        _ => {
            error!("❌ Unknown source: {}", source);
            return Ok(());
        }
    };

    info!("🔧 Loading config from: {}", config_path);
    let config = ApiConfig::from_file(config_path)?;

    info!("✅ Config loaded successfully!");
    info!("   API Name: {}", config.api.name);
    info!("   Base URL: {}", config.api.base_url);

    // Test fetching
    let fetcher = ApiFetcher::new_async(config).await?;
    let data = fetcher.fetch_all_categories().await?;

    info!("✅ Fetched {} records", data.len());

    Ok(())
}

async fn test_memory_efficient() -> Result<()> {
    info!("🧪 Testing Memory-Efficient Pipeline");
    run_from_storage_pipeline(None, true).await
}

async fn test_pagination(source: String) -> Result<()> {
    info!("🧪 Testing Pagination Robustness: {}", source);
    // Implementation would test pagination logic
    info!("Pagination test not yet implemented");
    Ok(())
}

async fn test_data_cleaning(source: String) -> Result<()> {
    info!("🧪 Testing Data Cleaning: {}", source);
    // Implementation would test data cleaning logic
    info!("Data cleaning test not yet implemented");
    Ok(())
}

async fn test_pandamart_config() -> Result<()> {
    info!("=== PANDAMART CONFIGURATION TEST ===");

    let config_path = "src/config/sources/pandamart.toml";
    info!("🔧 Loading Pandamart config from: {}", config_path);

    let config = ApiConfig::from_file(config_path)?;

    info!("✅ Config loaded successfully!");
    info!("   API Name: {}", config.api.name);
    info!("   Method: {}", config.request.method);
    info!("   Base URL: {}", config.api.base_url);

    Ok(())
}

async fn run_utility(util_type: UtilCommands) -> Result<()> {
    match util_type {
        UtilCommands::Compare { file1, file2 } => compare_api_data(file1, file2).await,
        UtilCommands::Count { input } => count_products(input).await,
        UtilCommands::DebugColumns { source } => debug_columns(source).await,
        UtilCommands::VerifyColumns { source } => verify_columns(source).await,
    }
}

async fn compare_api_data(file1: String, file2: Option<String>) -> Result<()> {
    info!("=== API DATA COMPARISON TOOL ===");

    // Read the first file
    let data1 = fs::read_to_string(&file1)?;
    let json1: Value = serde_json::from_str(&data1)?;

    // Extract product IDs from first file
    let mut product_ids1 = HashSet::new();
    extract_product_ids(&json1, &mut product_ids1);

    info!("File '{}' contains {} products", file1, product_ids1.len());

    if let Some(file2) = file2 {
        let data2 = fs::read_to_string(&file2)?;
        let json2: Value = serde_json::from_str(&data2)?;

        let mut product_ids2 = HashSet::new();
        extract_product_ids(&json2, &mut product_ids2);

        info!("File '{}' contains {} products", file2, product_ids2.len());

        let common = product_ids1.intersection(&product_ids2).count();
        let unique1 = product_ids1.difference(&product_ids2).count();
        let unique2 = product_ids2.difference(&product_ids1).count();

        info!("Common products: {}", common);
        info!("Unique to {}: {}", file1, unique1);
        info!("Unique to {}: {}", file2, unique2);
    }

    Ok(())
}

fn extract_product_ids(json: &Value, product_ids: &mut HashSet<u64>) {
    if let Some(data_array) = json.get("data").and_then(|d| d.as_array()) {
        for data_item in data_array {
            if let Some(l2_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                for product in l2_products {
                    if let Some(product_id) = product.get("product_id").and_then(|id| id.as_u64()) {
                        product_ids.insert(product_id);
                    }
                }
            }
        }
    }
}

async fn count_products(input: String) -> Result<()> {
    info!("=== PRODUCT COUNT TOOL ===");
    info!("Counting products in: {}", input);

    // Try to read as file first
    if let Ok(data) = fs::read_to_string(&input) {
        if let Ok(json) = serde_json::from_str::<Value>(&data) {
            let mut product_ids = HashSet::new();
            extract_product_ids(&json, &mut product_ids);
            info!("Found {} products in file", product_ids.len());
        } else {
            warn!("File is not valid JSON");
        }
    } else {
        info!("Could not read as file, treating as source name");
        // Could integrate with storage to count products from a source
    }

    Ok(())
}

async fn debug_columns(source: String) -> Result<()> {
    info!("=== DEBUG COLUMNS TOOL ===");
    info!("Debugging columns for source: {}", source);
    info!("Debug columns functionality not yet implemented");
    Ok(())
}

async fn verify_columns(source: String) -> Result<()> {
    info!("=== VERIFY COLUMNS TOOL ===");
    info!("Verifying columns for source: {}", source);
    info!("Verify columns functionality not yet implemented");
    Ok(())
}
