use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use data_pipeline::config::{ApiConfig, HtmlConfig, MinioConfig};
use data_pipeline::pipeline::{
    PipelineContext, PipelineFactory, RawData, SourceType, UnifiedPipeline,
};
use data_pipeline::storage::MinioStorage;

/// Demonstration of the unified pipeline architecture
///
/// This example shows how the unified pipeline can process data from different sources
/// (API, HTML, Storage) through the same standardized pipeline stages:
///
/// 1. **Fetch** - Get raw data from source
/// 2. **Extract** - Extract structured data
/// 3. **Transform** - Convert to unified JSON format
/// 4. **Flatten** - Convert JSON to tabular format
/// 5. **Classify** - Apply ML field classification
/// 6. **Normalize** - Apply rule-based normalization
/// 7. **Validate** - Validate data quality
/// 8. **Store** - Save to storage
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("🚀 Unified Pipeline Architecture Demo");
    info!("=====================================");

    // Load MinIO configuration
    let minio_config = MinioConfig::from_file("config/sources/minio.toml")?;
    let storage = Arc::new(MinioStorage::from_config(&minio_config)?);

    // Create pipeline factory
    let factory = PipelineFactory::new(storage.clone());

    // Demo 1: API Pipeline with Krave Mart
    info!("\n📡 Demo 1: API Pipeline (Krave Mart)");
    info!("====================================");

    let api_config = ApiConfig::from_file("src/config/sources/krave_mart.toml")?;
    let api_adapter = factory.create_api_adapter();

    // Create custom context for testing (skip storage to avoid side effects)
    let api_context = PipelineContext::for_api("krave_mart_demo".to_string())
        .skip_storage()
        .with_batch_size(100);

    match api_adapter
        .execute_with_context(api_config, api_context)
        .await
    {
        Ok(result) => {
            info!("✅ API Pipeline completed successfully!");
            info!("   📊 Total items: {}", result.total_items);
            info!("   📊 Processed items: {}", result.processed_items);
            info!("   ⏱️  Duration: {:?}", result.duration);
        }
        Err(e) => {
            info!("❌ API Pipeline failed: {}", e);
        }
    }

    // Demo 2: HTML Pipeline with Naheed
    info!("\n🌐 Demo 2: HTML Pipeline (Naheed)");
    info!("==================================");

    let html_config = HtmlConfig::from_file("config/sources/naheed.toml")?;
    let html_adapter = factory.create_html_adapter();

    // Create custom context for testing (skip storage and limit pages)
    let html_context = PipelineContext::for_html("naheed_demo".to_string())
        .skip_storage()
        .skip_validation(); // Skip validation for demo

    match html_adapter
        .execute_with_context(html_config, html_context)
        .await
    {
        Ok(result) => {
            info!("✅ HTML Pipeline completed successfully!");
            info!("   📊 Total items: {}", result.total_items);
            info!("   📊 Processed items: {}", result.processed_items);
            info!("   ⏱️  Duration: {:?}", result.duration);
        }
        Err(e) => {
            info!("❌ HTML Pipeline failed: {}", e);
        }
    }

    // Demo 3: Direct Unified Pipeline Usage
    info!("\n🔧 Demo 3: Direct Unified Pipeline");
    info!("===================================");

    let unified_pipeline = UnifiedPipeline::new(storage.clone());

    // Create some sample JSON data
    let sample_data = vec![
        serde_json::json!({
            "name": "Sample Product 1",
            "price": 29.99,
            "category": "Electronics",
            "sku": "SAMPLE001"
        }),
        serde_json::json!({
            "name": "Sample Product 2",
            "price": 15.50,
            "category": "Books",
            "sku": "SAMPLE002"
        }),
    ];

    let context = PipelineContext::for_api("sample_demo".to_string())
        .skip_storage()
        .skip_validation();

    match unified_pipeline
        .execute(context, RawData::Json(sample_data))
        .await
    {
        Ok(result) => {
            info!("✅ Direct Pipeline completed successfully!");
            info!("   📊 Total items: {}", result.total_items);
            info!("   📊 Processed items: {}", result.processed_items);
            info!("   ⏱️  Duration: {:?}", result.duration);
        }
        Err(e) => {
            info!("❌ Direct Pipeline failed: {}", e);
        }
    }

    // Demo 4: Factory Pattern Usage
    info!("\n🏭 Demo 4: Factory Pattern");
    info!("==========================");

    let sources = vec![
        (SourceType::Api, "krave_mart".to_string()),
        (SourceType::Html, "naheed".to_string()),
    ];

    info!("Processing multiple sources using factory pattern...");

    for (source_type, source_name) in sources {
        info!("🔄 Processing {} ({:?})", source_name, source_type);

        // Note: This would normally load configs and execute, but we'll skip for demo
        // to avoid actual network requests
        match source_type {
            SourceType::Api => {
                info!("   📡 Would execute API pipeline for {}", source_name);
            }
            SourceType::Html => {
                info!("   🌐 Would execute HTML pipeline for {}", source_name);
            }
            SourceType::Storage => {
                info!("   💾 Would execute storage pipeline for {}", source_name);
            }
        }
    }

    info!("\n🎉 Unified Pipeline Demo Complete!");
    info!("===================================");
    info!("Key Benefits Demonstrated:");
    info!("✅ Consistent pipeline stages across all source types");
    info!("✅ Flexible context configuration (batch size, storage, validation)");
    info!("✅ Adapter pattern for different source types");
    info!("✅ Factory pattern for automatic adapter selection");
    info!("✅ Unified error handling and result reporting");
    info!("✅ Standardized metrics and monitoring");

    Ok(())
}
