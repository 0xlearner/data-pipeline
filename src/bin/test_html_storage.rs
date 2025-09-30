use anyhow::Result;
use data_pipeline::extractor::html_extractor::ScrapedProduct;
use data_pipeline::pipeline::unified_pipeline::{PipelineContext, RawData, UnifiedPipeline, SourceType};
use data_pipeline::storage::MinioStorage;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("🧪 Testing HTML Storage Feature");

    // Create MinIO storage (using test credentials)
    let storage = Arc::new(MinioStorage::new(
        "test_access_key",
        "test_secret_key", 
        "http://localhost:9000",
        "test-bucket"
    )?);

    // Create unified pipeline
    let pipeline = UnifiedPipeline::new(storage);

    // Create test products with HTML storage metadata
    let test_products = vec![
        ScrapedProduct::new(
            "Fresh Apples".to_string(),
            "Rs. 250".to_string(),
            "NAHEED_001".to_string(),
            "Fruits".to_string(),
            Some("https://naheed.pk/fruits/apples".to_string()),
            "<div class='product'><h3>Fresh Apples</h3><p>Price: Rs. 250</p></div>".to_string(),
        ).with_storage_metadata("groceries-pets/fresh-products", "fruits", None),
        
        ScrapedProduct::new(
            "Organic Bananas".to_string(),
            "Rs. 180".to_string(),
            "NAHEED_002".to_string(),
            "Fruits".to_string(),
            Some("https://naheed.pk/fruits/bananas".to_string()),
            "<div class='product'><h3>Organic Bananas</h3><p>Price: Rs. 180</p></div>".to_string(),
        ).with_storage_metadata("groceries-pets/fresh-products", "fruits", Some(2)),
        
        ScrapedProduct::new(
            "Fresh Tomatoes".to_string(),
            "Rs. 120".to_string(),
            "NAHEED_003".to_string(),
            "Vegetables".to_string(),
            Some("https://naheed.pk/vegetables/tomatoes".to_string()),
            "<div class='product'><h3>Fresh Tomatoes</h3><p>Price: Rs. 120</p></div>".to_string(),
        ).with_storage_metadata("groceries-pets/fresh-products", "vegetables", None),
    ];

    info!("📦 Created {} test products with HTML storage metadata", test_products.len());

    // Create pipeline context
    let context = PipelineContext {
        source_name: "naheed".to_string(),
        source_type: SourceType::Html,
        batch_size: None,
        skip_storage: false,
        validate_data: false,
    };

    // Execute pipeline with HTML data
    let raw_data = RawData::Html(test_products);
    
    info!("🚀 Executing pipeline...");
    let result = pipeline.execute(context, raw_data).await?;

    info!("✅ Pipeline execution completed!");
    info!("📊 Results:");
    info!("  - Total items: {}", result.total_items);
    info!("  - Processed items: {}", result.processed_items);
    info!("  - Duration: {:?}", result.duration);
    
    if let Some(raw_key) = result.raw_storage_key {
        info!("  - Raw storage key: {}", raw_key);
    }
    
    if let Some(processed_key) = result.processed_storage_key {
        info!("  - Processed storage key: {}", processed_key);
    }

    info!("🎯 Expected HTML files to be stored at:");
    info!("  - YYYY/MM/DD/raw/naheed/groceries-pets/fresh-products/fruits.html");
    info!("  - YYYY/MM/DD/raw/naheed/groceries-pets/fresh-products/fruits.html?p=2");
    info!("  - YYYY/MM/DD/raw/naheed/groceries-pets/fresh-products/vegetables.html");
    info!("  - YYYY/MM/DD/raw/naheed/naheed-YYYYMMDD-HHMMSS.json");

    info!("🎉 HTML Storage test completed!");

    Ok(())
}
