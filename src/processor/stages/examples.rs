use anyhow::Result;
use serde_json::json;
use std::sync::Arc;

use super::{
    RegistryFactory, SourceRegistry, PipelineFactory,
    SourceType, RawSourceData, ProcessingData,
    JsonTransformer, JsonFlattenerStage, FieldClassifierStage,
    PipelineBuilder, SourcePipelineConfig, PipelineConfig,
};
use crate::extractor::ScrapedProduct;

/// Examples demonstrating how to use the new modular processing system
pub struct ProcessingExamples;

impl ProcessingExamples {
    /// Example 1: Using the default registry for standard JSON processing
    pub fn example_standard_json_processing() -> Result<()> {
        println!("=== Example 1: Standard JSON Processing ===");
        
        // Create the default registry with all configurations
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Create a pipeline for JSON API data
        let pipeline = factory.create_for_source(&SourceType::JsonApi)?;
        
        // Sample JSON data
        let json_data = vec![
            json!({
                "name": "iPhone 13",
                "special_price": "699.99",
                "product_price": "799.99",
                "item_id": "IPHONE13",
                "category_section": "Electronics",
                "units": "1 piece"
            }),
            json!({
                "name": "Samsung Galaxy S21",
                "special_price": "599.99", 
                "product_price": "699.99",
                "item_id": "GALAXY21",
                "category_section": "Electronics",
                "units": "1 piece"
            })
        ];
        
        // Process the data
        let raw_data = RawSourceData::Json(json_data);
        let result = pipeline.execute_with_raw_data(raw_data)?;
        
        println!("✅ Processed {} items successfully", result.metrics.total_items_processed);
        println!("⏱️  Total processing time: {}ms", result.metrics.total_time_ms);
        println!("📊 Stages executed: {}", result.metrics.stages_executed);
        
        // The result.data contains the final DataFrame
        match result.data {
            ProcessingData::DataFrame(df) => {
                println!("📋 Final DataFrame: {} rows, {} columns", df.height(), df.width());
                println!("📝 Columns: {:?}", df.get_column_names());
            }
            _ => println!("❌ Unexpected output type"),
        }
        
        Ok(())
    }
    
    /// Example 2: Using HTML scraping with transformer
    pub fn example_html_scraping() -> Result<()> {
        println!("\n=== Example 2: HTML Scraping Processing ===");
        
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Create a pipeline for HTML scraping
        let pipeline = factory.create_for_source(&SourceType::HtmlScraping)?;
        
        // Sample scraped products with HTML storage metadata
        let scraped_products = vec![
            ScrapedProduct {
                name: "Wireless Headphones".to_string(),
                price: "$89.99".to_string(),
                product_id: "WH001".to_string(),
                category: "Audio".to_string(),
                url: Some("https://example.com/headphones".to_string()),
                raw_html: "<div>Product HTML</div>".to_string(),
                category_path: Some("electronics/audio".to_string()),
                page_name: Some("headphones".to_string()),
                page_number: None,
            },
            ScrapedProduct {
                name: "Bluetooth Speaker".to_string(),
                price: "$49.99".to_string(),
                product_id: "BS002".to_string(),
                category: "Audio".to_string(),
                url: Some("https://example.com/speaker".to_string()),
                raw_html: "<div>Speaker HTML</div>".to_string(),
                category_path: Some("electronics/audio".to_string()),
                page_name: Some("speakers".to_string()),
                page_number: Some(2),
            }
        ];
        
        // Process the HTML data
        let raw_data = RawSourceData::Html(scraped_products);
        let result = pipeline.execute_with_raw_data(raw_data)?;
        
        println!("✅ Processed {} items successfully", result.metrics.total_items_processed);
        println!("⏱️  Total processing time: {}ms", result.metrics.total_time_ms);
        
        Ok(())
    }
    
    /// Example 3: Building a custom pipeline
    pub fn example_custom_pipeline() -> Result<()> {
        println!("\n=== Example 3: Custom Pipeline ===");
        
        // Build a custom pipeline with specific stages
        let pipeline = PipelineBuilder::new(
            "custom_pipeline".to_string(),
            SourceType::JsonApi,
        )
        .add_stage(Box::new(JsonFlattenerStage::new()))
        .add_stage(Box::new(FieldClassifierStage::new()))
        // Skip rule normalizer for this example
        .fail_fast(false)
        .max_time(10000) // 10 seconds max
        .build();
        
        // Sample data
        let json_data = vec![
            json!({
                "product_name": "Custom Product",
                "cost": "29.99",
                "identifier": "CUSTOM001"
            })
        ];
        
        let initial_data = ProcessingData::Json(json_data);
        let result = pipeline.execute(initial_data)?;
        
        println!("✅ Custom pipeline completed");
        println!("📊 Stages: {:?}", pipeline.stage_names());
        println!("⏱️  Processing time: {}ms", result.metrics.total_time_ms);
        
        Ok(())
    }
    
    /// Example 4: Adding a new source type
    pub fn example_new_source_type() -> Result<()> {
        println!("\n=== Example 4: Adding New Source Type ===");
        
        // Create a custom registry
        let mut registry = SourceRegistry::new();
        
        // Register shared stages
        registry.register_shared_stage(
            "json_flattener".to_string(),
            Arc::new(JsonFlattenerStage::new())
        );
        registry.register_shared_stage(
            "field_classifier".to_string(),
            Arc::new(FieldClassifierStage::new())
        );
        
        // Register a transformer for the new source
        registry.register_transformer(Box::new(JsonTransformer::new()));
        
        // Register pipeline configuration for new source
        registry.register_pipeline_config(SourcePipelineConfig {
            source_type: SourceType::Custom("my_api".to_string()),
            stage_names: vec![
                "json_flattener".to_string(),
                "field_classifier".to_string(),
            ],
            pipeline_config: PipelineConfig {
                name: "my_api_pipeline".to_string(),
                source_type: SourceType::Custom("my_api".to_string()),
                fail_fast: false,
                max_processing_time_ms: 0,
                custom_config: std::collections::HashMap::new(),
            },
            requires_transformer: false,
        });
        
        println!("✅ New source type 'my_api' registered");
        println!("📋 Available sources: {:?}", registry.get_registered_sources());
        
        Ok(())
    }
    
    /// Example 5: Processing different source types
    pub fn example_multi_source_processing() -> Result<()> {
        println!("\n=== Example 5: Multi-Source Processing ===");
        
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Process JSON API data
        let json_pipeline = factory.create_for_source(&SourceType::JsonApi)?;
        let json_data = vec![json!({"name": "JSON Product", "price": "19.99"})];
        let json_result = json_pipeline.execute_with_raw_data(RawSourceData::Json(json_data))?;
        println!("📊 JSON processing: {} items", json_result.metrics.total_items_processed);
        
        // Process Pandamart data
        let pandamart_pipeline = factory.create_for_source(&SourceType::Pandamart)?;
        let pandamart_data = vec![json!({
            "productID": "PM001",
            "name": "Pandamart Product",
            "price": 150.0,
            "originalPrice": 200.0
        })];
        let pandamart_result = pandamart_pipeline.execute_with_raw_data(RawSourceData::Json(pandamart_data))?;
        println!("📊 Pandamart processing: {} items", pandamart_result.metrics.total_items_processed);
        
        println!("✅ Multi-source processing completed");
        
        Ok(())
    }
    
    /// Example 6: HTML Storage with Category Structure and Pagination
    pub fn example_html_storage_structure() -> Result<()> {
        println!("\n=== Example 6: HTML Storage with Category Structure ===");

        // Create products with HTML storage metadata for Naheed-like structure
        let _naheed_products = vec![
            // Fruits page 1
            ScrapedProduct::new(
                "Fresh Apples".to_string(),
                "Rs. 250".to_string(),
                "NAHEED_001".to_string(),
                "Fruits".to_string(),
                Some("https://naheed.pk/fruits/apples".to_string()),
                "<div class='product'>Fresh Apples HTML</div>".to_string(),
            ).with_storage_metadata("groceries-pets/fresh-products", "fruits", None),

            // Fruits page 2
            ScrapedProduct::new(
                "Organic Bananas".to_string(),
                "Rs. 180".to_string(),
                "NAHEED_002".to_string(),
                "Fruits".to_string(),
                Some("https://naheed.pk/fruits/bananas".to_string()),
                "<div class='product'>Organic Bananas HTML</div>".to_string(),
            ).with_storage_metadata("groceries-pets/fresh-products", "fruits", Some(2)),

            // Vegetables page 1
            ScrapedProduct::new(
                "Fresh Tomatoes".to_string(),
                "Rs. 120".to_string(),
                "NAHEED_003".to_string(),
                "Vegetables".to_string(),
                Some("https://naheed.pk/vegetables/tomatoes".to_string()),
                "<div class='product'>Fresh Tomatoes HTML</div>".to_string(),
            ).with_storage_metadata("groceries-pets/fresh-products", "vegetables", None),
        ];

        println!("📁 Storage structure will be:");
        println!("   raw/naheed/groceries-pets/fresh-products/fruits.html");
        println!("   raw/naheed/groceries-pets/fresh-products/fruits.html?p=2");
        println!("   raw/naheed/groceries-pets/fresh-products/vegetables.html");
        println!("   raw/naheed/naheed-YYYYMMDD-HHMMSS.json (summary)");

        println!("✅ HTML storage structure example completed");

        Ok(())
    }

    /// Run all examples
    pub fn run_all_examples() -> Result<()> {
        println!("🚀 Running Modular Processing System Examples\n");

        Self::example_standard_json_processing()?;
        Self::example_html_scraping()?;
        Self::example_custom_pipeline()?;
        Self::example_new_source_type()?;
        Self::example_multi_source_processing()?;
        Self::example_html_storage_structure()?;

        println!("\n🎉 All examples completed successfully!");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_json_processing() {
        let result = ProcessingExamples::example_standard_json_processing();
        assert!(result.is_ok(), "Standard JSON processing should succeed");
    }

    #[test]
    fn test_custom_pipeline() {
        let result = ProcessingExamples::example_custom_pipeline();
        assert!(result.is_ok(), "Custom pipeline should succeed");
    }

    #[test]
    fn test_new_source_type() {
        let result = ProcessingExamples::example_new_source_type();
        assert!(result.is_ok(), "New source type registration should succeed");
    }
}
