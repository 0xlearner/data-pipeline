use anyhow::Result;
use serde_json::json;
use std::sync::Arc;

use super::{
    RegistryFactory, PipelineFactory, SourceType, RawSourceData, ProcessingData,
    JsonFlattenerStage, FieldClassifierStage,
    PipelineBuilder,
};
use crate::extractor::ScrapedProduct;

/// Integration tests for the modular processing system
/// 
/// These tests verify that the entire system works together correctly,
/// from raw data input through all processing stages to final output.
#[cfg(test)]
mod tests {
    use super::*;

    /// Test complete JSON processing pipeline
    #[test]
    fn test_complete_json_pipeline() -> Result<()> {
        // Create registry and factory
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Create pipeline for JSON API
        let pipeline = factory.create_for_source(&SourceType::JsonApi)?;
        
        // Sample JSON data with various field names that need normalization
        let json_data = vec![
            json!({
                "product_name": "Test Product 1",
                "special_price": "19.99",
                "product_price": "29.99",
                "item_id": "TEST001",
                "category_section": "Electronics",
                "units": "1 piece"
            }),
            json!({
                "name": "Test Product 2",
                "cost_price": "39.99",
                "mrp": "49.99",
                "product_id": "TEST002",
                "category": "Home",
                "units_of_mass": "500g"
            })
        ];
        
        // Execute pipeline
        let raw_data = RawSourceData::Json(json_data);
        let result = pipeline.execute_with_raw_data(raw_data)?;
        
        // Verify results
        assert!(result.success, "Pipeline should succeed");
        assert_eq!(result.metrics.total_items_processed, 2);
        assert!(result.metrics.total_time_ms > 0);
        assert_eq!(result.stage_results.len(), 3); // 3 stages
        
        // Check final data is DataFrame
        match result.data {
            ProcessingData::DataFrame(df) => {
                assert_eq!(df.height(), 2); // 2 rows
                assert!(df.width() > 0); // Has columns
                
                // Check that canonical field names are present
                let column_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
                assert!(column_names.contains(&"name") || column_names.contains(&"product_name"));
                assert!(column_names.contains(&"cost_price") || column_names.contains(&"special_price"));
            }
            _ => panic!("Expected DataFrame output"),
        }
        
        Ok(())
    }
    
    /// Test HTML processing pipeline with transformer
    #[test]
    fn test_complete_html_pipeline() -> Result<()> {
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Create pipeline for HTML scraping
        let pipeline = factory.create_for_source(&SourceType::HtmlScraping)?;
        
        // Sample scraped products
        let scraped_products = vec![
            ScrapedProduct {
                name: "Scraped Product 1".to_string(),
                price: "$25.99".to_string(),
                product_id: "SCRAPE001".to_string(),
                category: "Books".to_string(),
                url: Some("https://example.com/book1".to_string()),
                raw_html: "<div>Book HTML</div>".to_string(),
                category_path: None,
                page_name: None,
                page_number: None,
            },
            ScrapedProduct {
                name: "Scraped Product 2".to_string(),
                price: "₹1,299".to_string(),
                product_id: "SCRAPE002".to_string(),
                category: "Electronics".to_string(),
                url: Some("https://example.com/electronics".to_string()),
                raw_html: "<div>Electronics HTML</div>".to_string(),
                category_path: None,
                page_name: None,
                page_number: None,
            }
        ];
        
        // Execute pipeline
        let raw_data = RawSourceData::Html(scraped_products);
        let result = pipeline.execute_with_raw_data(raw_data)?;
        
        // Verify results
        assert!(result.success, "HTML pipeline should succeed");
        assert_eq!(result.metrics.total_items_processed, 2);
        
        // Check final data
        match result.data {
            ProcessingData::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                
                // Verify that HTML transformer converted data correctly
                let column_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
                assert!(column_names.contains(&"name") || column_names.contains(&"product_name"));
                assert!(column_names.contains(&"cost_price") || column_names.contains(&"price"));
            }
            _ => panic!("Expected DataFrame output"),
        }
        
        Ok(())
    }
    
    /// Test Pandamart processing pipeline
    #[test]
    fn test_pandamart_pipeline() -> Result<()> {
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Create pipeline for Pandamart
        let pipeline = factory.create_for_source(&SourceType::Pandamart)?;
        
        // Sample Pandamart GraphQL response data
        let pandamart_data = vec![
            json!({
                "productID": "PM001",
                "name": "Pandamart Product 1",
                "price": 150.0,
                "originalPrice": 200.0,
                "categoryProducts": {
                    "name": "Groceries"
                },
                "attributes": [
                    {"value": "PM-SKU-001"},
                    {"value": "Brand Name"},
                    {"value": "500g"}
                ]
            }),
            json!({
                "productID": "PM002",
                "name": "Pandamart Product 2",
                "price": 75.0,
                "originalPrice": 100.0,
                "categoryProducts": {
                    "name": "Beverages"
                },
                "attributes": [
                    {"value": "PM-SKU-002"},
                    {"value": "Another Brand"},
                    {"value": "1L"}
                ]
            })
        ];
        
        // Execute pipeline
        let raw_data = RawSourceData::Json(pandamart_data);
        let result = pipeline.execute_with_raw_data(raw_data)?;
        
        // Verify results
        assert!(result.success, "Pandamart pipeline should succeed");
        assert_eq!(result.metrics.total_items_processed, 2);
        
        // Check final data
        match result.data {
            ProcessingData::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                
                // Verify Pandamart-specific transformations
                let column_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
                assert!(column_names.contains(&"name") || column_names.contains(&"product_name"));
                assert!(column_names.contains(&"cost_price") || column_names.contains(&"price"));
                assert!(column_names.contains(&"category"));
            }
            _ => panic!("Expected DataFrame output"),
        }
        
        Ok(())
    }
    
    /// Test custom pipeline creation
    #[test]
    fn test_custom_pipeline() -> Result<()> {
        // Create a custom pipeline with only specific stages
        let pipeline = PipelineBuilder::new(
            "test_custom".to_string(),
            SourceType::JsonApi,
        )
        .add_stage(Box::new(JsonFlattenerStage::new()))
        .add_stage(Box::new(FieldClassifierStage::new()))
        // Skip rule normalizer
        .fail_fast(false)
        .build();
        
        // Test data
        let json_data = vec![
            json!({
                "product_name": "Custom Test",
                "special_price": "99.99",
                "item_id": "CUSTOM001"
            })
        ];
        
        let initial_data = ProcessingData::Json(json_data);
        let result = pipeline.execute(initial_data)?;
        
        // Verify custom pipeline
        assert!(result.success);
        assert_eq!(result.stage_results.len(), 2); // Only 2 stages
        assert_eq!(result.metrics.total_items_processed, 1);
        
        Ok(())
    }
    
    /// Test error handling in pipeline
    #[test]
    fn test_pipeline_error_handling() -> Result<()> {
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Create pipeline
        let pipeline = factory.create_for_source(&SourceType::JsonApi)?;
        
        // Test with empty data
        let empty_data = RawSourceData::Json(vec![]);
        let result = pipeline.execute_with_raw_data(empty_data)?;
        
        // Should handle empty data gracefully
        assert!(!result.success);
        assert_eq!(result.metrics.total_items_processed, 0);
        
        Ok(())
    }
    
    /// Test multiple source types in sequence
    #[test]
    fn test_multiple_source_types() -> Result<()> {
        let registry = RegistryFactory::create_default_registry();
        let factory = PipelineFactory::new(Arc::new(registry));
        
        // Test JSON API
        let json_pipeline = factory.create_for_source(&SourceType::JsonApi)?;
        let json_data = vec![json!({"name": "JSON Test", "price": "10.00"})];
        let json_result = json_pipeline.execute_with_raw_data(RawSourceData::Json(json_data))?;
        assert!(json_result.success);
        
        // Test HTML Scraping
        let html_pipeline = factory.create_for_source(&SourceType::HtmlScraping)?;
        let html_data = vec![ScrapedProduct {
            name: "HTML Test".to_string(),
            price: "$20.00".to_string(),
            product_id: "HTML001".to_string(),
            category: "Test".to_string(),
            url: Some("https://test.com".to_string()),
            raw_html: "<div>Test</div>".to_string(),
            category_path: None,
            page_name: None,
            page_number: None,
        }];
        let html_result = html_pipeline.execute_with_raw_data(RawSourceData::Html(html_data))?;
        assert!(html_result.success);
        
        // Test Pandamart
        let pandamart_pipeline = factory.create_for_source(&SourceType::Pandamart)?;
        let pandamart_data = vec![json!({
            "productID": "PM_TEST",
            "name": "Pandamart Test",
            "price": 30.0,
            "originalPrice": 40.0
        })];
        let pandamart_result = pandamart_pipeline.execute_with_raw_data(RawSourceData::Json(pandamart_data))?;
        assert!(pandamart_result.success);
        
        Ok(())
    }
    
    /// Test registry functionality
    #[test]
    fn test_registry_functionality() -> Result<()> {
        let registry = RegistryFactory::create_default_registry();
        
        // Check that all expected sources are registered
        assert!(registry.is_source_registered(&SourceType::JsonApi));
        assert!(registry.is_source_registered(&SourceType::HtmlScraping));
        assert!(registry.is_source_registered(&SourceType::Pandamart));
        assert!(registry.is_source_registered(&SourceType::Naheed));
        
        // Check transformers
        assert!(registry.get_transformer(&SourceType::HtmlScraping).is_some());
        assert!(registry.get_transformer(&SourceType::Pandamart).is_some());
        assert!(registry.get_transformer(&SourceType::JsonApi).is_some());
        
        // Test pipeline creation for each source
        for source_type in [SourceType::JsonApi, SourceType::HtmlScraping, SourceType::Pandamart] {
            let pipeline_result = registry.create_pipeline(&source_type);
            assert!(pipeline_result.is_ok(), "Failed to create pipeline for {:?}", source_type);
        }
        
        Ok(())
    }
}
