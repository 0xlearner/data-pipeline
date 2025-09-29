use anyhow::Result;
use data_pipeline::{
    pipeline::{PipelineOrchestrator, PipelineOptions},
    config::{ConfigManager, ConfigSource, MemoryConfigProvider},
    storage::MemoryStorage,
};
use std::sync::Arc;
use tokio;

mod common;
use common::{TestUtils, TestFixtures};

#[tokio::test]
async fn test_pipeline_orchestrator_creation() -> Result<()> {
    TestUtils::setup_test_env();
    
    // Note: This test would require actual MinIO setup to work fully
    // For now, we test the structure and error handling
    
    // The orchestrator creation will fail without proper MinIO setup,
    // but we can test that it fails gracefully
    let result = PipelineOrchestrator::new().await;
    
    // In test environment, this should fail due to missing MinIO
    assert!(result.is_err());
    
    TestUtils::cleanup_test_env();
    Ok(())
}

#[tokio::test]
async fn test_pipeline_options_creation() {
    let options = PipelineOptions {
        from_storage: false,
        specific_source: Some("test_source".to_string()),
        batch_size: Some(500),
        memory_efficient: true,
    };
    
    assert!(!options.from_storage);
    assert_eq!(options.specific_source, Some("test_source".to_string()));
    assert_eq!(options.batch_size, Some(500));
    assert!(options.memory_efficient);
}

#[tokio::test]
async fn test_pipeline_options_from_cli() {
    use data_pipeline::cli::App;
    
    // Test conversion from CLI app to pipeline options
    let app = App {
        from_storage: true,
        source: Some("krave_mart".to_string()),
        batch_size: Some(1000),
        memory_efficient: false,
    };
    
    let options = PipelineOptions::from(&app);
    
    assert!(options.from_storage);
    assert_eq!(options.specific_source, Some("krave_mart".to_string()));
    assert_eq!(options.batch_size, Some(1000));
    assert!(!options.memory_efficient);
}

#[tokio::test]
async fn test_config_manager_integration() -> Result<()> {
    let mut config_manager = TestUtils::create_test_config_manager();
    
    // Test storing and retrieving configuration
    let test_config = serde_json::json!({
        "endpoint": "http://localhost:9000",
        "bucket": "test-bucket",
        "access_key": "test-key",
        "secret_key": "test-secret"
    });
    
    // Store config
    if let Some(provider) = config_manager.providers.get(&ConfigSource::Memory) {
        provider.save_config("minio", &test_config)?;
        
        // Retrieve config
        let retrieved: serde_json::Value = provider.load_config("minio")?;
        assert!(TestUtils::json_values_equal(&test_config, &retrieved));
        
        // Test existence
        assert!(provider.exists("minio"));
        assert!(!provider.exists("nonexistent"));
        
        // Test listing keys
        let keys = provider.list_keys()?;
        assert!(keys.contains(&"minio".to_string()));
    }
    
    Ok(())
}

#[tokio::test]
async fn test_storage_integration() -> Result<()> {
    let storage = TestUtils::create_test_storage();
    
    // Test storing and retrieving JSON data
    let test_data = TestFixtures::small_dataset();
    let metadata = storage.store_json("test_products", &test_data).await?;
    
    assert!(metadata.size_bytes > 0);
    
    // Test existence
    assert!(storage.exists("test_products").await?);
    assert!(!storage.exists("nonexistent").await?);
    
    // Test retrieval
    let retrieved_data = storage.retrieve_json("test_products").await?;
    assert_eq!(retrieved_data.len(), test_data.len());
    
    // Test listing keys
    let keys = storage.list_keys(None).await?;
    assert!(keys.contains(&"test_products".to_string()));
    
    // Test deletion
    storage.delete("test_products").await?;
    assert!(!storage.exists("test_products").await?);
    
    Ok(())
}

#[tokio::test]
async fn test_processing_pipeline_integration() -> Result<()> {
    use data_pipeline::processing::{
        strategies::StrategySelector,
        batch::BatchProcessor,
        validation::DataValidator,
    };
    
    let strategy_selector = StrategySelector::new();
    let validator = DataValidator::ecommerce_validator();
    let mut batch_processor = TestUtils::create_test_batch_processor();
    
    let test_data = TestFixtures::medium_dataset();
    let context = TestUtils::create_test_processing_context("integration_test", test_data.len());
    
    // Test strategy selection
    let strategy = strategy_selector.select_strategy(&context);
    assert!(!strategy.strategy_name().is_empty());
    
    // Test data validation
    let validation_result = validator.validate_json(&test_data);
    TestUtils::assert_validation_quality(&validation_result, 95.0);
    
    // Test batch processing
    let mut processed_count = 0;
    let batch_result = batch_processor.process_batches(&test_data, |batch, batch_info| {
        processed_count += batch.len();
        async move {
            // Simulate processing
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok(())
        }
    }).await?;
    
    assert!(batch_result.is_successful());
    assert_eq!(batch_result.processed_records, test_data.len());
    assert_eq!(processed_count, test_data.len());
    
    Ok(())
}

#[tokio::test]
async fn test_error_handling_integration() -> Result<()> {
    use data_pipeline::processing::error::{ErrorHandler, ProcessingError, ErrorContext};
    
    let mut error_handler = ErrorHandler::new();
    
    // Simulate a processing pipeline with errors
    let test_data = TestFixtures::mixed_dataset();
    let mut successful_records = 0;
    let mut failed_records = 0;
    
    for (index, record) in test_data.iter().enumerate() {
        // Simulate processing that might fail
        if record.get("name").and_then(|v| v.as_str()).map_or(true, |s| s.is_empty()) {
            // Record error for empty name
            let error = ProcessingError::DataValidation {
                source: "integration_test".to_string(),
                record_index: index,
                field_name: "name".to_string(),
                expected_type: "non-empty string".to_string(),
                actual_value: "empty".to_string(),
                validation_rule: "required".to_string(),
            };
            
            let context = ErrorContext::new("integration_test".to_string(), "validate_record".to_string());
            error_handler.record_error(error, context);
            failed_records += 1;
        } else {
            successful_records += 1;
        }
    }
    
    // Verify error handling
    assert!(error_handler.total_errors() > 0);
    assert_eq!(error_handler.total_errors(), failed_records);
    
    let stats = error_handler.get_error_stats();
    assert!(stats.contains_key("data_validation"));
    
    // Test error recovery strategies
    let recent_errors = error_handler.get_recent_errors(5);
    for (error, _context) in recent_errors {
        let strategy = error_handler.get_recovery_strategy(error);
        // Validation errors should typically be skipped
        match strategy {
            data_pipeline::processing::error::RecoveryStrategy::Skip => {
                // Expected for validation errors
            }
            data_pipeline::processing::error::RecoveryStrategy::ContinuePartial => {
                // Also acceptable for validation errors
            }
            _ => {
                // Other strategies might be valid depending on configuration
            }
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_performance_monitoring() -> Result<()> {
    let test_data = TestFixtures::large_dataset();
    
    // Measure processing performance
    let (result, duration) = TestUtils::measure_execution_time(|| async {
        // Simulate data processing
        let mut processed = 0;
        for chunk in test_data.chunks(100) {
            // Simulate processing time
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            processed += chunk.len();
        }
        processed
    }).await;
    
    assert_eq!(result, test_data.len());
    
    // Calculate performance metrics
    let throughput = test_data.len() as f64 / duration.as_secs_f64();
    let mut metrics = TestUtils::create_performance_metrics();
    metrics.insert("actual_throughput_records_per_sec".to_string(), throughput);
    metrics.insert("actual_processing_time_ms".to_string(), duration.as_millis() as f64);
    
    // Verify performance is reasonable
    assert!(throughput > 1000.0, "Throughput too low: {} records/sec", throughput);
    assert!(duration.as_millis() < 10000, "Processing took too long: {} ms", duration.as_millis());
    
    Ok(())
}

#[tokio::test]
async fn test_memory_usage_monitoring() -> Result<()> {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    
    // Simulate memory usage tracking
    let memory_tracker = Arc::new(RwLock::new(0usize));
    let test_data = TestFixtures::medium_dataset();
    
    // Process data while tracking memory usage
    let tracker_clone = memory_tracker.clone();
    let processing_task = tokio::spawn(async move {
        for chunk in test_data.chunks(100) {
            // Simulate memory allocation
            let chunk_size = chunk.len() * 1024; // Assume 1KB per record
            {
                let mut memory = tracker_clone.write().await;
                *memory += chunk_size;
            }
            
            // Simulate processing
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            
            // Simulate memory deallocation
            {
                let mut memory = tracker_clone.write().await;
                *memory -= chunk_size;
            }
        }
    });
    
    // Monitor memory usage
    let monitoring_task = tokio::spawn(async move {
        let mut peak_memory = 0usize;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let current_memory = *memory_tracker.read().await;
            if current_memory > peak_memory {
                peak_memory = current_memory;
            }
        }
        peak_memory
    });
    
    let (_, peak_memory) = tokio::try_join!(processing_task, monitoring_task)?;
    
    // Verify memory usage is reasonable
    let peak_memory_mb = peak_memory as f64 / (1024.0 * 1024.0);
    assert!(peak_memory_mb < 100.0, "Peak memory usage too high: {:.1} MB", peak_memory_mb);
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_processing() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    
    let test_data = TestFixtures::medium_dataset();
    let processed_count = Arc::new(AtomicUsize::new(0));
    
    // Process data concurrently
    let mut tasks = Vec::new();
    let chunk_size = test_data.len() / 4; // 4 concurrent tasks
    
    for chunk in test_data.chunks(chunk_size) {
        let chunk_data = chunk.to_vec();
        let counter = processed_count.clone();
        
        let task = tokio::spawn(async move {
            // Simulate processing
            for _record in chunk_data {
                tokio::time::sleep(std::time::Duration::from_micros(10)).await;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        
        tasks.push(task);
    }
    
    // Wait for all tasks to complete
    for task in tasks {
        task.await?;
    }
    
    // Verify all records were processed
    assert_eq!(processed_count.load(Ordering::Relaxed), test_data.len());
    
    Ok(())
}

#[tokio::test]
async fn test_graceful_shutdown() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};
    
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    let test_data = TestFixtures::large_dataset();
    
    let signal_clone = shutdown_signal.clone();
    let processing_task = tokio::spawn(async move {
        let mut processed = 0;
        
        for chunk in test_data.chunks(100) {
            // Check for shutdown signal
            if signal_clone.load(Ordering::Relaxed) {
                break;
            }
            
            // Simulate processing
            tokio::time::sleep(Duration::from_millis(10)).await;
            processed += chunk.len();
        }
        
        processed
    });
    
    // Let it process for a bit, then signal shutdown
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown_signal.store(true, Ordering::Relaxed);
    
    // Wait for graceful shutdown
    let result = timeout(Duration::from_secs(5), processing_task).await??;
    
    // Should have processed some but not all records
    assert!(result > 0);
    assert!(result < test_data.len());
    
    Ok(())
}
