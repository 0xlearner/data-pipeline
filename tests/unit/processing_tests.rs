use anyhow::Result;
use data_pipeline::processing::{
    strategies::{StandardProcessingStrategy, MemoryEfficientStrategy, StreamingStrategy, StrategySelector},
    batch::{BatchProcessor, BatchConfig},
    validation::{DataValidator, ValidationRule, DataType},
    error::{ErrorHandler, ProcessingError, ErrorContext},
    ProcessingContext, ProcessingResult
};
use serde_json::Value;

mod common;
use common::{TestUtils, TestFixtures};

#[tokio::test]
async fn test_standard_processing_strategy() {
    let strategy = StandardProcessingStrategy::new();
    let data = TestFixtures::small_dataset();
    let context = TestUtils::create_test_processing_context("test_source", data.len());
    
    // Note: This test would need actual processor components to work fully
    // For now, we test the strategy selection logic
    assert_eq!(strategy.strategy_name(), "standard");
    assert!(strategy.is_suitable(&context));
    
    let memory_estimate = strategy.estimate_memory_usage(&context);
    assert!(memory_estimate > 0);
    assert!(memory_estimate < 1024 * 1024); // Should be reasonable for small dataset
}

#[tokio::test]
async fn test_memory_efficient_strategy() {
    let strategy = MemoryEfficientStrategy::new(500, 256);
    let data = TestFixtures::medium_dataset();
    let context = TestUtils::create_test_processing_context("test_source", data.len());
    
    assert_eq!(strategy.strategy_name(), "memory_efficient");
    assert!(strategy.is_suitable(&context));
    
    let memory_estimate = strategy.estimate_memory_usage(&context);
    assert!(memory_estimate > 0);
    assert!(memory_estimate <= 500 * 1024); // Should respect batch size limit
}

#[tokio::test]
async fn test_streaming_strategy() {
    let strategy = StreamingStrategy::new(100, 1024);
    let data = TestFixtures::large_dataset();
    let context = TestUtils::create_test_processing_context("test_source", data.len());
    
    assert_eq!(strategy.strategy_name(), "streaming");
    assert!(strategy.is_suitable(&context));
    
    let memory_estimate = strategy.estimate_memory_usage(&context);
    assert!(memory_estimate > 0);
    assert!(memory_estimate <= 100 * 512); // Should be very memory efficient
}

#[tokio::test]
async fn test_strategy_selector() {
    let selector = StrategySelector::new();
    
    // Test small dataset - should select standard strategy
    let small_context = TestUtils::create_test_processing_context("small", 100);
    let strategy = selector.select_strategy(&small_context);
    assert_eq!(strategy.strategy_name(), "standard");
    
    // Test medium dataset - should select memory efficient strategy
    let medium_context = ProcessingContext {
        source_name: "medium".to_string(),
        total_records: 5000,
        memory_limit_mb: Some(256),
        batch_size: Some(1000),
        enable_validation: true,
        enable_metrics: true,
    };
    let strategy = selector.select_strategy(&medium_context);
    assert_eq!(strategy.strategy_name(), "memory_efficient");
    
    // Test large dataset - should select streaming strategy
    let large_context = ProcessingContext {
        source_name: "large".to_string(),
        total_records: 100000,
        memory_limit_mb: Some(128),
        batch_size: Some(100),
        enable_validation: true,
        enable_metrics: true,
    };
    let strategy = selector.select_strategy(&large_context);
    assert_eq!(strategy.strategy_name(), "streaming");
}

#[tokio::test]
async fn test_batch_processor_creation() {
    let processor = TestUtils::create_test_batch_processor();
    let config = processor.config();
    
    assert_eq!(config.batch_size, 50);
    assert_eq!(config.max_memory_mb, Some(256));
    assert!(config.retry_failed_batches);
    assert_eq!(config.max_retries, 2);
}

#[tokio::test]
async fn test_batch_config_auto_adjust() {
    let mut config = BatchConfig::default();
    
    // Test auto-adjustment for large records
    config.auto_adjust(1000, 10 * 1024); // 10KB per record
    assert!(config.batch_size <= 1000); // Should be adjusted down
    
    // Test auto-adjustment for small dataset
    let mut small_config = BatchConfig::default();
    small_config.auto_adjust(50, 1024); // Small dataset
    assert_eq!(small_config.batch_size, 50); // Should match dataset size
}

#[tokio::test]
async fn test_batch_processor_create_batches() {
    let processor = TestUtils::create_test_batch_processor();
    let data = TestFixtures::medium_dataset();
    
    let batches = processor.create_batches(&data);
    
    assert!(!batches.is_empty());
    assert_eq!(batches.len(), (data.len() + 49) / 50); // Ceiling division for batch size 50
    
    // Check first batch
    let first_batch = &batches[0];
    assert_eq!(first_batch.batch_id, 1);
    assert_eq!(first_batch.start_index, 0);
    assert_eq!(first_batch.record_count, 50.min(data.len()));
    assert!(first_batch.estimated_memory_mb > 0.0);
    
    // Check last batch
    let last_batch = &batches[batches.len() - 1];
    assert_eq!(last_batch.batch_id, batches.len());
    assert!(last_batch.record_count <= 50);
    assert!(last_batch.record_count > 0);
}

#[tokio::test]
async fn test_data_validator_ecommerce() {
    let validator = DataValidator::ecommerce_validator();
    let valid_data = TestFixtures::small_dataset();
    
    let result = validator.validate_json(&valid_data);
    
    assert!(result.is_valid);
    assert_eq!(result.total_records, valid_data.len());
    assert_eq!(result.valid_records, valid_data.len());
    assert_eq!(result.invalid_records, 0);
    assert!(result.quality_score >= 95.0);
    assert!(result.errors.is_empty());
}

#[tokio::test]
async fn test_data_validator_with_invalid_data() {
    let validator = DataValidator::ecommerce_validator();
    let invalid_data = TestFixtures::invalid_dataset();
    
    let result = validator.validate_json(&invalid_data);
    
    assert!(!result.is_valid);
    assert_eq!(result.total_records, invalid_data.len());
    assert!(result.invalid_records > 0);
    assert!(result.quality_score < 100.0);
    assert!(!result.errors.is_empty());
    
    // Check that we have validation errors
    let has_required_errors = result.errors.iter().any(|e| {
        matches!(e.rule, ValidationRule::Required { .. })
    });
    assert!(has_required_errors, "Should have required field validation errors");
}

#[tokio::test]
async fn test_data_validator_custom_rules() {
    let mut validator = DataValidator::new();
    
    // Add custom validation rules
    validator.add_rule(ValidationRule::Required { field: "custom_field".to_string() });
    validator.add_rule(ValidationRule::Range { 
        field: "price".to_string(), 
        min: 0.0, 
        max: 1000.0 
    });
    
    let test_data = vec![
        serde_json::json!({
            "custom_field": "value",
            "price": 50.0
        }),
        serde_json::json!({
            "price": 1500.0  // This should fail range validation
        })
    ];
    
    let result = validator.validate_json(&test_data);
    
    assert!(!result.is_valid);
    assert_eq!(result.total_records, 2);
    assert_eq!(result.valid_records, 1);
    assert_eq!(result.invalid_records, 1);
    assert!(!result.errors.is_empty());
}

#[tokio::test]
async fn test_error_handler() {
    let mut error_handler = TestUtils::create_test_error_handler();
    
    // Record some errors
    let error1 = ProcessingError::DataFetch {
        source: "test_source".to_string(),
        url: Some("http://example.com".to_string()),
        status_code: Some(404),
        message: "Not found".to_string(),
        retry_count: 1,
    };
    
    let context1 = ErrorContext::new("test_source".to_string(), "fetch_data".to_string());
    error_handler.record_error(error1, context1);
    
    let error2 = ProcessingError::DataValidation {
        source: "test_source".to_string(),
        record_index: 5,
        field_name: "price".to_string(),
        expected_type: "number".to_string(),
        actual_value: "invalid".to_string(),
        validation_rule: "data_type".to_string(),
    };
    
    let context2 = ErrorContext::new("test_source".to_string(), "validate_data".to_string());
    error_handler.record_error(error2, context2);
    
    // Check error statistics
    assert_eq!(error_handler.total_errors(), 2);
    
    let stats = error_handler.get_error_stats();
    assert!(stats.contains_key("data_fetch"));
    assert!(stats.contains_key("data_validation"));
    
    // Check recent errors
    let recent = error_handler.get_recent_errors(5);
    assert_eq!(recent.len(), 2);
    
    // Generate report
    let report = error_handler.generate_report();
    assert!(report.contains("Total errors: 2"));
    assert!(report.contains("data_fetch"));
    assert!(report.contains("data_validation"));
}

#[tokio::test]
async fn test_error_recovery_strategies() {
    let error_handler = TestUtils::create_test_error_handler();
    
    // Test different error types and their recovery strategies
    let fetch_error = ProcessingError::DataFetch {
        source: "test".to_string(),
        url: None,
        status_code: Some(500),
        message: "Server error".to_string(),
        retry_count: 0,
    };
    
    let strategy = error_handler.get_recovery_strategy(&fetch_error);
    match strategy {
        data_pipeline::processing::error::RecoveryStrategy::Retry { max_attempts, .. } => {
            assert!(max_attempts > 0);
        }
        _ => panic!("Expected retry strategy for fetch error"),
    }
    
    // Test if error is recoverable
    assert!(error_handler.is_recoverable(&fetch_error));
    
    let config_error = ProcessingError::Configuration {
        source: "test".to_string(),
        message: "Invalid config".to_string(),
        config_path: None,
    };
    
    assert!(!error_handler.is_recoverable(&config_error));
}

#[tokio::test]
async fn test_processing_context_creation() {
    let context = TestUtils::create_test_processing_context("test_source", 1000);
    
    assert_eq!(context.source_name, "test_source");
    assert_eq!(context.total_records, 1000);
    assert_eq!(context.memory_limit_mb, Some(512));
    assert_eq!(context.batch_size, Some(100));
    assert!(context.enable_validation);
    assert!(context.enable_metrics);
}

#[tokio::test]
async fn test_validation_result_methods() {
    let validator = DataValidator::ecommerce_validator();
    let mixed_data = TestFixtures::mixed_dataset();
    
    let result = validator.validate_json(&mixed_data);
    
    // Test result methods
    let success_rate = result.record_success_rate();
    assert!(success_rate >= 0.0 && success_rate <= 100.0);
    
    let critical_errors = result.critical_errors();
    // Should not have critical errors in our test data
    assert!(critical_errors.is_empty());
    
    let report = result.generate_report();
    assert!(report.contains("Data Validation Report"));
    assert!(report.contains("Total records:"));
    assert!(report.contains("Quality score:"));
}

#[tokio::test]
async fn test_batch_result_methods() {
    use data_pipeline::processing::batch::BatchResult;
    use std::time::Duration;
    
    let result = BatchResult {
        total_batches: 10,
        successful_batches: 8,
        failed_batches: 2,
        total_records: 1000,
        processed_records: 800,
        failed_records: 200,
        total_processing_time: Duration::from_secs(30),
        average_batch_time: Duration::from_secs(3),
        memory_peak_mb: 256.0,
        throughput_records_per_sec: 26.67,
    };
    
    assert!(!result.is_successful()); // Has failed batches
    assert_eq!(result.success_rate(), 80.0);
    assert_eq!(result.record_success_rate(), 80.0);
    
    let summary = result.performance_summary();
    assert!(summary.contains("8/10"));
    assert!(summary.contains("800/1000"));
    assert!(summary.contains("26.7"));
}
