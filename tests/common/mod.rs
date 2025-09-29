use anyhow::Result;
use data_pipeline::config::{ConfigManager, ConfigSource, MemoryConfigProvider};
use data_pipeline::processing::{
    ProcessingStrategy, ProcessingContext, StrategySelector,
    BatchProcessor, BatchConfig, DataValidator, ErrorHandler
};
use data_pipeline::storage::{MemoryStorage, Storage};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Test utilities and common setup functions
pub struct TestUtils;

impl TestUtils {
    /// Create test JSON data for e-commerce products
    pub fn create_test_products(count: usize) -> Vec<Value> {
        (0..count)
            .map(|i| {
                json!({
                    "id": format!("product_{}", i),
                    "name": format!("Test Product {}", i),
                    "price": 10.0 + (i as f64 * 5.0),
                    "category": if i % 3 == 0 { "Electronics" } else if i % 3 == 1 { "Clothing" } else { "Home" },
                    "description": format!("Description for product {}", i),
                    "in_stock": i % 4 != 0,
                    "rating": 3.5 + (i % 3) as f64 * 0.5,
                    "brand": format!("Brand {}", i % 5),
                    "sku": format!("SKU-{:04}", i),
                    "weight": format!("{:.1}kg", 0.5 + (i as f64 * 0.1)),
                    "dimensions": {
                        "length": 10 + (i % 10),
                        "width": 8 + (i % 8),
                        "height": 5 + (i % 5)
                    },
                    "tags": ["tag1", "tag2", "tag3"],
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z"
                })
            })
            .collect()
    }

    /// Create test data with validation errors
    pub fn create_invalid_test_products(count: usize) -> Vec<Value> {
        (0..count)
            .map(|i| {
                json!({
                    "id": if i % 3 == 0 { Value::Null } else { json!(format!("product_{}", i)) },
                    "name": if i % 4 == 0 { "" } else { format!("Test Product {}", i) },
                    "price": if i % 5 == 0 { "invalid_price" } else { json!(10.0 + (i as f64 * 5.0)) },
                    "category": format!("Category {}", i % 3),
                    "description": format!("Description for product {}", i),
                    "in_stock": i % 4 != 0,
                    "rating": if i % 6 == 0 { 10.0 } else { 3.5 + (i % 3) as f64 * 0.5 }, // Invalid rating > 5
                })
            })
            .collect()
    }

    /// Create a test storage instance
    pub fn create_test_storage() -> Arc<dyn Storage> {
        Arc::new(MemoryStorage::new())
    }

    /// Create a test configuration manager
    pub fn create_test_config_manager() -> ConfigManager {
        let mut manager = ConfigManager::new(ConfigSource::Memory);
        manager.add_provider(ConfigSource::Memory, Box::new(MemoryConfigProvider::new()));
        manager
    }

    /// Create a test processing context
    pub fn create_test_processing_context(source_name: &str, record_count: usize) -> ProcessingContext {
        ProcessingContext {
            source_name: source_name.to_string(),
            total_records: record_count,
            memory_limit_mb: Some(512),
            batch_size: Some(100),
            enable_validation: true,
            enable_metrics: true,
        }
    }

    /// Create a test batch processor
    pub fn create_test_batch_processor() -> BatchProcessor {
        let config = BatchConfig {
            batch_size: 50,
            max_memory_mb: Some(256),
            max_processing_time: Some(std::time::Duration::from_secs(30)),
            retry_failed_batches: true,
            max_retries: 2,
            parallel_batches: false,
            max_parallel: 1,
        };
        BatchProcessor::new(config)
    }

    /// Create a test error handler
    pub fn create_test_error_handler() -> ErrorHandler {
        ErrorHandler::new()
    }

    /// Create a test data validator
    pub fn create_test_validator() -> DataValidator {
        DataValidator::ecommerce_validator()
    }

    /// Assert that processing result is successful
    pub fn assert_processing_success(result: &data_pipeline::processing::ProcessingResult) {
        assert!(result.processed_records > 0, "No records were processed");
        assert_eq!(result.failed_records, 0, "Some records failed processing");
        assert!(result.processing_time_ms > 0, "Processing time should be greater than 0");
        assert!(result.output_size_bytes > 0, "Output size should be greater than 0");
    }

    /// Assert that validation result meets quality standards
    pub fn assert_validation_quality(result: &data_pipeline::processing::ValidationResult, min_quality: f64) {
        assert!(result.quality_score >= min_quality, 
                "Quality score {:.1}% is below minimum {:.1}%", 
                result.quality_score, min_quality);
        assert!(result.valid_records > 0, "No valid records found");
    }

    /// Create test environment variables
    pub fn setup_test_env() {
        std::env::set_var("TEST_MODE", "true");
        std::env::set_var("LOG_LEVEL", "debug");
        std::env::set_var("MINIO_ENDPOINT", "http://localhost:9000");
        std::env::set_var("MINIO_BUCKET", "test-bucket");
        std::env::set_var("MINIO_ACCESS_KEY", "test-access-key");
        std::env::set_var("MINIO_SECRET_KEY", "test-secret-key");
    }

    /// Clean up test environment
    pub fn cleanup_test_env() {
        std::env::remove_var("TEST_MODE");
        std::env::remove_var("LOG_LEVEL");
        std::env::remove_var("MINIO_ENDPOINT");
        std::env::remove_var("MINIO_BUCKET");
        std::env::remove_var("MINIO_ACCESS_KEY");
        std::env::remove_var("MINIO_SECRET_KEY");
    }

    /// Wait for async operations to complete
    pub async fn wait_for_completion(duration_ms: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
    }

    /// Generate random test data
    pub fn generate_random_products(count: usize, seed: u64) -> Vec<Value> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        let mut rng_state = hasher.finish();
        
        (0..count)
            .map(|i| {
                // Simple LCG for deterministic "random" values
                rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
                let price = 5.0 + ((rng_state % 1000) as f64 / 10.0);
                let rating = 1.0 + ((rng_state % 50) as f64 / 10.0);
                
                json!({
                    "id": format!("rand_product_{}", i),
                    "name": format!("Random Product {}", i),
                    "price": price,
                    "category": ["Electronics", "Clothing", "Home", "Books", "Sports"][rng_state as usize % 5],
                    "rating": rating,
                    "in_stock": (rng_state % 4) != 0,
                    "brand": format!("Brand {}", rng_state % 10),
                    "sku": format!("RND-{:06}", rng_state % 1000000),
                })
            })
            .collect()
    }

    /// Compare two JSON values for testing
    pub fn json_values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Object(a_obj), Value::Object(b_obj)) => {
                if a_obj.len() != b_obj.len() {
                    return false;
                }
                for (key, a_val) in a_obj {
                    if let Some(b_val) = b_obj.get(key) {
                        if !Self::json_values_equal(a_val, b_val) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            (Value::Array(a_arr), Value::Array(b_arr)) => {
                if a_arr.len() != b_arr.len() {
                    return false;
                }
                a_arr.iter().zip(b_arr.iter()).all(|(a_val, b_val)| Self::json_values_equal(a_val, b_val))
            }
            _ => a == b,
        }
    }

    /// Create a mock HTTP response for testing
    pub fn create_mock_response(status: u16, body: &str) -> Result<String> {
        Ok(format!(
            "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            status,
            body.len(),
            body
        ))
    }

    /// Measure execution time of an async function
    pub async fn measure_execution_time<F, Fut, T>(f: F) -> (T, std::time::Duration)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = std::time::Instant::now();
        let result = f().await;
        let duration = start.elapsed();
        (result, duration)
    }

    /// Create test metrics for performance testing
    pub fn create_performance_metrics() -> HashMap<String, f64> {
        let mut metrics = HashMap::new();
        metrics.insert("throughput_records_per_sec".to_string(), 1000.0);
        metrics.insert("memory_usage_mb".to_string(), 256.0);
        metrics.insert("cpu_usage_percent".to_string(), 45.0);
        metrics.insert("error_rate_percent".to_string(), 0.1);
        metrics.insert("average_response_time_ms".to_string(), 150.0);
        metrics
    }

    /// Assert performance metrics meet requirements
    pub fn assert_performance_requirements(metrics: &HashMap<String, f64>) {
        if let Some(throughput) = metrics.get("throughput_records_per_sec") {
            assert!(*throughput >= 100.0, "Throughput too low: {} records/sec", throughput);
        }
        
        if let Some(memory) = metrics.get("memory_usage_mb") {
            assert!(*memory <= 1024.0, "Memory usage too high: {} MB", memory);
        }
        
        if let Some(error_rate) = metrics.get("error_rate_percent") {
            assert!(*error_rate <= 5.0, "Error rate too high: {}%", error_rate);
        }
        
        if let Some(response_time) = metrics.get("average_response_time_ms") {
            assert!(*response_time <= 5000.0, "Response time too high: {} ms", response_time);
        }
    }
}

/// Test fixtures for common test scenarios
pub struct TestFixtures;

impl TestFixtures {
    /// Small dataset for unit tests
    pub fn small_dataset() -> Vec<Value> {
        TestUtils::create_test_products(10)
    }

    /// Medium dataset for integration tests
    pub fn medium_dataset() -> Vec<Value> {
        TestUtils::create_test_products(1000)
    }

    /// Large dataset for performance tests
    pub fn large_dataset() -> Vec<Value> {
        TestUtils::create_test_products(10000)
    }

    /// Dataset with validation errors
    pub fn invalid_dataset() -> Vec<Value> {
        TestUtils::create_invalid_test_products(100)
    }

    /// Mixed dataset (valid and invalid records)
    pub fn mixed_dataset() -> Vec<Value> {
        let mut data = TestUtils::create_test_products(80);
        data.extend(TestUtils::create_invalid_test_products(20));
        data
    }

    /// Empty dataset
    pub fn empty_dataset() -> Vec<Value> {
        Vec::new()
    }
}

/// Async test helper macros
#[macro_export]
macro_rules! async_test {
    ($test_name:ident, $test_body:expr) => {
        #[tokio::test]
        async fn $test_name() {
            $test_body
        }
    };
}

#[macro_export]
macro_rules! async_test_with_setup {
    ($test_name:ident, $setup:expr, $test_body:expr) => {
        #[tokio::test]
        async fn $test_name() {
            $setup;
            $test_body;
            TestUtils::cleanup_test_env();
        }
    };
}
