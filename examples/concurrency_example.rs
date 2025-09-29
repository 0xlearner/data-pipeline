//! Example demonstrating the global concurrency configuration system
//!
//! This example shows how to:
//! - Initialize global configuration
//! - Use the HTTP client factory for concurrent requests
//! - Configure source-specific overrides
//! - Monitor system health and statistics
//! - Manage rate limiting and retry behavior

use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use data_pipeline::config::{
    ConcurrencyConfig, ConfigManager, GlobalRateLimitConfig, GlobalRetryConfig,
    SourceOverride,
};
use data_pipeline::config::concurrency_config::{DomainRateLimit, RateLimitStrategy};
use data_pipeline::fetcher::HttpFetcher;
use data_pipeline::http::GlobalHttpClientFactory;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("🚀 Starting Global Concurrency Configuration Example");

    // Example 1: Basic configuration initialization
    basic_configuration_example().await?;

    // Example 2: Using the HTTP client factory
    http_client_factory_example().await?;

    // Example 3: Source-specific configuration
    source_specific_example().await?;

    // Example 4: Concurrent batch operations
    concurrent_batch_example().await?;

    // Example 5: Monitoring and health checks
    monitoring_example().await?;

    // Example 6: Dynamic configuration updates
    dynamic_configuration_example().await?;

    info!("✅ All examples completed successfully!");
    Ok(())
}

/// Example 1: Basic configuration initialization and inspection
async fn basic_configuration_example() -> Result<()> {
    info!("📋 Example 1: Basic Configuration");

    // Get the global configuration manager
    let config_manager = ConfigManager::global().await;

    // Show current configuration statistics
    let stats = config_manager.get_stats();
    info!("Configuration loaded from: {}", stats.config_path);
    info!("Max concurrent requests: {}", stats.max_concurrent_requests);
    info!("Global rate limit: {:.1} req/s", stats.global_rate_limit);
    info!("Sources with overrides: {}", stats.sources_with_overrides);

    // Validate the configuration
    match config_manager.validate() {
        Ok(()) => info!("✅ Configuration is valid"),
        Err(e) => error!("❌ Configuration validation failed: {}", e),
    }

    Ok(())
}

/// Example 2: Using the HTTP client factory for making requests
async fn http_client_factory_example() -> Result<()> {
    info!("🌐 Example 2: HTTP Client Factory Usage");

    // Get the global HTTP client factory
    let factory = GlobalHttpClientFactory::instance().await?;

    // Example URLs for testing (replace with actual test endpoints)
    let test_urls = vec![
        "https://httpbin.org/delay/1",
        "https://httpbin.org/json",
        "https://jsonplaceholder.typicode.com/posts/1",
    ];

    info!("Making concurrent requests with global configuration...");

    // Make concurrent requests using the factory
    let mut handles = Vec::new();
    for (i, url) in test_urls.iter().enumerate() {
        let factory = factory;
        let url = url.to_string();

        let handle = tokio::spawn(async move {
            let source_name = format!("test_source_{}", i);
            match factory.get_json(&source_name, &url).await {
                Ok(data) => {
                    info!(
                        "✅ Request {} succeeded: {} bytes",
                        i,
                        serde_json::to_string(&data).unwrap_or_default().len()
                    );
                    Ok(())
                }
                Err(e) => {
                    warn!("❌ Request {} failed: {}", i, e);
                    Err(e)
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => info!("Request {} completed successfully", i),
            Ok(Err(e)) => warn!("Request {} failed: {}", i, e),
            Err(e) => error!("Request {} panicked: {}", i, e),
        }
    }

    Ok(())
}

/// Example 3: Source-specific configuration and overrides
async fn source_specific_example() -> Result<()> {
    info!("🎯 Example 3: Source-Specific Configuration");

    let config_manager = ConfigManager::global().await;

    // Create a conservative override for a sensitive API
    let conservative_override = SourceOverride {
        retry: Some(GlobalRetryConfig {
            max_attempts: 2,
            base_delay_ms: 1000,
            max_delay_seconds: 10,
            backoff_multiplier: 1.5,
            jitter: true,
            retryable_status_codes: vec![429, 500, 502, 503, 504],
            retry_on_connection_error: true,
            retry_on_timeout: true,
        }),
        rate_limit: Some(GlobalRateLimitConfig {
            enabled: true,
            requests_per_second: 2.0,
            burst_size: 1,
            strategy: RateLimitStrategy::TokenBucket,
            domain_limits: std::collections::HashMap::new(),
        }),
        http: None,
        emulation: None,
        concurrency: None,
    };

    // Apply the override for a test source
    config_manager.update_source_override("conservative_api", conservative_override)?;
    info!("✅ Applied conservative configuration for 'conservative_api'");

    // Create an aggressive override for a robust API
    let aggressive_override = SourceOverride {
        retry: Some(GlobalRetryConfig {
            max_attempts: 5,
            base_delay_ms: 200,
            max_delay_seconds: 60,
            backoff_multiplier: 2.0,
            jitter: true,
            retryable_status_codes: vec![408, 429, 500, 502, 503, 504],
            retry_on_connection_error: true,
            retry_on_timeout: true,
        }),
        rate_limit: Some(GlobalRateLimitConfig {
            enabled: true,
            requests_per_second: 15.0,
            burst_size: 8,
            strategy: RateLimitStrategy::TokenBucket,
            domain_limits: std::collections::HashMap::new(),
        }),
        http: None,
        emulation: None,
        concurrency: None,
    };

    config_manager.update_source_override("robust_api", aggressive_override)?;
    info!("✅ Applied aggressive configuration for 'robust_api'");

    // Test both configurations
    let factory = GlobalHttpClientFactory::instance().await?;
    let test_url = "https://httpbin.org/delay/0.5";

    // Test conservative configuration
    info!("Testing conservative API configuration...");
    match factory.get_json("conservative_api", test_url).await {
        Ok(_) => info!("✅ Conservative API request succeeded"),
        Err(e) => warn!("❌ Conservative API request failed: {}", e),
    }

    // Test aggressive configuration
    info!("Testing aggressive API configuration...");
    match factory.get_json("robust_api", test_url).await {
        Ok(_) => info!("✅ Aggressive API request succeeded"),
        Err(e) => warn!("❌ Aggressive API request failed: {}", e),
    }

    Ok(())
}

/// Example 4: Concurrent batch operations with proper limiting
async fn concurrent_batch_example() -> Result<()> {
    info!("🔄 Example 4: Concurrent Batch Operations");

    // Create multiple HttpFetcher instances for different sources
    let api_fetcher = HttpFetcher::new_for_source("batch_api").await?;
    let scraper_fetcher = HttpFetcher::new_for_source("batch_scraper").await?;

    // Prepare test URLs
    let api_urls = vec![
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3",
        "https://jsonplaceholder.typicode.com/posts/4",
        "https://jsonplaceholder.typicode.com/posts/5",
    ];

    let scraping_urls = vec![
        "https://httpbin.org/json",
        "https://httpbin.org/headers",
        "https://httpbin.org/user-agent",
    ];

    info!("Starting batch operations...");

    // Start API batch operation
    let api_handle = tokio::spawn(async move {
        info!("🔗 Starting API batch fetch...");
        let url_refs: Vec<&str> = api_urls.iter().map(|s| s.as_ref()).collect();
        let results = api_fetcher.get_json_batch(url_refs).await;

        let successful = results.iter().filter(|(_, result)| result.is_ok()).count();
        let failed = results.len() - successful;

        info!(
            "📊 API batch completed: {} successful, {} failed",
            successful, failed
        );
        results
    });

    // Start scraping batch operation
    let scraper_handle = tokio::spawn(async move {
        info!("🕷️  Starting scraper batch fetch...");
        let url_refs: Vec<&str> = scraping_urls.iter().map(|s| s.as_ref()).collect();
        let results = scraper_fetcher.get_json_batch(url_refs).await;

        let successful = results.iter().filter(|(_, result)| result.is_ok()).count();
        let failed = results.len() - successful;

        info!(
            "📊 Scraper batch completed: {} successful, {} failed",
            successful, failed
        );
        results
    });

    // Wait for both batches to complete
    let (api_results, scraper_results) = tokio::try_join!(api_handle, scraper_handle)?;

    info!("✅ All batch operations completed");
    info!("API results: {} total", api_results.len());
    info!("Scraper results: {} total", scraper_results.len());

    Ok(())
}

/// Example 5: System monitoring and health checks
async fn monitoring_example() -> Result<()> {
    info!("📊 Example 5: System Monitoring");

    let factory = GlobalHttpClientFactory::instance().await?;

    // Check system health
    let health = factory.health_check();
    info!("System Health: {:?}", health.status);
    info!(
        "Global Utilization: {:.1}%",
        health.global_utilization * 100.0
    );

    // Get concurrency statistics
    let stats = factory.get_concurrency_stats();
    info!(
        "Available Global Permits: {}/{}",
        stats.global_available_permits, stats.global_total_permits
    );
    info!("Cached HTTP Clients: {}", stats.cached_clients);

    // Show domain-specific statistics
    if !stats.domain_stats.is_empty() {
        info!("Domain Statistics:");
        for domain_stat in &stats.domain_stats {
            info!(
                "  {}: {}/{} permits available",
                domain_stat.domain, domain_stat.available_permits, domain_stat.total_permits
            );
        }
    }

    // Monitor system under load
    info!("Testing system under load...");
    let mut handles = Vec::new();

    for i in 0..20 {
        let factory_ref = factory;
        let handle = tokio::spawn(async move {
            let source_name = format!("load_test_{}", i);
            factory_ref
                .get_json(&source_name, "https://httpbin.org/delay/0.1")
                .await
        });
        handles.push(handle);
    }

    // Monitor health during load
    tokio::spawn(async move {
        for _ in 0..5 {
            sleep(Duration::from_millis(200)).await;
            let health = factory.health_check();
            info!(
                "During load - Health: {:?}, Utilization: {:.1}%",
                health.status,
                health.global_utilization * 100.0
            );
        }
    });

    // Wait for all load test requests
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await? {
            Ok(_) => info!("Load test {} completed", i),
            Err(e) => warn!("Load test {} failed: {}", i, e),
        }
    }

    // Final health check
    let final_health = factory.health_check();
    info!("Final Health: {:?}", final_health.status);

    Ok(())
}

/// Example 6: Dynamic configuration updates
async fn dynamic_configuration_example() -> Result<()> {
    info!("⚡ Example 6: Dynamic Configuration Updates");

    let config_manager = ConfigManager::global().await;

    // Show current global rate limit
    let current_config = config_manager.get_config();
    info!(
        "Current global rate limit: {:.1} req/s",
        current_config.rate_limit.requests_per_second
    );

    // Update global configuration
    let mut new_config = current_config.clone();
    new_config.rate_limit.requests_per_second = 20.0;
    new_config.rate_limit.burst_size = 10;

    config_manager.update_config(new_config)?;
    info!("✅ Updated global rate limit to 20.0 req/s");

    // Add domain-specific rate limit
    let mut config = config_manager.get_config();
    let domain_limit = DomainRateLimit {
        requests_per_second: 5.0,
        burst_size: 2,
        enabled: true,
    };

    config
        .rate_limit
        .domain_limits
        .insert("httpbin.org".to_string(), domain_limit);

    config_manager.update_config(config)?;
    info!("✅ Added domain-specific rate limit for httpbin.org: 5.0 req/s");

    // Test the updated configuration
    let factory = GlobalHttpClientFactory::instance().await?;

    // This should respect the domain-specific limit
    match factory
        .get_json("test_domain", "https://httpbin.org/json")
        .await
    {
        Ok(_) => info!("✅ Request with domain-specific limit succeeded"),
        Err(e) => warn!("❌ Request with domain-specific limit failed: {}", e),
    }

    // Reload configuration to demonstrate hot reloading
    info!("Demonstrating configuration reload...");
    factory.reload_configuration().await?;
    info!("✅ Configuration reloaded successfully");

    // Show final configuration stats
    let final_stats = config_manager.get_stats();
    info!("Final configuration:");
    info!(
        "  Max concurrent requests: {}",
        final_stats.max_concurrent_requests
    );
    info!(
        "  Global rate limit: {:.1} req/s",
        final_stats.global_rate_limit
    );
    info!("  Domain rate limits: {}", final_stats.domain_rate_limits);
    info!(
        "  Sources with overrides: {}",
        final_stats.sources_with_overrides
    );

    Ok(())
}

/// Helper function to demonstrate error handling and retry behavior
#[allow(dead_code)]
async fn demonstrate_retry_behavior() -> Result<()> {
    info!("🔄 Demonstrating Retry Behavior");

    let factory = GlobalHttpClientFactory::instance().await?;

    // Try to request a URL that will return various HTTP errors
    let error_urls = vec![
        ("404 Error", "https://httpbin.org/status/404"),
        ("500 Error", "https://httpbin.org/status/500"),
        ("502 Error", "https://httpbin.org/status/502"),
        ("Timeout", "https://httpbin.org/delay/35"), // Should timeout
    ];

    for (description, url) in error_urls {
        info!("Testing {} with URL: {}", description, url);

        let start = std::time::Instant::now();
        match factory.get_json("retry_test", url).await {
            Ok(_) => info!("✅ {} request succeeded", description),
            Err(e) => {
                let duration = start.elapsed();
                warn!(
                    "❌ {} request failed after {:?}: {}",
                    description, duration, e
                );
            }
        }
    }

    Ok(())
}

/// Utility function to show configuration in a readable format
#[allow(dead_code)]
fn display_configuration_summary(config: &ConcurrencyConfig) {
    info!("📋 Configuration Summary:");
    info!("  HTTP Timeout: {}s", config.http.timeout_seconds);
    info!(
        "  Max Concurrent Requests: {}",
        config.concurrency.max_concurrent_requests
    );
    info!(
        "  Rate Limiting: {} ({:.1} req/s)",
        if config.rate_limit.enabled {
            "Enabled"
        } else {
            "Disabled"
        },
        config.rate_limit.requests_per_second
    );
    info!("  Retry Attempts: {}", config.retry.max_attempts);
    info!("  Browser Emulation: {:?}", config.emulation.strategy);
    info!("  Source Overrides: {}", config.source_overrides.len());
    info!(
        "  Domain Rate Limits: {}",
        config.rate_limit.domain_limits.len()
    );
}
