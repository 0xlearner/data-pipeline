use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::http::{GlobalHttpClientFactory, HttpClientFactory};

/// Pure HTTP fetcher that only handles network operations using global configuration
/// No data processing or extraction logic - delegates to the global HTTP client factory
pub struct HttpFetcher {
    factory: &'static HttpClientFactory,
    source_name: String,
    custom_headers: HashMap<String, String>,
}

impl HttpFetcher {
    /// Create a new HTTP fetcher with API-optimized configuration for a specific source
    pub async fn new_for_source(source_name: &str) -> Result<Self> {
        let factory = GlobalHttpClientFactory::instance().await?;
        Ok(Self {
            factory,
            source_name: source_name.to_string(),
            custom_headers: HashMap::new(),
        })
    }

    /// Create a new HTTP fetcher with API-optimized configuration using global settings
    pub async fn new_for_apis() -> Result<Self> {
        Self::new_for_source("__api__").await
    }

    /// Create a new HTTP fetcher with web scraping-optimized configuration using global settings
    pub async fn new_for_scraping() -> Result<Self> {
        Self::new_for_source("__scraping__").await
    }

    /// Create HTTP fetcher for a named source (e.g., "bazaarapp", "pandamart")
    pub async fn for_source(source_name: &str) -> Result<Self> {
        let factory = GlobalHttpClientFactory::instance().await?;
        Ok(Self {
            factory,
            source_name: source_name.to_string(),
            custom_headers: HashMap::new(),
        })
    }

    /// Set custom headers (updates the underlying client configuration)
    /// Note: This creates a new client instance with updated headers
    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        debug!(
            "HttpFetcher configured with {} custom headers for source: {}",
            headers.len(),
            self.source_name
        );
        self.custom_headers = headers;
        self
    }

    /// Perform a GET request and return raw JSON using global configuration
    pub async fn get_json(&self, url: &str) -> Result<Value> {
        debug!(
            "GET JSON request to {} using source config: {} with {} custom headers",
            url, self.source_name, self.custom_headers.len()
        );

        if self.custom_headers.is_empty() {
            self.factory.get_json(&self.source_name, url).await
        } else {
            self.factory.get_json_with_headers(&self.source_name, url, &self.custom_headers).await
        }
    }

    /// Perform a POST request and return raw JSON using global configuration
    pub async fn post_json(&self, url: &str, body: &str) -> Result<Value> {
        debug!(
            "POST JSON request to {} using source config: {} with {} custom headers",
            url, self.source_name, self.custom_headers.len()
        );

        if self.custom_headers.is_empty() {
            self.factory.post_json(&self.source_name, url, body).await
        } else {
            self.factory.post_json_with_headers(&self.source_name, url, body, &self.custom_headers).await
        }
    }

    /// Perform a GET request and return raw HTML using global configuration
    pub async fn get_html(&self, url: &str) -> Result<String> {
        debug!(
            "GET HTML request to {} using source config: {}",
            url, self.source_name
        );

        let response = self.factory.get(&self.source_name, url).await?;
        let html = response
            .text()
            .await
            .map_err(|e| anyhow!("Failed to read response text: {}", e))?;

        if html.is_empty() {
            return Err(anyhow!("Empty HTML response"));
        }

        // Basic HTML validation
        if !html.contains("<html") && !html.contains("<div") && !html.contains("<body") {
            return Err(anyhow!("Invalid HTML content"));
        }

        Ok(html)
    }

    /// Perform a GET request with smart delays for web scraping
    pub async fn get_html_smart(&self, url: &str) -> Result<String> {
        // Get source-specific configuration for delay calculation
        let config = self.factory.get_source_config(&self.source_name);

        // Calculate smart delay based on rate limiting configuration
        let base_delay_ms = if config.rate_limit.enabled {
            // Use rate limit to calculate minimum delay
            let min_delay = (1000.0 / config.rate_limit.requests_per_second) as u64;
            min_delay.max(500) // Minimum 500ms for web scraping
        } else {
            1000 // Default 1s delay
        };

        // Add random jitter for human-like behavior
        let jitter = rand::random::<u64>() % base_delay_ms;
        let delay = Duration::from_millis(base_delay_ms + jitter);

        debug!("Smart delay for web scraping: {:?}", delay);
        sleep(delay).await;

        self.get_html(url).await
    }

    /// Fetch multiple URLs concurrently with proper rate limiting
    /// Uses the global concurrency controls automatically
    pub async fn get_json_batch(&self, urls: Vec<&str>) -> Vec<(String, Result<Value>)> {
        debug!(
            "Batch GET JSON request for {} URLs using source: {}",
            urls.len(),
            self.source_name
        );

        // Use the factory's batch executor for proper concurrency control
        let batch_executor = self.factory.create_batch_executor(&self.source_name);
        let results = batch_executor.get_batch(urls.clone()).await;

        // Combine URLs with results
        urls.into_iter()
            .zip(results.into_iter())
            .map(|(url, result)| (url.to_string(), result))
            .collect()
    }

    /// Fetch multiple HTML pages with smart delays and concurrency control
    pub async fn get_html_batch(&self, urls: Vec<&str>) -> Vec<(String, Result<String>)> {
        debug!(
            "Batch GET HTML request for {} URLs using source: {}",
            urls.len(),
            self.source_name
        );

        let mut results = Vec::new();
        let config = self.factory.get_source_config(&self.source_name);

        // Calculate delay between requests in batch
        let batch_delay = if config.rate_limit.enabled {
            let min_delay = (1000.0 / config.rate_limit.requests_per_second) as u64;
            Duration::from_millis(min_delay.max(1000)) // Minimum 1s for HTML scraping
        } else {
            Duration::from_millis(1500) // Default 1.5s delay
        };

        let total_urls = urls.len();

        for url in urls {
            let result = self.get_html_smart(url).await;
            results.push((url.to_string(), result));

            // Delay between requests in batch
            if results.len() < total_urls {
                let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
                sleep(batch_delay + jitter).await;
            }
        }

        results
    }

    /// Get the current concurrency statistics for this fetcher's source
    pub fn get_concurrency_stats(&self) -> crate::http::ConcurrencyStats {
        self.factory.get_concurrency_stats()
    }

    /// Get the health status of the HTTP client factory
    pub fn health_check(&self) -> crate::http::HealthStatus {
        self.factory.health_check()
    }

    /// Get the configuration being used for this source
    pub fn get_config(&self) -> crate::config::ConcurrencyConfig {
        self.factory.get_source_config(&self.source_name)
    }

    /// Get the source name this fetcher is configured for
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Update the source name (useful for reusing fetcher instances)
    pub fn set_source_name(&mut self, source_name: String) {
        self.source_name = source_name;
    }

    /// Perform a raw GET request (returns the response directly)
    pub async fn get_raw(&self, url: &str) -> Result<wreq::Response> {
        debug!(
            "Raw GET request to {} using source config: {}",
            url, self.source_name
        );
        self.factory.get(&self.source_name, url).await
    }

    /// Perform a raw POST request (returns the response directly)
    pub async fn post_raw(&self, url: &str, body: &str) -> Result<wreq::Response> {
        debug!(
            "Raw POST request to {} using source config: {}",
            url, self.source_name
        );
        self.factory.post(&self.source_name, url, body).await
    }

    /// Create a specialized fetcher for a different source while maintaining the same factory
    pub fn for_different_source(&self, source_name: &str) -> Self {
        Self {
            factory: self.factory,
            source_name: source_name.to_string(),
            custom_headers: HashMap::new(),
        }
    }

    /// Check if the fetcher is ready (factory is healthy)
    pub fn is_ready(&self) -> bool {
        matches!(self.health_check().status, crate::http::Health::Healthy)
    }

    /// Get domain-specific statistics for the URLs this fetcher typically accesses
    pub fn get_domain_stats(&self, domain: &str) -> Option<crate::http::DomainStats> {
        self.factory
            .get_concurrency_stats()
            .domain_stats
            .into_iter()
            .find(|stats| stats.domain == domain)
    }

    /// Wait for capacity if needed (useful before batch operations)
    pub async fn wait_for_capacity(&self) -> Result<()> {
        let health = self.health_check();
        if health.global_utilization > 0.9 {
            warn!(
                "High utilization ({}%), waiting before proceeding",
                health.global_utilization * 100.0
            );

            // Calculate wait time based on current rate limit
            let config = self.get_config();
            let wait_time = if config.rate_limit.enabled {
                Duration::from_millis((1000.0 / config.rate_limit.requests_per_second) as u64 * 2)
            } else {
                Duration::from_millis(1000)
            };

            sleep(wait_time).await;
        }
        Ok(())
    }
}

impl Default for HttpFetcher {
    fn default() -> Self {
        // Create a runtime to initialize the async factory
        // This is not ideal but maintains backward compatibility
        let rt = tokio::runtime::Handle::try_current()
            .expect("Default HttpFetcher requires a tokio runtime");

        rt.block_on(async {
            Self::new_for_apis()
                .await
                .expect("Failed to create default HTTP fetcher")
        })
    }
}

// Convenience functions for quick access without creating a fetcher instance
impl HttpFetcher {
    /// Quick GET JSON request using global configuration
    pub async fn quick_get_json(url: &str) -> Result<Value> {
        HttpClientFactory::quick_get(url).await
    }

    /// Quick POST JSON request using global configuration
    pub async fn quick_post_json(url: &str, body: &str) -> Result<Value> {
        HttpClientFactory::quick_post(url, body).await
    }
}
