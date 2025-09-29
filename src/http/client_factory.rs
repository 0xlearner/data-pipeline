use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tracing::{debug, info};

use super::HttpClient;
use crate::config::{ConcurrencyConfig, ConfigManager};

/// Centralized HTTP client factory that manages clients with global configuration
/// Provides connection pooling, concurrency limits, and per-source configuration
pub struct HttpClientFactory {
    /// Global semaphore for concurrent requests
    global_semaphore: Arc<Semaphore>,
    /// Per-domain semaphores for domain-specific limits
    domain_semaphores: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    /// Cached HTTP clients per source
    client_cache: Arc<Mutex<HashMap<String, Arc<HttpClient>>>>,
    /// Configuration manager reference
    config_manager: &'static ConfigManager,
}

impl HttpClientFactory {
    /// Create a new HTTP client factory
    pub async fn new() -> Result<Self> {
        let config_manager = ConfigManager::global().await;
        let concurrency_limits = config_manager.concurrency_limits();

        Ok(Self {
            global_semaphore: Arc::new(Semaphore::new(concurrency_limits.max_concurrent_requests)),
            domain_semaphores: Arc::new(Mutex::new(HashMap::new())),
            client_cache: Arc::new(Mutex::new(HashMap::new())),
            config_manager,
        })
    }

    /// Get or create an HTTP client for the global configuration
    pub async fn get_global_client(&self) -> Result<Arc<HttpClient>> {
        self.get_client_for_source("__global__").await
    }

    /// Get or create an HTTP client for a specific source
    pub async fn get_client_for_source(&self, source_name: &str) -> Result<Arc<HttpClient>> {
        // Check cache first
        {
            let cache = self.client_cache.lock().unwrap();
            if let Some(client) = cache.get(source_name) {
                debug!("Using cached HTTP client for source: {}", source_name);
                return Ok(client.clone());
            }
        }

        // Create new client
        let client = self.create_client_for_source(source_name).await?;
        let client_arc = Arc::new(client);

        // Cache the client
        {
            let mut cache = self.client_cache.lock().unwrap();
            cache.insert(source_name.to_string(), client_arc.clone());
        }

        info!("Created new HTTP client for source: {}", source_name);
        Ok(client_arc)
    }

    /// Create a new HTTP client for a specific source
    async fn create_client_for_source(&self, source_name: &str) -> Result<HttpClient> {
        let client_config = if source_name == "__global__" {
            self.config_manager.create_http_client_config()
        } else {
            self.config_manager
                .create_source_http_client_config(source_name)
        };

        HttpClient::with_config(client_config)
    }

    /// Get or create a domain-specific semaphore for concurrency control
    fn get_domain_semaphore(&self, domain: &str) -> Arc<Semaphore> {
        let mut semaphores = self.domain_semaphores.lock().unwrap();

        if let Some(semaphore) = semaphores.get(domain) {
            return semaphore.clone();
        }

        // Check if there's a domain-specific rate limit
        let limit = if let Some(domain_limit) = self.config_manager.domain_rate_limit(domain) {
            domain_limit.burst_size as usize
        } else {
            self.config_manager.max_concurrent_per_domain()
        };

        let semaphore = Arc::new(Semaphore::new(limit));
        semaphores.insert(domain.to_string(), semaphore.clone());

        debug!(
            "Created domain semaphore for {} with limit: {}",
            domain, limit
        );
        semaphore
    }

    /// Execute a request with global and domain-specific concurrency limits
    pub async fn execute_with_limits<F, T>(&self, domain: &str, operation: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        // Acquire global semaphore
        let _global_permit = self
            .global_semaphore
            .acquire()
            .await
            .map_err(|_| anyhow!("Failed to acquire global concurrency permit"))?;

        // Acquire domain-specific semaphore
        let domain_semaphore = self.get_domain_semaphore(domain);
        let _domain_permit = domain_semaphore.acquire().await.map_err(|_| {
            anyhow!(
                "Failed to acquire domain concurrency permit for: {}",
                domain
            )
        })?;

        debug!("Acquired concurrency permits for domain: {}", domain);

        // Execute the operation
        let result = operation.await;

        debug!("Released concurrency permits for domain: {}", domain);
        result
    }

    /// Make a GET request with concurrency limits
    pub async fn get(&self, source_name: &str, url: &str) -> Result<wreq::Response> {
        let client = self.get_client_for_source(source_name).await?;
        let domain = self.extract_domain(url)?;

        self.execute_with_limits(&domain, async { client.get(url).await })
            .await
    }

    /// Make a POST request with concurrency limits
    pub async fn post(&self, source_name: &str, url: &str, body: &str) -> Result<wreq::Response> {
        let client = self.get_client_for_source(source_name).await?;
        let domain = self.extract_domain(url)?;

        self.execute_with_limits(&domain, async { client.post(url, body).await })
            .await
    }

    /// Make a GET request and parse JSON with concurrency limits
    pub async fn get_json(&self, source_name: &str, url: &str) -> Result<serde_json::Value> {
        let client = self.get_client_for_source(source_name).await?;
        let domain = self.extract_domain(url)?;

        self.execute_with_limits(&domain, async { client.get_json(url).await })
            .await
    }

    /// Make a POST request and parse JSON with concurrency limits
    pub async fn post_json(
        &self,
        source_name: &str,
        url: &str,
        body: &str,
    ) -> Result<serde_json::Value> {
        let client = self.get_client_for_source(source_name).await?;
        let domain = self.extract_domain(url)?;

        self.execute_with_limits(&domain, async { client.post_json(url, body).await })
            .await
    }

    /// Make a POST request with custom headers and parse JSON with concurrency limits
    pub async fn post_json_with_headers(
        &self,
        source_name: &str,
        url: &str,
        body: &str,
        custom_headers: &HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        let domain = self.extract_domain(url)?;

        // Create a temporary client with custom headers
        let client = self.create_client_with_custom_headers(source_name, custom_headers).await?;

        self.execute_with_limits(&domain, async { client.post_json(url, body).await })
            .await
    }

    /// Make a GET request with custom headers and parse JSON with concurrency limits
    pub async fn get_json_with_headers(
        &self,
        source_name: &str,
        url: &str,
        custom_headers: &HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        let domain = self.extract_domain(url)?;

        // Create a temporary client with custom headers
        let client = self.create_client_with_custom_headers(source_name, custom_headers).await?;

        self.execute_with_limits(&domain, async { client.get_json(url).await })
            .await
    }

    /// Create a temporary HTTP client with custom headers for a specific source
    async fn create_client_with_custom_headers(
        &self,
        source_name: &str,
        custom_headers: &HashMap<String, String>,
    ) -> Result<HttpClient> {
        let mut client_config = if source_name == "__global__" {
            self.config_manager.create_http_client_config()
        } else {
            self.config_manager
                .create_source_http_client_config(source_name)
        };

        // Merge custom headers with default headers
        for (key, value) in custom_headers {
            client_config.default_headers.insert(key.clone(), value.clone());
        }

        HttpClient::with_config(client_config)
    }

    /// Extract domain from URL for concurrency limiting
    fn extract_domain(&self, url: &str) -> Result<String> {
        if let Ok(parsed_url) = url::Url::parse(url) {
            if let Some(host) = parsed_url.host_str() {
                Ok(host.to_string())
            } else {
                Err(anyhow!("Failed to extract host from URL: {}", url))
            }
        } else {
            Err(anyhow!("Failed to parse URL: {}", url))
        }
    }

    /// Clear the client cache (useful for configuration updates)
    pub fn clear_cache(&self) {
        let mut cache = self.client_cache.lock().unwrap();
        cache.clear();
        info!("Cleared HTTP client cache");
    }

    /// Update configuration and clear cache
    pub async fn reload_configuration(&self) -> Result<()> {
        // Reload the global configuration
        self.config_manager.reload()?;

        // Clear the cache to force recreation of clients with new config
        self.clear_cache();

        // Clear domain semaphores to recreate with new limits
        {
            let mut semaphores = self.domain_semaphores.lock().unwrap();
            semaphores.clear();
        }

        info!("Configuration reloaded and caches cleared");
        Ok(())
    }

    /// Get current concurrency statistics
    pub fn get_concurrency_stats(&self) -> ConcurrencyStats {
        let global_available = self.global_semaphore.available_permits();
        let global_total = self.config_manager.max_concurrent_requests();

        let domain_stats = {
            let semaphores = self.domain_semaphores.lock().unwrap();
            semaphores
                .iter()
                .map(|(domain, semaphore)| DomainStats {
                    domain: domain.clone(),
                    available_permits: semaphore.available_permits(),
                    total_permits: self.config_manager.max_concurrent_per_domain(),
                })
                .collect()
        };

        let cached_clients = {
            let cache = self.client_cache.lock().unwrap();
            cache.len()
        };

        ConcurrencyStats {
            global_available_permits: global_available,
            global_total_permits: global_total,
            domain_stats,
            cached_clients,
        }
    }

    /// Check if the factory is healthy
    pub fn health_check(&self) -> HealthStatus {
        let stats = self.get_concurrency_stats();

        // Check if we have available permits
        let global_utilization =
            1.0 - (stats.global_available_permits as f64 / stats.global_total_permits as f64);

        let status = if global_utilization > 0.9 {
            Health::Critical
        } else if global_utilization > 0.7 {
            Health::Warning
        } else {
            Health::Healthy
        };

        HealthStatus {
            status,
            global_utilization,
            message: format!(
                "Global permits: {}/{}",
                stats.global_available_permits, stats.global_total_permits
            ),
        }
    }

    /// Create a batch request executor for multiple requests
    pub fn create_batch_executor(&self, source_name: &str) -> BatchExecutor<'_> {
        BatchExecutor::new(self, source_name.to_string())
    }

    /// Get configuration for a specific source
    pub fn get_source_config(&self, source_name: &str) -> ConcurrencyConfig {
        self.config_manager.get_source_config(source_name)
    }
}

/// Statistics about current concurrency usage
#[derive(Debug, Clone)]
pub struct ConcurrencyStats {
    pub global_available_permits: usize,
    pub global_total_permits: usize,
    pub domain_stats: Vec<DomainStats>,
    pub cached_clients: usize,
}

/// Domain-specific concurrency statistics
#[derive(Debug, Clone)]
pub struct DomainStats {
    pub domain: String,
    pub available_permits: usize,
    pub total_permits: usize,
}

/// Health status of the HTTP client factory
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub status: Health,
    pub global_utilization: f64,
    pub message: String,
}

/// Health enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum Health {
    Healthy,
    Warning,
    Critical,
}

/// Batch executor for multiple HTTP requests with shared concurrency limits
pub struct BatchExecutor<'a> {
    factory: &'a HttpClientFactory,
    source_name: String,
}

impl<'a> BatchExecutor<'a> {
    fn new(factory: &'a HttpClientFactory, source_name: String) -> Self {
        Self {
            factory,
            source_name,
        }
    }

    /// Execute multiple GET requests concurrently
    pub async fn get_batch(&self, urls: Vec<&str>) -> Vec<Result<serde_json::Value>> {
        let futures = urls
            .into_iter()
            .map(|url| self.factory.get_json(&self.source_name, url));

        futures::future::join_all(futures).await
    }

    /// Execute multiple POST requests concurrently
    pub async fn post_batch(&self, requests: Vec<(&str, &str)>) -> Vec<Result<serde_json::Value>> {
        let futures = requests
            .into_iter()
            .map(|(url, body)| self.factory.post_json(&self.source_name, url, body));

        futures::future::join_all(futures).await
    }
}

/// Global HTTP client factory instance
pub struct GlobalHttpClientFactory;

impl GlobalHttpClientFactory {
    /// Get the global HTTP client factory instance
    pub async fn instance() -> Result<&'static HttpClientFactory> {
        static FACTORY: tokio::sync::OnceCell<HttpClientFactory> =
            tokio::sync::OnceCell::const_new();

        FACTORY
            .get_or_try_init(|| async { HttpClientFactory::new().await })
            .await
    }
}

// Convenience functions for easy access
impl HttpClientFactory {
    /// Quick GET request using global configuration
    pub async fn quick_get(url: &str) -> Result<serde_json::Value> {
        let factory = GlobalHttpClientFactory::instance().await?;
        factory.get_json("__global__", url).await
    }

    /// Quick POST request using global configuration
    pub async fn quick_post(url: &str, body: &str) -> Result<serde_json::Value> {
        let factory = GlobalHttpClientFactory::instance().await?;
        factory.post_json("__global__", url, body).await
    }
}
