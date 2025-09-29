use anyhow::{Result, anyhow};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::OnceCell;
use tracing::{error, info, warn};

use super::concurrency_config::ConcurrencyConfig;

/// Global configuration manager for the data pipeline
/// Provides singleton access to configuration with thread-safe updates
#[derive(Debug)]
pub struct ConfigManager {
    config: Arc<RwLock<ConcurrencyConfig>>,
    config_path: String,
}

impl ConfigManager {
    /// Create a new configuration manager
    fn new(config: ConcurrencyConfig, config_path: String) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            config_path,
        }
    }

    /// Get the global configuration manager instance
    pub async fn global() -> &'static ConfigManager {
        static INSTANCE: OnceCell<ConfigManager> = OnceCell::const_new();

        INSTANCE
            .get_or_init(|| async {
                Self::initialize().await.unwrap_or_else(|e| {
                    error!("Failed to initialize configuration manager: {}", e);
                    warn!("Using default configuration");
                    Self::new(
                        ConcurrencyConfig::default(),
                        "config/global.toml".to_string(),
                    )
                })
            })
            .await
    }

    /// Initialize the configuration manager
    async fn initialize() -> Result<ConfigManager> {
        let config_paths = vec![
            "config/global.toml",
            "config/concurrency.toml",
            "./global.toml",
            "./concurrency.toml",
        ];

        // Try to load from multiple possible locations
        for path in config_paths {
            if Path::new(path).exists() {
                info!("Loading global configuration from: {}", path);
                match ConcurrencyConfig::from_file(path) {
                    Ok(config) => {
                        info!("Successfully loaded configuration from: {}", path);
                        return Ok(Self::new(config, path.to_string()));
                    }
                    Err(e) => {
                        warn!("Failed to load configuration from {}: {}", path, e);
                        continue;
                    }
                }
            }
        }

        // If no config file found, create a default one
        let default_path = "config/global.toml";
        let config = ConcurrencyConfig::default();

        // Create config directory if it doesn't exist
        if let Some(parent) = Path::new(default_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Save default configuration
        config.to_file(default_path)?;
        info!("Created default configuration file at: {}", default_path);

        Ok(Self::new(config, default_path.to_string()))
    }

    /// Get a copy of the current configuration
    pub fn get_config(&self) -> ConcurrencyConfig {
        self.config.read().unwrap().clone()
    }

    /// Get configuration for a specific source
    pub fn get_source_config(&self, source_name: &str) -> ConcurrencyConfig {
        let config = self.config.read().unwrap();
        config.get_source_config(source_name)
    }

    /// Update the configuration
    pub fn update_config(&self, new_config: ConcurrencyConfig) -> Result<()> {
        {
            let mut config = self.config.write().unwrap();
            *config = new_config.clone();
        }

        // Save to file
        new_config.to_file(&self.config_path)?;
        info!("Configuration updated and saved to: {}", self.config_path);

        Ok(())
    }

    /// Reload configuration from file
    pub fn reload(&self) -> Result<()> {
        let new_config = ConcurrencyConfig::from_file(&self.config_path)?;
        {
            let mut config = self.config.write().unwrap();
            *config = new_config;
        }
        info!("Configuration reloaded from: {}", self.config_path);
        Ok(())
    }

    /// Get the path to the configuration file
    pub fn config_path(&self) -> &str {
        &self.config_path
    }

    /// Check if configuration file exists
    pub fn config_exists(&self) -> bool {
        Path::new(&self.config_path).exists()
    }

    /// Get HTTP configuration
    pub fn http_config(&self) -> super::concurrency_config::HttpConfig {
        self.config.read().unwrap().http.clone()
    }

    /// Get retry configuration
    pub fn retry_config(&self) -> super::concurrency_config::GlobalRetryConfig {
        self.config.read().unwrap().retry.clone()
    }

    /// Get rate limit configuration
    pub fn rate_limit_config(&self) -> super::concurrency_config::GlobalRateLimitConfig {
        self.config.read().unwrap().rate_limit.clone()
    }

    /// Get emulation configuration
    pub fn emulation_config(&self) -> super::concurrency_config::EmulationConfig {
        self.config.read().unwrap().emulation.clone()
    }

    /// Get concurrency limits
    pub fn concurrency_limits(&self) -> super::concurrency_config::ConcurrencyLimits {
        self.config.read().unwrap().concurrency.clone()
    }

    /// Get maximum concurrent requests
    pub fn max_concurrent_requests(&self) -> usize {
        self.config
            .read()
            .unwrap()
            .concurrency
            .max_concurrent_requests
    }

    /// Get maximum concurrent requests per domain
    pub fn max_concurrent_per_domain(&self) -> usize {
        self.config
            .read()
            .unwrap()
            .concurrency
            .max_concurrent_per_domain
    }

    /// Check if rate limiting is enabled
    pub fn is_rate_limiting_enabled(&self) -> bool {
        self.config.read().unwrap().rate_limit.enabled
    }

    /// Get requests per second limit
    pub fn requests_per_second(&self) -> f64 {
        self.config.read().unwrap().rate_limit.requests_per_second
    }

    /// Get rate limit for specific domain
    pub fn domain_rate_limit(
        &self,
        domain: &str,
    ) -> Option<super::concurrency_config::DomainRateLimit> {
        self.config
            .read()
            .unwrap()
            .rate_limit
            .domain_limits
            .get(domain)
            .cloned()
    }

    /// Update source override configuration
    pub fn update_source_override(
        &self,
        source_name: &str,
        override_config: super::concurrency_config::SourceOverride,
    ) -> Result<()> {
        {
            let mut config = self.config.write().unwrap();
            config
                .source_overrides
                .insert(source_name.to_string(), override_config);
        }

        // Save to file
        let config = self.get_config();
        config.to_file(&self.config_path)?;
        info!("Source override updated for: {}", source_name);

        Ok(())
    }

    /// Remove source override configuration
    pub fn remove_source_override(&self, source_name: &str) -> Result<()> {
        {
            let mut config = self.config.write().unwrap();
            config.source_overrides.remove(source_name);
        }

        // Save to file
        let config = self.get_config();
        config.to_file(&self.config_path)?;
        info!("Source override removed for: {}", source_name);

        Ok(())
    }

    /// Get all configured sources with overrides
    pub fn get_configured_sources(&self) -> Vec<String> {
        self.config
            .read()
            .unwrap()
            .source_overrides
            .keys()
            .cloned()
            .collect()
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        let config = self.config.read().unwrap();

        // Validate retry configuration
        if config.retry.max_attempts == 0 {
            return Err(anyhow!("Max retry attempts must be greater than 0"));
        }

        if config.retry.base_delay_ms == 0 {
            return Err(anyhow!("Base delay must be greater than 0"));
        }

        if config.retry.backoff_multiplier < 1.0 {
            return Err(anyhow!("Backoff multiplier must be >= 1.0"));
        }

        // Validate rate limiting configuration
        if config.rate_limit.enabled {
            if config.rate_limit.requests_per_second <= 0.0 {
                return Err(anyhow!("Requests per second must be greater than 0"));
            }

            if config.rate_limit.burst_size == 0 {
                return Err(anyhow!("Burst size must be greater than 0"));
            }
        }

        // Validate concurrency limits
        if config.concurrency.max_concurrent_requests == 0 {
            return Err(anyhow!("Max concurrent requests must be greater than 0"));
        }

        if config.concurrency.max_concurrent_per_domain == 0 {
            return Err(anyhow!("Max concurrent per domain must be greater than 0"));
        }

        // Validate timeouts
        if config.http.timeout_seconds == 0 {
            return Err(anyhow!("HTTP timeout must be greater than 0"));
        }

        info!("Configuration validation passed");
        Ok(())
    }

    /// Get configuration statistics
    pub fn get_stats(&self) -> ConfigStats {
        let config = self.config.read().unwrap();

        ConfigStats {
            sources_with_overrides: config.source_overrides.len(),
            domain_rate_limits: config.rate_limit.domain_limits.len(),
            max_concurrent_requests: config.concurrency.max_concurrent_requests,
            global_rate_limit: config.rate_limit.requests_per_second,
            rate_limiting_enabled: config.rate_limit.enabled,
            retry_attempts: config.retry.max_attempts,
            total_browsers: config.emulation.browsers.len(),
            config_path: self.config_path.clone(),
        }
    }
}

/// Configuration statistics for monitoring and debugging
#[derive(Debug, Clone)]
pub struct ConfigStats {
    pub sources_with_overrides: usize,
    pub domain_rate_limits: usize,
    pub max_concurrent_requests: usize,
    pub global_rate_limit: f64,
    pub rate_limiting_enabled: bool,
    pub retry_attempts: u32,
    pub total_browsers: usize,
    pub config_path: String,
}

impl ConfigStats {
    /// Display configuration statistics
    pub fn display(&self) {
        info!("=== Configuration Statistics ===");
        info!("Config Path: {}", self.config_path);
        info!("Sources with overrides: {}", self.sources_with_overrides);
        info!("Domain-specific rate limits: {}", self.domain_rate_limits);
        info!("Max concurrent requests: {}", self.max_concurrent_requests);
        info!("Global rate limit: {} req/sec", self.global_rate_limit);
        info!("Rate limiting enabled: {}", self.rate_limiting_enabled);
        info!("Retry attempts: {}", self.retry_attempts);
        info!("Browser emulations: {}", self.total_browsers);
        info!("================================");
    }
}

// Convenience functions for easy access
impl ConfigManager {
    /// Create HTTP client configuration from global settings
    pub fn create_http_client_config(&self) -> crate::http::ClientConfig {
        let config = self.get_config();

        crate::http::ClientConfig {
            emulation_strategy: config.to_emulation_strategy(),
            timeout: config.timeout(),
            user_agent: config.http.user_agent.clone(),
            default_headers: config.http.default_headers.clone(),
            retry_config: config.to_retry_config(),
            rate_limit_config: config.to_rate_limit_config(),
        }
    }

    /// Create HTTP client configuration for a specific source
    pub fn create_source_http_client_config(&self, source_name: &str) -> crate::http::ClientConfig {
        let config = self.get_source_config(source_name);

        crate::http::ClientConfig {
            emulation_strategy: config.to_emulation_strategy(),
            timeout: config.timeout(),
            user_agent: config.http.user_agent.clone(),
            default_headers: config.http.default_headers.clone(),
            retry_config: config.to_retry_config(),
            rate_limit_config: config.to_rate_limit_config(),
        }
    }
}
