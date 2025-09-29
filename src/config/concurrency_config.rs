use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use wreq_util::Emulation;

use super::super::http::{EmulationStrategy, RateLimitConfig, RetryConfig};

/// Global concurrency configuration for the entire data pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    /// Global HTTP client settings
    pub http: HttpConfig,
    /// Global retry configuration
    pub retry: GlobalRetryConfig,
    /// Global rate limiting configuration
    pub rate_limit: GlobalRateLimitConfig,
    /// Browser emulation settings
    pub emulation: EmulationConfig,
    /// Connection pooling and concurrency limits
    pub concurrency: ConcurrencyLimits,
    /// Per-source overrides
    pub source_overrides: HashMap<String, SourceOverride>,
}

/// HTTP client configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_seconds: u64,
    /// Read timeout in seconds
    #[serde(default = "default_read_timeout")]
    pub read_timeout_seconds: u64,
    /// Maximum number of redirects to follow
    #[serde(default = "default_max_redirects")]
    pub max_redirects: u32,
    /// Custom user agent (optional)
    pub user_agent: Option<String>,
    /// Default headers to include with all requests
    #[serde(default)]
    pub default_headers: HashMap<String, String>,
    /// Enable HTTP/2
    #[serde(default = "default_true")]
    pub http2: bool,
    /// Enable cookie jar
    #[serde(default = "default_true")]
    pub cookies: bool,
}

/// Global retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRetryConfig {
    /// Maximum number of retry attempts
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Base delay between retries in milliseconds
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,
    /// Maximum delay between retries in seconds
    #[serde(default = "default_max_delay_seconds")]
    pub max_delay_seconds: u64,
    /// Backoff multiplier for exponential backoff
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    /// Enable jitter to prevent thundering herd
    #[serde(default = "default_true")]
    pub jitter: bool,
    /// HTTP status codes that should trigger a retry
    #[serde(default = "default_retryable_status_codes")]
    pub retryable_status_codes: Vec<u16>,
    /// Enable retry on connection errors
    #[serde(default = "default_true")]
    pub retry_on_connection_error: bool,
    /// Enable retry on timeout
    #[serde(default = "default_true")]
    pub retry_on_timeout: bool,
}

/// Global rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRateLimitConfig {
    /// Enable global rate limiting
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Default requests per second limit
    #[serde(default = "default_requests_per_second")]
    pub requests_per_second: f64,
    /// Default burst size (number of requests that can be made at once)
    #[serde(default = "default_burst_size")]
    pub burst_size: u32,
    /// Rate limiting strategy
    #[serde(default)]
    pub strategy: RateLimitStrategy,
    /// Per-domain rate limits
    #[serde(default)]
    pub domain_limits: HashMap<String, DomainRateLimit>,
}

/// Rate limiting strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitStrategy {
    /// Token bucket algorithm (default)
    TokenBucket,
    /// Fixed window algorithm
    FixedWindow,
    /// Sliding window algorithm
    SlidingWindow,
}

/// Per-domain rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainRateLimit {
    /// Requests per second for this domain
    pub requests_per_second: f64,
    /// Burst size for this domain
    pub burst_size: u32,
    /// Enable rate limiting for this domain
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Browser emulation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmulationConfig {
    /// Emulation strategy to use
    #[serde(default)]
    pub strategy: EmulationStrategyConfig,
    /// List of browsers to rotate through (used with rotate strategy)
    #[serde(default = "default_browser_list")]
    pub browsers: Vec<String>,
    /// Enable random user agent rotation
    #[serde(default = "default_false")]
    pub rotate_user_agents: bool,
    /// Custom user agents to rotate through
    #[serde(default)]
    pub custom_user_agents: Vec<String>,
}

/// Emulation strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EmulationStrategyConfig {
    /// Use a fixed browser emulation
    Fixed(String),
    /// Use random emulation for each request
    Random,
    /// Rotate through a list of emulations
    Rotate,
    /// Use modern browsers only
    ModernBrowsers,
    /// Use Chrome variants only
    ChromeVariants,
    /// Use Firefox variants only
    FirefoxVariants,
}

/// Concurrency limits and connection pooling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyLimits {
    /// Maximum number of concurrent requests globally
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Maximum number of concurrent requests per domain
    #[serde(default = "default_max_concurrent_per_domain")]
    pub max_concurrent_per_domain: usize,
    /// Maximum number of connections in the connection pool
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Maximum number of idle connections to keep
    #[serde(default = "default_max_idle_connections")]
    pub max_idle_connections: usize,
    /// Idle connection timeout in seconds
    #[serde(default = "default_idle_timeout_seconds")]
    pub idle_timeout_seconds: u64,
    /// Enable HTTP/2 multiplexing
    #[serde(default = "default_true")]
    pub http2_multiplexing: bool,
}

/// Per-source configuration overrides
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceOverride {
    /// Override HTTP configuration
    pub http: Option<HttpConfig>,
    /// Override retry configuration
    pub retry: Option<GlobalRetryConfig>,
    /// Override rate limiting configuration
    pub rate_limit: Option<GlobalRateLimitConfig>,
    /// Override emulation configuration
    pub emulation: Option<EmulationConfig>,
    /// Override concurrency limits
    pub concurrency: Option<ConcurrencyLimits>,
}

// Default value functions
fn default_timeout() -> u64 {
    30
}
fn default_connect_timeout() -> u64 {
    10
}
fn default_read_timeout() -> u64 {
    30
}
fn default_max_redirects() -> u32 {
    5
}
fn default_true() -> bool {
    true
}
fn default_false() -> bool {
    false
}

fn default_max_attempts() -> u32 {
    3
}
fn default_base_delay_ms() -> u64 {
    500
}
fn default_max_delay_seconds() -> u64 {
    30
}
fn default_backoff_multiplier() -> f64 {
    2.0
}
fn default_retryable_status_codes() -> Vec<u16> {
    vec![408, 429, 500, 502, 503, 504]
}

fn default_requests_per_second() -> f64 {
    10.0
}
fn default_burst_size() -> u32 {
    5
}

fn default_browser_list() -> Vec<String> {
    vec![
        "chrome_137".to_string(),
        "chrome_136".to_string(),
        "chrome_135".to_string(),
        "firefox_139".to_string(),
        "firefox_136".to_string(),
        "firefox_135".to_string(),
        "edge_134".to_string(),
        "safari_18_5".to_string(),
    ]
}

fn default_max_concurrent_requests() -> usize {
    50
}
fn default_max_concurrent_per_domain() -> usize {
    10
}
fn default_max_connections() -> usize {
    100
}
fn default_max_idle_connections() -> usize {
    20
}
fn default_idle_timeout_seconds() -> u64 {
    60
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            http: HttpConfig::default(),
            retry: GlobalRetryConfig::default(),
            rate_limit: GlobalRateLimitConfig::default(),
            emulation: EmulationConfig::default(),
            concurrency: ConcurrencyLimits::default(),
            source_overrides: HashMap::new(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: default_timeout(),
            connect_timeout_seconds: default_connect_timeout(),
            read_timeout_seconds: default_read_timeout(),
            max_redirects: default_max_redirects(),
            user_agent: None,
            default_headers: HashMap::new(),
            http2: default_true(),
            cookies: default_true(),
        }
    }
}

impl Default for GlobalRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            base_delay_ms: default_base_delay_ms(),
            max_delay_seconds: default_max_delay_seconds(),
            backoff_multiplier: default_backoff_multiplier(),
            jitter: default_true(),
            retryable_status_codes: default_retryable_status_codes(),
            retry_on_connection_error: default_true(),
            retry_on_timeout: default_true(),
        }
    }
}

impl Default for GlobalRateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            requests_per_second: default_requests_per_second(),
            burst_size: default_burst_size(),
            strategy: RateLimitStrategy::default(),
            domain_limits: HashMap::new(),
        }
    }
}

impl Default for RateLimitStrategy {
    fn default() -> Self {
        Self::TokenBucket
    }
}

impl Default for EmulationConfig {
    fn default() -> Self {
        Self {
            strategy: EmulationStrategyConfig::default(),
            browsers: default_browser_list(),
            rotate_user_agents: default_false(),
            custom_user_agents: Vec::new(),
        }
    }
}

impl Default for EmulationStrategyConfig {
    fn default() -> Self {
        Self::ModernBrowsers
    }
}

impl Default for ConcurrencyLimits {
    fn default() -> Self {
        Self {
            max_concurrent_requests: default_max_concurrent_requests(),
            max_concurrent_per_domain: default_max_concurrent_per_domain(),
            max_connections: default_max_connections(),
            max_idle_connections: default_max_idle_connections(),
            idle_timeout_seconds: default_idle_timeout_seconds(),
            http2_multiplexing: default_true(),
        }
    }
}

impl ConcurrencyConfig {
    /// Load configuration from a TOML file
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: ConcurrencyConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to a TOML file
    pub fn to_file(&self, path: &str) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Get configuration for a specific source, applying overrides if present
    pub fn get_source_config(&self, source_name: &str) -> ConcurrencyConfig {
        let mut config = self.clone();

        if let Some(override_config) = self.source_overrides.get(source_name) {
            if let Some(http_override) = &override_config.http {
                config.http = http_override.clone();
            }
            if let Some(retry_override) = &override_config.retry {
                config.retry = retry_override.clone();
            }
            if let Some(rate_limit_override) = &override_config.rate_limit {
                config.rate_limit = rate_limit_override.clone();
            }
            if let Some(emulation_override) = &override_config.emulation {
                config.emulation = emulation_override.clone();
            }
            if let Some(concurrency_override) = &override_config.concurrency {
                config.concurrency = concurrency_override.clone();
            }
        }

        config
    }

    /// Convert to RetryConfig for the existing HTTP client
    pub fn to_retry_config(&self) -> RetryConfig {
        RetryConfig {
            max_attempts: self.retry.max_attempts,
            base_delay: Duration::from_millis(self.retry.base_delay_ms),
            max_delay: Duration::from_secs(self.retry.max_delay_seconds),
            backoff_multiplier: self.retry.backoff_multiplier,
            jitter: self.retry.jitter,
        }
    }

    /// Convert to RateLimitConfig for the existing HTTP client
    pub fn to_rate_limit_config(&self) -> RateLimitConfig {
        RateLimitConfig {
            requests_per_second: self.rate_limit.requests_per_second,
            burst_size: self.rate_limit.burst_size,
            enabled: self.rate_limit.enabled,
        }
    }

    /// Convert to EmulationStrategy for the existing HTTP client
    pub fn to_emulation_strategy(&self) -> EmulationStrategy {
        match &self.emulation.strategy {
            EmulationStrategyConfig::Fixed(browser) => {
                EmulationStrategy::Fixed(self.parse_emulation(browser))
            }
            EmulationStrategyConfig::Random => EmulationStrategy::Random,
            EmulationStrategyConfig::Rotate => {
                let emulations = self
                    .emulation
                    .browsers
                    .iter()
                    .map(|b| self.parse_emulation(b))
                    .collect();
                EmulationStrategy::Rotate(emulations)
            }
            EmulationStrategyConfig::ModernBrowsers => EmulationStrategy::modern_browsers(),
            EmulationStrategyConfig::ChromeVariants => EmulationStrategy::chrome_variants(),
            EmulationStrategyConfig::FirefoxVariants => EmulationStrategy::firefox_variants(),
        }
    }

    /// Parse browser string to Emulation enum
    fn parse_emulation(&self, browser: &str) -> Emulation {
        match browser.to_lowercase().as_str() {
            "chrome_137" => Emulation::Chrome137,
            "chrome_136" => Emulation::Chrome136,
            "chrome_135" => Emulation::Chrome135,
            "chrome_134" => Emulation::Chrome134,
            "firefox_139" => Emulation::Firefox139,
            "firefox_136" => Emulation::Firefox136,
            "firefox_135" => Emulation::Firefox135,
            "firefox_133" => Emulation::Firefox133,
            "edge_134" => Emulation::Edge134,
            "edge_131" => Emulation::Edge131,
            "safari_18_5" => Emulation::Safari18_5,
            "safari_18_3" => Emulation::Safari18_3,
            _ => Emulation::Chrome137, // Default fallback
        }
    }

    /// Get timeout as Duration
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.http.timeout_seconds)
    }

    /// Get connect timeout as Duration
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.http.connect_timeout_seconds)
    }

    /// Get read timeout as Duration
    pub fn read_timeout(&self) -> Duration {
        Duration::from_secs(self.http.read_timeout_seconds)
    }

    /// Get idle timeout as Duration
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.concurrency.idle_timeout_seconds)
    }
}
