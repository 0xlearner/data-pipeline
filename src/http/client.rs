use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error};
use wreq::{Client, Response};
use wreq_util::Emulation;

use super::retry::{RetryPolicy, RetryConfig};
use super::rate_limiter::{RateLimiter, RateLimitConfig};

/// Strategy for browser emulation
#[derive(Debug, Clone)]
pub enum EmulationStrategy {
    /// Use a fixed emulation
    Fixed(Emulation),
    /// Use random emulation for each request
    Random,
    /// Rotate through a list of emulations
    Rotate(Vec<Emulation>),
}

impl Default for EmulationStrategy {
    fn default() -> Self {
        Self::Fixed(Emulation::Firefox136)
    }
}

impl EmulationStrategy {
    /// Get the next emulation to use
    pub fn get_emulation(&self) -> Emulation {
        match self {
            EmulationStrategy::Fixed(emulation) => emulation.clone(),
            EmulationStrategy::Random => Self::random_emulation(),
            EmulationStrategy::Rotate(emulations) => {
                if emulations.is_empty() {
                    Emulation::Firefox136
                } else {
                    // Simple rotation based on current time
                    let index = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as usize) % emulations.len();
                    emulations[index].clone()
                }
            }
        }
    }

    /// Get a random emulation from common browsers
    fn random_emulation() -> Emulation {
        let emulations = vec![
            Emulation::Chrome137,
            Emulation::Chrome136,
            Emulation::Chrome135,
            Emulation::Firefox139,
            Emulation::Firefox136,
            Emulation::Firefox135,
            Emulation::Edge134,
            Emulation::Edge131,
            Emulation::Safari18_5,
            Emulation::Safari18_3,
        ];

        let index = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as usize) % emulations.len();
        emulations[index].clone()
    }

    /// Create a strategy with common modern browsers
    pub fn modern_browsers() -> Self {
        Self::Rotate(vec![
            Emulation::Chrome137,
            Emulation::Chrome136,
            Emulation::Chrome135,
            Emulation::Firefox139,
            Emulation::Firefox136,
            Emulation::Firefox135,
            Emulation::Edge134,
            Emulation::Edge131,
            Emulation::Safari18_5,
            Emulation::Safari18_3,
        ])
    }

    /// Create a strategy with Chrome variants only
    pub fn chrome_variants() -> Self {
        Self::Rotate(vec![
            Emulation::Chrome137,
            Emulation::Chrome136,
            Emulation::Chrome135,
            Emulation::Chrome134,
            Emulation::Chrome133,
        ])
    }

    /// Create a strategy with Firefox variants only
    pub fn firefox_variants() -> Self {
        Self::Rotate(vec![
            Emulation::Firefox139,
            Emulation::Firefox136,
            Emulation::Firefox135,
            Emulation::Firefox133,
            Emulation::FirefoxPrivate136,
        ])
    }
}

/// Configuration for HTTP client
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub emulation_strategy: EmulationStrategy,
    pub timeout: Duration,
    pub user_agent: Option<String>,
    pub default_headers: HashMap<String, String>,
    pub retry_config: RetryConfig,
    pub rate_limit_config: RateLimitConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            emulation_strategy: EmulationStrategy::default(),
            timeout: Duration::from_secs(30),
            user_agent: None,
            default_headers: HashMap::new(),
            retry_config: RetryConfig::default(),
            rate_limit_config: RateLimitConfig::default(),
        }
    }
}

/// Shared HTTP client with retry logic and rate limiting
pub struct HttpClient {
    config: ClientConfig,
    retry_policy: RetryPolicy,
    rate_limiter: RateLimiter,
}

impl HttpClient {
    /// Create a new HTTP client with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(ClientConfig::default())
    }

    /// Create a new HTTP client with custom configuration
    pub fn with_config(config: ClientConfig) -> Result<Self> {
        let retry_policy = RetryPolicy::new(config.retry_config.clone());
        let rate_limiter = RateLimiter::new(config.rate_limit_config.clone());

        Ok(Self {
            config,
            retry_policy,
            rate_limiter,
        })
    }

    /// Create a wreq client with current emulation strategy
    fn create_client(&self) -> Result<Client> {
        let emulation = self.config.emulation_strategy.get_emulation();

        let mut client_builder = Client::builder()
            .emulation(emulation)
            .timeout(self.config.timeout);

        if let Some(user_agent) = &self.config.user_agent {
            client_builder = client_builder.user_agent(user_agent);
        }

        client_builder.build().map_err(|e| anyhow!("Failed to create HTTP client: {}", e))
    }

    /// Perform a GET request with retry logic and rate limiting
    pub async fn get(&self, url: &str) -> Result<Response> {
        self.rate_limiter.wait().await;

        let headers = self.config.default_headers.clone();

        self.retry_policy.execute(|| async {
            debug!("Making GET request to: {}", url);

            // Create a fresh client with potentially new emulation
            let client = self.create_client()?;
            let mut request = client.get(url);

            // Add headers
            for (key, value) in &headers {
                request = request.header(key, value);
            }

            let response = request.send().await?;

            if response.status().is_success() {
                debug!("GET request successful: {} - Status: {}", url, response.status());
                Ok(response)
            } else {
                let status = response.status();
                let error_msg = format!("HTTP error: {} for URL: {}", status, url);
                error!("{}", error_msg);
                Err(anyhow!(error_msg))
            }
        }).await
    }

    /// Perform a POST request with retry logic and rate limiting
    pub async fn post(&self, url: &str, body: &str) -> Result<Response> {
        self.rate_limiter.wait().await;

        let headers = self.config.default_headers.clone();

        self.retry_policy.execute(|| async {
            debug!("Making POST request to: {}", url);

            // Create a fresh client with potentially new emulation
            let client = self.create_client()?;
            let mut request = client.post(url).body(body.to_string());

            // Add headers
            for (key, value) in &headers {
                request = request.header(key, value);
            }

            let response = request.send().await?;

            if response.status().is_success() {
                debug!("POST request successful: {} - Status: {}", url, response.status());
                Ok(response)
            } else {
                let status = response.status();
                let error_msg = format!("HTTP error: {} for URL: {}", status, url);
                error!("{}", error_msg);
                Err(anyhow!(error_msg))
            }
        }).await
    }

    /// Perform a GET request and parse JSON response
    pub async fn get_json(&self, url: &str) -> Result<Value> {
        let response = self.get(url).await?;
        let text = response.text().await?;
        
        serde_json::from_str(&text)
            .map_err(|e| anyhow!("Failed to parse JSON from {}: {}", url, e))
    }

    /// Perform a POST request and parse JSON response
    pub async fn post_json(&self, url: &str, body: &str) -> Result<Value> {
        let response = self.post(url, body).await?;
        let text = response.text().await?;
        
        serde_json::from_str(&text)
            .map_err(|e| anyhow!("Failed to parse JSON from {}: {}", url, e))
    }

    /// Create a new wreq client with current configuration (for advanced usage)
    pub fn create_raw_client(&self) -> Result<Client> {
        self.create_client()
    }

    /// Update default headers
    pub fn set_default_headers(&mut self, headers: HashMap<String, String>) {
        self.config.default_headers = headers;
    }

    /// Add a default header
    pub fn add_default_header(&mut self, key: String, value: String) {
        self.config.default_headers.insert(key, value);
    }
}

/// Builder for HttpClient with fluent API
pub struct HttpClientBuilder {
    config: ClientConfig,
}

impl HttpClientBuilder {
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
        }
    }

    pub fn emulation(mut self, emulation: Emulation) -> Self {
        self.config.emulation_strategy = EmulationStrategy::Fixed(emulation);
        self
    }

    pub fn emulation_strategy(mut self, strategy: EmulationStrategy) -> Self {
        self.config.emulation_strategy = strategy;
        self
    }

    pub fn random_emulation(mut self) -> Self {
        self.config.emulation_strategy = EmulationStrategy::Random;
        self
    }

    pub fn modern_browsers(mut self) -> Self {
        self.config.emulation_strategy = EmulationStrategy::modern_browsers();
        self
    }

    pub fn chrome_variants(mut self) -> Self {
        self.config.emulation_strategy = EmulationStrategy::chrome_variants();
        self
    }

    pub fn firefox_variants(mut self) -> Self {
        self.config.emulation_strategy = EmulationStrategy::firefox_variants();
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub fn user_agent(mut self, user_agent: String) -> Self {
        self.config.user_agent = Some(user_agent);
        self
    }

    pub fn default_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.config.default_headers = headers;
        self
    }

    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.config.retry_config = retry_config;
        self
    }

    pub fn rate_limit_config(mut self, rate_limit_config: RateLimitConfig) -> Self {
        self.config.rate_limit_config = rate_limit_config;
        self
    }

    pub fn build(self) -> Result<HttpClient> {
        HttpClient::with_config(self.config)
    }
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
