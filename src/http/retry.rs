use anyhow::Result;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

/// Retry policy implementation with exponential backoff
pub struct RetryPolicy {
    config: RetryConfig,
}

impl RetryPolicy {
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    /// Execute a function with retry logic
    pub async fn execute<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut last_error = None;

        for attempt in 1..=self.config.max_attempts {
            match operation().await {
                Ok(result) => {
                    if attempt > 1 {
                        debug!("Operation succeeded on attempt {}", attempt);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    last_error = Some(error);
                    
                    if attempt < self.config.max_attempts {
                        let delay = self.calculate_delay(attempt);
                        warn!(
                            "Operation failed on attempt {} of {}, retrying in {:?}: {}",
                            attempt, self.config.max_attempts, delay, last_error.as_ref().unwrap()
                        );
                        sleep(delay).await;
                    } else {
                        warn!(
                            "Operation failed on final attempt {} of {}",
                            attempt, self.config.max_attempts
                        );
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Calculate delay for the given attempt number
    fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_delay_ms = self.config.base_delay.as_millis() as f64;
        let multiplier = self.config.backoff_multiplier.powi((attempt - 1) as i32);
        let delay_ms = base_delay_ms * multiplier;

        let mut delay = Duration::from_millis(delay_ms as u64);

        // Apply maximum delay limit
        if delay > self.config.max_delay {
            delay = self.config.max_delay;
        }

        // Apply jitter if enabled
        if self.config.jitter {
            delay = self.apply_jitter(delay);
        }

        delay
    }

    /// Apply jitter to delay to avoid thundering herd
    fn apply_jitter(&self, delay: Duration) -> Duration {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let jitter_factor = rng.gen_range(0.5..1.5);
        let jittered_ms = (delay.as_millis() as f64 * jitter_factor) as u64;
        Duration::from_millis(jittered_ms)
    }
}

/// Predefined retry configurations for common scenarios
impl RetryConfig {
    /// Conservative retry policy for production
    pub fn conservative() -> Self {
        Self {
            max_attempts: 2,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }

    /// Aggressive retry policy for development/testing
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 1.5,
            jitter: true,
        }
    }

    /// No retry policy
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            base_delay: Duration::from_millis(0),
            max_delay: Duration::from_millis(0),
            backoff_multiplier: 1.0,
            jitter: false,
        }
    }
}
