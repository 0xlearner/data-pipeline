use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::debug;

/// Configuration for rate limiting
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_second: f64,
    pub burst_size: u32,
    pub enabled: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 10.0, // 10 requests per second
            burst_size: 5,             // Allow burst of 5 requests
            enabled: true,
        }
    }
}

/// Token bucket rate limiter implementation
pub struct RateLimiter {
    config: RateLimitConfig,
    state: Arc<Mutex<RateLimiterState>>,
}

struct RateLimiterState {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        let state = RateLimiterState {
            tokens: config.burst_size as f64,
            last_refill: Instant::now(),
        };

        Self {
            config,
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Wait until a request can be made according to rate limit
    pub async fn wait(&self) {
        if !self.config.enabled {
            return;
        }

        loop {
            let delay = {
                let mut state = self.state.lock().await;
                self.refill_tokens(&mut state);

                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    debug!("Rate limiter: Request allowed, {} tokens remaining", state.tokens);
                    return; // Request can proceed immediately
                } else {
                    // Calculate how long to wait for next token
                    let time_for_next_token = Duration::from_secs_f64(1.0 / self.config.requests_per_second);
                    debug!("Rate limiter: Request delayed, waiting {:?}", time_for_next_token);
                    time_for_next_token
                }
            };

            sleep(delay).await;
        }
    }

    /// Check if a request can be made without waiting
    pub async fn try_acquire(&self) -> bool {
        if !self.config.enabled {
            return true;
        }

        let mut state = self.state.lock().await;
        self.refill_tokens(&mut state);

        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            debug!("Rate limiter: Request acquired, {} tokens remaining", state.tokens);
            true
        } else {
            debug!("Rate limiter: Request denied, insufficient tokens");
            false
        }
    }

    /// Get current number of available tokens
    pub async fn available_tokens(&self) -> f64 {
        let mut state = self.state.lock().await;
        self.refill_tokens(&mut state);
        state.tokens
    }

    /// Refill tokens based on elapsed time
    fn refill_tokens(&self, state: &mut RateLimiterState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill);
        let tokens_to_add = elapsed.as_secs_f64() * self.config.requests_per_second;

        if tokens_to_add > 0.0 {
            state.tokens = (state.tokens + tokens_to_add).min(self.config.burst_size as f64);
            state.last_refill = now;
        }
    }
}

/// Predefined rate limit configurations
impl RateLimitConfig {
    /// Conservative rate limiting for production APIs
    pub fn conservative() -> Self {
        Self {
            requests_per_second: 5.0,
            burst_size: 2,
            enabled: true,
        }
    }

    /// Moderate rate limiting for most APIs
    pub fn moderate() -> Self {
        Self {
            requests_per_second: 10.0,
            burst_size: 5,
            enabled: true,
        }
    }

    /// Aggressive rate limiting for development
    pub fn aggressive() -> Self {
        Self {
            requests_per_second: 20.0,
            burst_size: 10,
            enabled: true,
        }
    }

    /// Disabled rate limiting
    pub fn disabled() -> Self {
        Self {
            requests_per_second: 0.0,
            burst_size: 0,
            enabled: false,
        }
    }

    /// Custom rate limiting
    pub fn custom(requests_per_second: f64, burst_size: u32) -> Self {
        Self {
            requests_per_second,
            burst_size,
            enabled: true,
        }
    }
}
