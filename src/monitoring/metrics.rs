use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, debug};

/// Types of metrics that can be collected
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Timer,
}

/// Metric values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(Vec<f64>),
    Timer(Duration),
}

/// Individual metric entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub metric_type: MetricType,
    pub value: MetricValue,
    pub timestamp: u64,
    pub tags: HashMap<String, String>,
}

/// Metrics report containing aggregated metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsReport {
    pub timestamp: u64,
    pub duration_seconds: u64,
    pub metrics: Vec<Metric>,
    pub summary: MetricsSummary,
}

/// Summary of key metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub average_response_time_ms: f64,
    pub throughput_per_second: f64,
    pub error_rate_percent: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
}

/// Metrics collector for gathering and aggregating metrics
pub struct MetricsCollector {
    metrics: Arc<RwLock<HashMap<String, Vec<Metric>>>>,
    start_time: Instant,
    enabled: bool,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
            enabled: true,
        }
    }

    /// Enable or disable metrics collection
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        if enabled {
            info!("Metrics collection enabled");
        } else {
            info!("Metrics collection disabled");
        }
    }

    /// Record a counter metric
    pub fn increment_counter(&self, name: &str, value: u64, tags: Option<HashMap<String, String>>) {
        if !self.enabled {
            return;
        }

        let metric = Metric {
            name: name.to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(value),
            timestamp: current_timestamp(),
            tags: tags.unwrap_or_default(),
        };

        self.record_metric(metric);
    }

    /// Record a gauge metric
    pub fn set_gauge(&self, name: &str, value: f64, tags: Option<HashMap<String, String>>) {
        if !self.enabled {
            return;
        }

        let metric = Metric {
            name: name.to_string(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(value),
            timestamp: current_timestamp(),
            tags: tags.unwrap_or_default(),
        };

        self.record_metric(metric);
    }

    /// Record a timer metric
    pub fn record_timer(&self, name: &str, duration: Duration, tags: Option<HashMap<String, String>>) {
        if !self.enabled {
            return;
        }

        let metric = Metric {
            name: name.to_string(),
            metric_type: MetricType::Timer,
            value: MetricValue::Timer(duration),
            timestamp: current_timestamp(),
            tags: tags.unwrap_or_default(),
        };

        self.record_metric(metric);
    }

    /// Record a histogram value
    pub fn record_histogram(&self, name: &str, value: f64, tags: Option<HashMap<String, String>>) {
        if !self.enabled {
            return;
        }

        let mut metrics = self.metrics.write().unwrap();
        let metric_list = metrics.entry(name.to_string()).or_insert_with(Vec::new);

        // Find existing histogram or create new one
        if let Some(existing) = metric_list.iter_mut().find(|m| {
            matches!(m.metric_type, MetricType::Histogram) && m.tags == *tags.as_ref().unwrap_or(&HashMap::new())
        }) {
            if let MetricValue::Histogram(ref mut values) = existing.value {
                values.push(value);
                existing.timestamp = current_timestamp();
            }
        } else {
            let metric = Metric {
                name: name.to_string(),
                metric_type: MetricType::Histogram,
                value: MetricValue::Histogram(vec![value]),
                timestamp: current_timestamp(),
                tags: tags.unwrap_or_default(),
            };
            metric_list.push(metric);
        }
    }

    /// Time a function execution and record the duration
    pub fn time_function<F, R>(&self, name: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let duration = start.elapsed();
        self.record_timer(name, duration, None);
        result
    }

    /// Time an async function execution
    pub async fn time_async_function<F, Fut, R>(&self, name: &str, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let start = Instant::now();
        let result = f().await;
        let duration = start.elapsed();
        self.record_timer(name, duration, None);
        result
    }

    /// Get current metrics snapshot
    pub fn get_metrics(&self) -> HashMap<String, Vec<Metric>> {
        self.metrics.read().unwrap().clone()
    }

    /// Generate a comprehensive metrics report
    pub fn generate_report(&self) -> MetricsReport {
        let metrics = self.get_metrics();
        let duration = self.start_time.elapsed();
        
        let mut all_metrics = Vec::new();
        let mut total_requests = 0u64;
        let mut successful_requests = 0u64;
        let mut failed_requests = 0u64;
        let mut response_times = Vec::new();
        let mut memory_usage = 0.0f64;
        let mut cpu_usage = 0.0f64;

        for (_, metric_list) in &metrics {
            for metric in metric_list {
                all_metrics.push(metric.clone());

                // Aggregate key metrics for summary
                match &metric.value {
                    MetricValue::Counter(value) => {
                        match metric.name.as_str() {
                            "requests_total" => total_requests += value,
                            "requests_successful" => successful_requests += value,
                            "requests_failed" => failed_requests += value,
                            _ => {}
                        }
                    }
                    MetricValue::Timer(duration) => {
                        if metric.name.contains("response_time") {
                            response_times.push(duration.as_millis() as f64);
                        }
                    }
                    MetricValue::Gauge(value) => {
                        match metric.name.as_str() {
                            "memory_usage_mb" => memory_usage = *value,
                            "cpu_usage_percent" => cpu_usage = *value,
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }

        let average_response_time = if response_times.is_empty() {
            0.0
        } else {
            response_times.iter().sum::<f64>() / response_times.len() as f64
        };

        let throughput = if duration.as_secs() > 0 {
            total_requests as f64 / duration.as_secs() as f64
        } else {
            0.0
        };

        let error_rate = if total_requests > 0 {
            (failed_requests as f64 / total_requests as f64) * 100.0
        } else {
            0.0
        };

        let summary = MetricsSummary {
            total_requests,
            successful_requests,
            failed_requests,
            average_response_time_ms: average_response_time,
            throughput_per_second: throughput,
            error_rate_percent: error_rate,
            memory_usage_mb: memory_usage,
            cpu_usage_percent: cpu_usage,
        };

        MetricsReport {
            timestamp: current_timestamp(),
            duration_seconds: duration.as_secs(),
            metrics: all_metrics,
            summary,
        }
    }

    /// Clear all collected metrics
    pub fn clear_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.clear();
        debug!("Metrics cleared");
    }

    /// Get metrics for a specific name
    pub fn get_metrics_by_name(&self, name: &str) -> Vec<Metric> {
        let metrics = self.metrics.read().unwrap();
        metrics.get(name).cloned().unwrap_or_default()
    }

    /// Record system metrics (memory, CPU, etc.)
    pub fn record_system_metrics(&self) {
        if !self.enabled {
            return;
        }

        // Record memory usage (simplified - in real implementation would use system APIs)
        let memory_usage = get_memory_usage_mb();
        self.set_gauge("memory_usage_mb", memory_usage, None);

        // Record CPU usage (simplified)
        let cpu_usage = get_cpu_usage_percent();
        self.set_gauge("cpu_usage_percent", cpu_usage, None);

        debug!("System metrics recorded: memory={:.1}MB, cpu={:.1}%", memory_usage, cpu_usage);
    }

    fn record_metric(&self, metric: Metric) {
        let mut metrics = self.metrics.write().unwrap();
        let metric_list = metrics.entry(metric.name.clone()).or_insert_with(Vec::new);
        metric_list.push(metric);
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsReport {
    /// Export metrics to JSON format
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Get metrics by type
    pub fn get_metrics_by_type(&self, metric_type: MetricType) -> Vec<&Metric> {
        self.metrics.iter()
            .filter(|m| m.metric_type == metric_type)
            .collect()
    }

    /// Check if performance is within acceptable thresholds
    pub fn is_performance_acceptable(&self) -> bool {
        self.summary.error_rate_percent < 5.0 &&
        self.summary.average_response_time_ms < 5000.0 &&
        self.summary.memory_usage_mb < 1024.0 &&
        self.summary.cpu_usage_percent < 80.0
    }
}

/// Get current timestamp in seconds since Unix epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Get current memory usage in MB (simplified implementation)
fn get_memory_usage_mb() -> f64 {
    // In a real implementation, this would use system APIs
    // For now, return a simulated value
    256.0 + (rand::random::<f64>() * 100.0)
}

/// Get current CPU usage percentage (simplified implementation)
fn get_cpu_usage_percent() -> f64 {
    // In a real implementation, this would use system APIs
    // For now, return a simulated value
    30.0 + (rand::random::<f64>() * 40.0)
}

/// Macro for easy metrics recording
#[macro_export]
macro_rules! record_metric {
    ($collector:expr, counter, $name:expr, $value:expr) => {
        $collector.increment_counter($name, $value, None);
    };
    ($collector:expr, gauge, $name:expr, $value:expr) => {
        $collector.set_gauge($name, $value, None);
    };
    ($collector:expr, timer, $name:expr, $duration:expr) => {
        $collector.record_timer($name, $duration, None);
    };
    ($collector:expr, histogram, $name:expr, $value:expr) => {
        $collector.record_histogram($name, $value, None);
    };
}

/// Macro for timing code blocks
#[macro_export]
macro_rules! time_block {
    ($collector:expr, $name:expr, $block:block) => {
        $collector.time_function($name, || $block)
    };
}
