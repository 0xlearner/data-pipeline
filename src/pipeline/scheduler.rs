use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::{sleep, interval};
use tracing::{info, warn, error};

use super::orchestrator::{PipelineOrchestrator, PipelineOptions};

/// Pipeline scheduler for automated execution
pub struct PipelineScheduler {
    orchestrator: PipelineOrchestrator,
}

/// Scheduling configuration
#[derive(Debug, Clone)]
pub struct ScheduleConfig {
    pub interval: Duration,
    pub max_retries: usize,
    pub retry_delay: Duration,
    pub sources: Vec<String>, // Empty means all sources
    pub from_storage: bool,
}

/// Scheduled execution result
#[derive(Debug)]
pub struct ScheduledResult {
    pub execution_time: Instant,
    pub duration: Duration,
    pub success: bool,
    pub total_products: usize,
    pub error_message: Option<String>,
}

impl PipelineScheduler {
    /// Create a new pipeline scheduler
    pub async fn new() -> Result<Self> {
        let orchestrator = PipelineOrchestrator::new().await?;
        Ok(Self { orchestrator })
    }

    /// Run the pipeline on a schedule
    pub async fn run_scheduled(&self, config: ScheduleConfig) -> Result<()> {
        info!("Starting scheduled pipeline execution");
        info!("Interval: {:?}", config.interval);
        info!("Max retries: {}", config.max_retries);
        info!("Sources: {:?}", if config.sources.is_empty() { vec!["all".to_string()] } else { config.sources.clone() });

        let mut interval_timer = interval(config.interval);

        loop {
            interval_timer.tick().await;
            
            info!("🕐 Starting scheduled pipeline execution");
            let start_time = Instant::now();

            let result = self.execute_with_retries(&config).await;
            let duration = start_time.elapsed();

            match result {
                Ok(pipeline_result) => {
                    info!("✅ Scheduled execution completed successfully in {:?}", duration);
                    info!("📊 Processed {} products from {} sources", 
                        pipeline_result.total_products, 
                        pipeline_result.successful_sources
                    );
                }
                Err(e) => {
                    error!("❌ Scheduled execution failed after {:?}: {}", duration, e);
                }
            }

            info!("⏰ Next execution in {:?}", config.interval);
        }
    }

    /// Execute a single pipeline run
    pub async fn execute_once(&self, options: &PipelineOptions) -> Result<ScheduledResult> {
        let start_time = Instant::now();
        
        match self.orchestrator.run(options).await {
            Ok(result) => {
                Ok(ScheduledResult {
                    execution_time: start_time,
                    duration: start_time.elapsed(),
                    success: true,
                    total_products: result.total_products,
                    error_message: None,
                })
            }
            Err(e) => {
                Ok(ScheduledResult {
                    execution_time: start_time,
                    duration: start_time.elapsed(),
                    success: false,
                    total_products: 0,
                    error_message: Some(e.to_string()),
                })
            }
        }
    }

    /// Execute with retry logic
    async fn execute_with_retries(&self, config: &ScheduleConfig) -> Result<super::orchestrator::PipelineResult> {
        let mut last_error = None;

        for attempt in 1..=config.max_retries {
            let options = PipelineOptions {
                from_storage: config.from_storage,
                specific_source: None, // Process all sources in scheduled mode
                batch_size: None, // Use default batch sizing
                memory_efficient: false,
            };

            match self.orchestrator.run(&options).await {
                Ok(result) => {
                    if attempt > 1 {
                        info!("✅ Pipeline succeeded on attempt {}/{}", attempt, config.max_retries);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < config.max_retries {
                        warn!("⚠️  Pipeline failed on attempt {}/{}: {}", attempt, config.max_retries, last_error.as_ref().unwrap());
                        warn!("🔄 Retrying in {:?}...", config.retry_delay);
                        sleep(config.retry_delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Run a health check on all sources
    pub async fn health_check(&self) -> Result<HealthCheckResult> {
        info!("🏥 Running pipeline health check");

        let available_sources = self.orchestrator.get_available_sources();
        let mut healthy_sources = Vec::new();
        let mut unhealthy_sources = Vec::new();

        for source in &available_sources {
            let options = PipelineOptions {
                from_storage: true, // Check storage health
                specific_source: Some(source.clone()),
                batch_size: Some(10), // Small batch for health check
                memory_efficient: false,
            };

            match self.orchestrator.run(&options).await {
                Ok(_) => {
                    info!("✅ Source '{}' is healthy", source);
                    healthy_sources.push(source.clone());
                }
                Err(e) => {
                    warn!("❌ Source '{}' is unhealthy: {}", source, e);
                    unhealthy_sources.push((source.clone(), e.to_string()));
                }
            }
        }

        let health_status = if unhealthy_sources.is_empty() {
            HealthStatus::Healthy
        } else if healthy_sources.is_empty() {
            HealthStatus::Critical
        } else {
            HealthStatus::Degraded
        };

        Ok(HealthCheckResult {
            status: health_status,
            total_sources: available_sources.len(),
            healthy_sources,
            unhealthy_sources,
            check_time: Instant::now(),
        })
    }
}

/// Health check result
#[derive(Debug)]
pub struct HealthCheckResult {
    pub status: HealthStatus,
    pub total_sources: usize,
    pub healthy_sources: Vec<String>,
    pub unhealthy_sources: Vec<(String, String)>, // (source_name, error_message)
    pub check_time: Instant,
}

/// Overall health status
#[derive(Debug, PartialEq)]
pub enum HealthStatus {
    Healthy,   // All sources working
    Degraded,  // Some sources failing
    Critical,  // All sources failing
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600), // 1 hour
            max_retries: 3,
            retry_delay: Duration::from_secs(300), // 5 minutes
            sources: Vec::new(), // All sources
            from_storage: false, // Fetch from APIs by default
        }
    }
}

impl HealthCheckResult {
    /// Get the health percentage
    pub fn health_percentage(&self) -> f64 {
        if self.total_sources == 0 {
            100.0
        } else {
            (self.healthy_sources.len() as f64 / self.total_sources as f64) * 100.0
        }
    }

    /// Check if the system is considered healthy
    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }
}
