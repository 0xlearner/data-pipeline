use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};
use tracing::{error, info, warn};

use crate::config::concurrency_config::{DomainRateLimit, EmulationStrategyConfig};
use crate::config::{
    ConcurrencyConfig, ConcurrencyLimits, ConfigManager, EmulationConfig, GlobalRateLimitConfig,
    GlobalRetryConfig, HttpConfig, SourceOverride,
};
use crate::http::GlobalHttpClientFactory;

/// Configuration management CLI commands
#[derive(Debug, Args)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Show current configuration
    Show {
        /// Show configuration for a specific source
        #[arg(short, long)]
        source: Option<String>,
        /// Show only specific section (http, retry, rate_limit, emulation, concurrency)
        #[arg(long)]
        section: Option<String>,
        /// Output format (json, toml, table)
        #[arg(short, long, default_value = "table")]
        format: String,
    },
    /// Set configuration values
    Set {
        /// Configuration key (e.g., http.timeout_seconds, rate_limit.requests_per_second)
        key: String,
        /// Configuration value
        value: String,
        /// Apply to specific source only
        #[arg(short, long)]
        source: Option<String>,
    },
    /// Get a specific configuration value
    Get {
        /// Configuration key
        key: String,
        /// Get from specific source configuration
        #[arg(short, long)]
        source: Option<String>,
    },
    /// Validate current configuration
    Validate,
    /// Show configuration statistics
    Stats,
    /// Reset configuration to defaults
    Reset {
        /// Reset only specific section
        #[arg(short, long)]
        section: Option<String>,
        /// Confirm reset without prompt
        #[arg(short, long)]
        yes: bool,
    },
    /// Manage source-specific overrides
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    /// Manage domain-specific rate limits
    Domain {
        #[command(subcommand)]
        command: DomainCommand,
    },
    /// Test configuration with a sample request
    Test {
        /// URL to test
        url: String,
        /// Source configuration to use
        #[arg(short, long)]
        source: Option<String>,
    },
    /// Export configuration to file
    Export {
        /// Output file path
        output: String,
        /// Export format (toml, json)
        #[arg(short, long, default_value = "toml")]
        format: String,
    },
    /// Import configuration from file
    Import {
        /// Input file path
        input: String,
        /// Backup current configuration before importing
        #[arg(short, long, default_value = "true")]
        backup: bool,
    },
    /// Show system health status
    Health,
    /// Reload configuration from file
    Reload,
}

#[derive(Debug, Subcommand)]
pub enum SourceCommand {
    /// List all sources with overrides
    List,
    /// Show configuration for a specific source
    Show {
        /// Source name
        source: String,
    },
    /// Set override for a specific source
    Set {
        /// Source name
        source: String,
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
    /// Remove override for a source
    Remove {
        /// Source name
        source: String,
        /// Specific key to remove (if not specified, removes all overrides)
        #[arg(short, long)]
        key: Option<String>,
    },
    /// Create a new source override
    Create {
        /// Source name
        source: String,
        /// Template to use (conservative, moderate, aggressive)
        #[arg(short, long)]
        template: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum DomainCommand {
    /// List all domain rate limits
    List,
    /// Set rate limit for a domain
    Set {
        /// Domain name
        domain: String,
        /// Requests per second
        #[arg(short, long)]
        rps: f64,
        /// Burst size
        #[arg(short, long)]
        burst: u32,
        /// Enable rate limiting for this domain
        #[arg(long, default_value = "true")]
        enabled: bool,
    },
    /// Remove rate limit for a domain
    Remove {
        /// Domain name
        domain: String,
    },
    /// Show rate limit for a domain
    Show {
        /// Domain name
        domain: String,
    },
}

/// Configuration CLI handler
pub struct ConfigCli;

impl ConfigCli {
    /// Execute configuration commands
    pub async fn execute(args: ConfigArgs) -> Result<()> {
        match args.command {
            ConfigCommand::Show {
                source,
                section,
                format,
            } => Self::show_config(source, section, format).await,
            ConfigCommand::Set { key, value, source } => Self::set_config(key, value, source).await,
            ConfigCommand::Get { key, source } => Self::get_config(key, source).await,
            ConfigCommand::Validate => Self::validate_config().await,
            ConfigCommand::Stats => Self::show_stats().await,
            ConfigCommand::Reset { section, yes } => Self::reset_config(section, yes).await,
            ConfigCommand::Source { command } => Self::handle_source_command(command).await,
            ConfigCommand::Domain { command } => Self::handle_domain_command(command).await,
            ConfigCommand::Test { url, source } => Self::test_config(url, source).await,
            ConfigCommand::Export { output, format } => Self::export_config(output, format).await,
            ConfigCommand::Import { input, backup } => Self::import_config(input, backup).await,
            ConfigCommand::Health => Self::show_health().await,
            ConfigCommand::Reload => Self::reload_config().await,
        }
    }

    async fn show_config(
        source: Option<String>,
        section: Option<String>,
        format: String,
    ) -> Result<()> {
        let config_manager = ConfigManager::global().await;

        let config = match source {
            Some(src) => config_manager.get_source_config(&src),
            None => config_manager.get_config(),
        };

        match format.as_str() {
            "json" => {
                let json = serde_json::to_string_pretty(&config)?;
                println!("{}", json);
            }
            "toml" => {
                let toml = toml::to_string_pretty(&config)?;
                println!("{}", toml);
            }
            "table" => {
                Self::display_config_table(&config, section.as_deref())?;
            }
            _ => return Err(anyhow!("Unsupported format: {}", format)),
        }

        Ok(())
    }

    async fn set_config(key: String, value: String, source: Option<String>) -> Result<()> {
        // Parse the key path (e.g., "http.timeout_seconds")
        let parts: Vec<&str> = key.split('.').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid key format. Use 'section.key' format"));
        }

        let section = parts[0];
        let field = parts[1];

        if let Some(source_name) = source {
            // Update source override
            Self::update_source_override(source_name, section, field, value).await
        } else {
            // Update global configuration
            Self::update_global_config(section, field, value).await
        }
    }

    async fn update_global_config(section: &str, field: &str, value: String) -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let mut config = config_manager.get_config();

        match section {
            "http" => Self::update_http_config(&mut config.http, field, value.clone())?,
            "retry" => Self::update_retry_config(&mut config.retry, field, value.clone())?,
            "rate_limit" => {
                Self::update_rate_limit_config(&mut config.rate_limit, field, value.clone())?
            }
            "emulation" => {
                Self::update_emulation_config(&mut config.emulation, field, value.clone())?
            }
            "concurrency" => {
                Self::update_concurrency_config(&mut config.concurrency, field, value.clone())?
            }
            _ => return Err(anyhow!("Unknown section: {}", section)),
        }

        config_manager.update_config(config)?;
        info!("✅ Updated {}.{} = {}", section, field, value);
        Ok(())
    }

    async fn update_source_override(
        source_name: String,
        section: &str,
        field: &str,
        value: String,
    ) -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let mut config = config_manager.get_config();

        let override_config = config
            .source_overrides
            .entry(source_name.clone())
            .or_insert_with(|| SourceOverride {
                http: None,
                retry: None,
                rate_limit: None,
                emulation: None,
                concurrency: None,
            });

        match section {
            "http" => {
                let mut http_config = override_config
                    .http
                    .clone()
                    .unwrap_or_else(HttpConfig::default);
                Self::update_http_config(&mut http_config, field, value.clone())?;
                override_config.http = Some(http_config);
            }
            "retry" => {
                let mut retry_config = override_config
                    .retry
                    .clone()
                    .unwrap_or_else(GlobalRetryConfig::default);
                Self::update_retry_config(&mut retry_config, field, value.clone())?;
                override_config.retry = Some(retry_config);
            }
            "rate_limit" => {
                let mut rate_limit_config = override_config
                    .rate_limit
                    .clone()
                    .unwrap_or_else(GlobalRateLimitConfig::default);
                Self::update_rate_limit_config(&mut rate_limit_config, field, value.clone())?;
                override_config.rate_limit = Some(rate_limit_config);
            }
            "emulation" => {
                let mut emulation_config = override_config
                    .emulation
                    .clone()
                    .unwrap_or_else(EmulationConfig::default);
                Self::update_emulation_config(&mut emulation_config, field, value.clone())?;
                override_config.emulation = Some(emulation_config);
            }
            "concurrency" => {
                let mut concurrency_config = override_config
                    .concurrency
                    .clone()
                    .unwrap_or_else(ConcurrencyLimits::default);
                Self::update_concurrency_config(&mut concurrency_config, field, value.clone())?;
                override_config.concurrency = Some(concurrency_config);
            }
            _ => return Err(anyhow!("Unknown section: {}", section)),
        }

        config_manager.update_config(config)?;
        info!(
            "✅ Updated source override {}: {}.{} = {}",
            source_name, section, field, value
        );
        Ok(())
    }

    fn update_http_config(config: &mut HttpConfig, field: &str, value: String) -> Result<()> {
        match field {
            "timeout_seconds" => config.timeout_seconds = value.parse()?,
            "connect_timeout_seconds" => config.connect_timeout_seconds = value.parse()?,
            "read_timeout_seconds" => config.read_timeout_seconds = value.parse()?,
            "max_redirects" => config.max_redirects = value.parse()?,
            "http2" => config.http2 = value.parse()?,
            "cookies" => config.cookies = value.parse()?,
            "user_agent" => config.user_agent = if value.is_empty() { None } else { Some(value) },
            _ => return Err(anyhow!("Unknown HTTP field: {}", field)),
        }
        Ok(())
    }

    fn update_retry_config(
        config: &mut GlobalRetryConfig,
        field: &str,
        value: String,
    ) -> Result<()> {
        match field {
            "max_attempts" => config.max_attempts = value.parse()?,
            "base_delay_ms" => config.base_delay_ms = value.parse()?,
            "max_delay_seconds" => config.max_delay_seconds = value.parse()?,
            "backoff_multiplier" => config.backoff_multiplier = value.parse()?,
            "jitter" => config.jitter = value.parse()?,
            "retry_on_connection_error" => config.retry_on_connection_error = value.parse()?,
            "retry_on_timeout" => config.retry_on_timeout = value.parse()?,
            _ => return Err(anyhow!("Unknown retry field: {}", field)),
        }
        Ok(())
    }

    fn update_rate_limit_config(
        config: &mut GlobalRateLimitConfig,
        field: &str,
        value: String,
    ) -> Result<()> {
        match field {
            "enabled" => config.enabled = value.parse()?,
            "requests_per_second" => config.requests_per_second = value.parse()?,
            "burst_size" => config.burst_size = value.parse()?,
            _ => return Err(anyhow!("Unknown rate limit field: {}", field)),
        }
        Ok(())
    }

    fn update_emulation_config(
        config: &mut EmulationConfig,
        field: &str,
        value: String,
    ) -> Result<()> {
        match field {
            "strategy" => {
                config.strategy = match value.as_str() {
                    "random" => EmulationStrategyConfig::Random,
                    "rotate" => EmulationStrategyConfig::Rotate,
                    "modern_browsers" => EmulationStrategyConfig::ModernBrowsers,
                    "chrome_variants" => EmulationStrategyConfig::ChromeVariants,
                    "firefox_variants" => EmulationStrategyConfig::FirefoxVariants,
                    _ => EmulationStrategyConfig::Fixed(value),
                };
            }
            "rotate_user_agents" => config.rotate_user_agents = value.parse()?,
            _ => return Err(anyhow!("Unknown emulation field: {}", field)),
        }
        Ok(())
    }

    fn update_concurrency_config(
        config: &mut ConcurrencyLimits,
        field: &str,
        value: String,
    ) -> Result<()> {
        match field {
            "max_concurrent_requests" => config.max_concurrent_requests = value.parse()?,
            "max_concurrent_per_domain" => config.max_concurrent_per_domain = value.parse()?,
            "max_connections" => config.max_connections = value.parse()?,
            "max_idle_connections" => config.max_idle_connections = value.parse()?,
            "idle_timeout_seconds" => config.idle_timeout_seconds = value.parse()?,
            "http2_multiplexing" => config.http2_multiplexing = value.parse()?,
            _ => return Err(anyhow!("Unknown concurrency field: {}", field)),
        }
        Ok(())
    }

    async fn get_config(key: String, source: Option<String>) -> Result<()> {
        let config_manager = ConfigManager::global().await;

        let config = match source {
            Some(src) => config_manager.get_source_config(&src),
            None => config_manager.get_config(),
        };

        let parts: Vec<&str> = key.split('.').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid key format. Use 'section.key' format"));
        }

        let section = parts[0];
        let field = parts[1];

        let value = match section {
            "http" => Self::get_http_field(&config.http, field)?,
            "retry" => Self::get_retry_field(&config.retry, field)?,
            "rate_limit" => Self::get_rate_limit_field(&config.rate_limit, field)?,
            "emulation" => Self::get_emulation_field(&config.emulation, field)?,
            "concurrency" => Self::get_concurrency_field(&config.concurrency, field)?,
            _ => return Err(anyhow!("Unknown section: {}", section)),
        };

        println!("{}", value);
        Ok(())
    }

    fn get_http_field(config: &HttpConfig, field: &str) -> Result<String> {
        match field {
            "timeout_seconds" => Ok(config.timeout_seconds.to_string()),
            "connect_timeout_seconds" => Ok(config.connect_timeout_seconds.to_string()),
            "read_timeout_seconds" => Ok(config.read_timeout_seconds.to_string()),
            "max_redirects" => Ok(config.max_redirects.to_string()),
            "http2" => Ok(config.http2.to_string()),
            "cookies" => Ok(config.cookies.to_string()),
            "user_agent" => Ok(config.user_agent.as_deref().unwrap_or("").to_string()),
            _ => Err(anyhow!("Unknown HTTP field: {}", field)),
        }
    }

    fn get_retry_field(config: &GlobalRetryConfig, field: &str) -> Result<String> {
        match field {
            "max_attempts" => Ok(config.max_attempts.to_string()),
            "base_delay_ms" => Ok(config.base_delay_ms.to_string()),
            "max_delay_seconds" => Ok(config.max_delay_seconds.to_string()),
            "backoff_multiplier" => Ok(config.backoff_multiplier.to_string()),
            "jitter" => Ok(config.jitter.to_string()),
            "retry_on_connection_error" => Ok(config.retry_on_connection_error.to_string()),
            "retry_on_timeout" => Ok(config.retry_on_timeout.to_string()),
            _ => Err(anyhow!("Unknown retry field: {}", field)),
        }
    }

    fn get_rate_limit_field(config: &GlobalRateLimitConfig, field: &str) -> Result<String> {
        match field {
            "enabled" => Ok(config.enabled.to_string()),
            "requests_per_second" => Ok(config.requests_per_second.to_string()),
            "burst_size" => Ok(config.burst_size.to_string()),
            _ => Err(anyhow!("Unknown rate limit field: {}", field)),
        }
    }

    fn get_emulation_field(config: &EmulationConfig, field: &str) -> Result<String> {
        match field {
            "strategy" => Ok(format!("{:?}", config.strategy)),
            "rotate_user_agents" => Ok(config.rotate_user_agents.to_string()),
            _ => Err(anyhow!("Unknown emulation field: {}", field)),
        }
    }

    fn get_concurrency_field(config: &ConcurrencyLimits, field: &str) -> Result<String> {
        match field {
            "max_concurrent_requests" => Ok(config.max_concurrent_requests.to_string()),
            "max_concurrent_per_domain" => Ok(config.max_concurrent_per_domain.to_string()),
            "max_connections" => Ok(config.max_connections.to_string()),
            "max_idle_connections" => Ok(config.max_idle_connections.to_string()),
            "idle_timeout_seconds" => Ok(config.idle_timeout_seconds.to_string()),
            "http2_multiplexing" => Ok(config.http2_multiplexing.to_string()),
            _ => Err(anyhow!("Unknown concurrency field: {}", field)),
        }
    }

    async fn validate_config() -> Result<()> {
        let config_manager = ConfigManager::global().await;

        match config_manager.validate() {
            Ok(()) => {
                info!("✅ Configuration is valid");
            }
            Err(e) => {
                error!("❌ Configuration validation failed: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    async fn show_stats() -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let stats = config_manager.get_stats();

        stats.display();
        Ok(())
    }

    async fn reset_config(section: Option<String>, yes: bool) -> Result<()> {
        if !yes {
            println!("Are you sure you want to reset configuration? This cannot be undone. [y/N]");
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
                println!("Cancelled.");
                return Ok(());
            }
        }

        let config_manager = ConfigManager::global().await;
        let mut config = config_manager.get_config();

        match section.as_deref() {
            Some("http") => config.http = HttpConfig::default(),
            Some("retry") => config.retry = GlobalRetryConfig::default(),
            Some("rate_limit") => config.rate_limit = GlobalRateLimitConfig::default(),
            Some("emulation") => config.emulation = EmulationConfig::default(),
            Some("concurrency") => config.concurrency = ConcurrencyLimits::default(),
            Some(s) => return Err(anyhow!("Unknown section: {}", s)),
            None => config = ConcurrencyConfig::default(),
        }

        config_manager.update_config(config)?;
        info!("✅ Configuration reset to defaults");
        Ok(())
    }

    async fn handle_source_command(command: SourceCommand) -> Result<()> {
        match command {
            SourceCommand::List => Self::list_sources().await,
            SourceCommand::Show { source } => Self::show_source(&source).await,
            SourceCommand::Set { source, key, value } => {
                Self::set_source_config(source, key, value).await
            }
            SourceCommand::Remove { source, key } => Self::remove_source_config(source, key).await,
            SourceCommand::Create { source, template } => {
                Self::create_source_override(source, template).await
            }
        }
    }

    async fn list_sources() -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let sources = config_manager.get_configured_sources();

        if sources.is_empty() {
            info!("No source overrides configured");
        } else {
            info!("Configured source overrides:");
            for source in sources {
                println!("  - {}", source);
            }
        }

        Ok(())
    }

    async fn show_source(source_name: &str) -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let config = config_manager.get_source_config(source_name);

        println!("Configuration for source '{}':", source_name);
        Self::display_config_table(&config, None)?;
        Ok(())
    }

    async fn set_source_config(source: String, key: String, value: String) -> Result<()> {
        Self::set_config(key, value, Some(source)).await
    }

    async fn remove_source_config(source: String, key: Option<String>) -> Result<()> {
        let config_manager = ConfigManager::global().await;

        match key {
            Some(_) => {
                // TODO: Implement removing specific keys from source overrides
                warn!(
                    "Removing specific keys not yet implemented. Use 'remove' without --key to remove all overrides."
                );
            }
            None => {
                config_manager.remove_source_override(&source)?;
                info!("✅ Removed all overrides for source: {}", source);
            }
        }

        Ok(())
    }

    async fn create_source_override(source: String, template: Option<String>) -> Result<()> {
        let config_manager = ConfigManager::global().await;

        let override_config = match template.as_deref() {
            Some("conservative") => SourceOverride {
                retry: Some(GlobalRetryConfig {
                    max_attempts: 2,
                    base_delay_ms: 1000,
                    ..GlobalRetryConfig::default()
                }),
                rate_limit: Some(GlobalRateLimitConfig {
                    requests_per_second: 3.0,
                    burst_size: 2,
                    ..GlobalRateLimitConfig::default()
                }),
                concurrency: Some(ConcurrencyLimits {
                    max_concurrent_per_domain: 3,
                    ..ConcurrencyLimits::default()
                }),
                ..SourceOverride {
                    http: None,
                    retry: None,
                    rate_limit: None,
                    emulation: None,
                    concurrency: None,
                }
            },
            Some("moderate") => SourceOverride {
                rate_limit: Some(GlobalRateLimitConfig {
                    requests_per_second: 10.0,
                    burst_size: 5,
                    ..GlobalRateLimitConfig::default()
                }),
                concurrency: Some(ConcurrencyLimits {
                    max_concurrent_per_domain: 8,
                    ..ConcurrencyLimits::default()
                }),
                ..SourceOverride {
                    http: None,
                    retry: None,
                    rate_limit: None,
                    emulation: None,
                    concurrency: None,
                }
            },
            Some("aggressive") => SourceOverride {
                retry: Some(GlobalRetryConfig {
                    max_attempts: 5,
                    base_delay_ms: 200,
                    ..GlobalRetryConfig::default()
                }),
                rate_limit: Some(GlobalRateLimitConfig {
                    requests_per_second: 20.0,
                    burst_size: 10,
                    ..GlobalRateLimitConfig::default()
                }),
                concurrency: Some(ConcurrencyLimits {
                    max_concurrent_per_domain: 15,
                    ..ConcurrencyLimits::default()
                }),
                ..SourceOverride {
                    http: None,
                    retry: None,
                    rate_limit: None,
                    emulation: None,
                    concurrency: None,
                }
            },
            _ => SourceOverride {
                http: None,
                retry: None,
                rate_limit: None,
                emulation: None,
                concurrency: None,
            },
        };

        config_manager.update_source_override(&source, override_config)?;
        let template_name = template.unwrap_or_else(|| "default".to_string());
        info!(
            "✅ Created source override for: {} (template: {})",
            source, template_name
        );
        Ok(())
    }

    async fn handle_domain_command(command: DomainCommand) -> Result<()> {
        match command {
            DomainCommand::List => Self::list_domains().await,
            DomainCommand::Set {
                domain,
                rps,
                burst,
                enabled,
            } => Self::set_domain_rate_limit(domain, rps, burst, enabled).await,
            DomainCommand::Remove { domain } => Self::remove_domain_rate_limit(domain).await,
            DomainCommand::Show { domain } => Self::show_domain_rate_limit(domain).await,
        }
    }

    async fn list_domains() -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let config = config_manager.get_config();

        if config.rate_limit.domain_limits.is_empty() {
            info!("No domain-specific rate limits configured");
        } else {
            info!("Domain-specific rate limits:");
            for (domain, limits) in &config.rate_limit.domain_limits {
                println!(
                    "  - {}: {:.1} req/s, burst: {}, enabled: {}",
                    domain, limits.requests_per_second, limits.burst_size, limits.enabled
                );
            }
        }

        Ok(())
    }

    async fn set_domain_rate_limit(
        domain: String,
        rps: f64,
        burst: u32,
        enabled: bool,
    ) -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let mut config = config_manager.get_config();

        let domain_limit = DomainRateLimit {
            requests_per_second: rps,
            burst_size: burst,
            enabled,
        };

        config
            .rate_limit
            .domain_limits
            .insert(domain.clone(), domain_limit);
        config_manager.update_config(config)?;

        info!(
            "✅ Set domain rate limit for {}: {:.1} req/s, burst: {}, enabled: {}",
            domain, rps, burst, enabled
        );
        Ok(())
    }

    async fn remove_domain_rate_limit(domain: String) -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let mut config = config_manager.get_config();

        config.rate_limit.domain_limits.remove(&domain);
        config_manager.update_config(config)?;

        info!("✅ Removed domain rate limit for: {}", domain);
        Ok(())
    }

    async fn show_domain_rate_limit(domain: String) -> Result<()> {
        let config_manager = ConfigManager::global().await;

        if let Some(limit) = config_manager.domain_rate_limit(&domain) {
            println!("Domain rate limit for '{}':", domain);
            println!("  Requests per second: {:.1}", limit.requests_per_second);
            println!("  Burst size: {}", limit.burst_size);
            println!("  Enabled: {}", limit.enabled);
        } else {
            info!("No specific rate limit configured for domain: {}", domain);
            let global = config_manager.rate_limit_config();
            println!(
                "Using global rate limit: {:.1} req/s, burst: {}",
                global.requests_per_second, global.burst_size
            );
        }

        Ok(())
    }

    async fn test_config(url: String, source: Option<String>) -> Result<()> {
        info!("Testing configuration with URL: {}", url);

        let factory = GlobalHttpClientFactory::instance().await?;
        let source_name = source.as_deref().unwrap_or("__test__");

        let start = std::time::Instant::now();
        match factory.get_json(source_name, &url).await {
            Ok(_) => {
                let duration = start.elapsed();
                info!("✅ Test successful! Response time: {:?}", duration);
            }
            Err(e) => {
                error!("❌ Test failed: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    async fn export_config(output: String, format: String) -> Result<()> {
        let config_manager = ConfigManager::global().await;
        let config = config_manager.get_config();

        let content = match format.as_str() {
            "toml" => toml::to_string_pretty(&config)?,
            "json" => serde_json::to_string_pretty(&config)?,
            _ => return Err(anyhow!("Unsupported export format: {}", format)),
        };

        std::fs::write(&output, content)?;
        info!("✅ Configuration exported to: {}", output);
        Ok(())
    }

    async fn import_config(input: String, backup: bool) -> Result<()> {
        let config_manager = ConfigManager::global().await;

        // Create backup if requested
        if backup {
            let backup_path = format!("{}.backup", config_manager.config_path());
            let current_config = config_manager.get_config();
            current_config.to_file(&backup_path)?;
            info!("✅ Created backup at: {}", backup_path);
        }

        // Read and parse new configuration
        let content = std::fs::read_to_string(&input)?;
        let new_config: ConcurrencyConfig = if input.ends_with(".toml") {
            toml::from_str(&content)?
        } else if input.ends_with(".json") {
            serde_json::from_str(&content)?
        } else {
            return Err(anyhow!("Unsupported file format. Use .toml or .json"));
        };

        // Validate the new configuration by attempting to load it
        match new_config.to_retry_config().max_attempts {
            0 => {
                return Err(anyhow!(
                    "Invalid retry configuration: max_attempts must be > 0"
                ));
            }
            _ => {}
        }
        if new_config.rate_limit.enabled && new_config.rate_limit.requests_per_second <= 0.0 {
            return Err(anyhow!(
                "Invalid rate limit: requests_per_second must be > 0"
            ));
        }

        // Apply the new configuration
        config_manager.update_config(new_config)?;
        info!("✅ Configuration imported from: {}", input);
        Ok(())
    }

    async fn show_health() -> Result<()> {
        let factory = GlobalHttpClientFactory::instance().await?;
        let health = factory.health_check();
        let stats = factory.get_concurrency_stats();

        println!("=== System Health Status ===");
        println!("Overall Status: {:?}", health.status);
        println!(
            "Global Utilization: {:.1}%",
            health.global_utilization * 100.0
        );
        println!("Message: {}", health.message);
        println!();
        println!("=== Concurrency Statistics ===");
        println!(
            "Global Available Permits: {}/{}",
            stats.global_available_permits, stats.global_total_permits
        );
        println!("Cached HTTP Clients: {}", stats.cached_clients);
        println!();

        if !stats.domain_stats.is_empty() {
            println!("=== Domain Statistics ===");
            for domain_stat in &stats.domain_stats {
                println!(
                    "{}: {}/{} permits",
                    domain_stat.domain, domain_stat.available_permits, domain_stat.total_permits
                );
            }
        }

        match health.status {
            crate::http::Health::Healthy => info!("✅ System is healthy"),
            crate::http::Health::Warning => warn!("⚠️  System has warnings"),
            crate::http::Health::Critical => error!("❌ System is in critical state"),
        }

        Ok(())
    }

    async fn reload_config() -> Result<()> {
        let config_manager = ConfigManager::global().await;
        config_manager.reload()?;

        // Also reload the HTTP client factory
        let factory = GlobalHttpClientFactory::instance().await?;
        factory.reload_configuration().await?;

        info!("✅ Configuration reloaded successfully");
        Ok(())
    }

    fn display_config_table(
        config: &ConcurrencyConfig,
        section_filter: Option<&str>,
    ) -> Result<()> {
        match section_filter {
            Some("http") => Self::display_http_section(&config.http),
            Some("retry") => Self::display_retry_section(&config.retry),
            Some("rate_limit") => Self::display_rate_limit_section(&config.rate_limit),
            Some("emulation") => Self::display_emulation_section(&config.emulation),
            Some("concurrency") => Self::display_concurrency_section(&config.concurrency),
            Some(s) => return Err(anyhow!("Unknown section: {}", s)),
            None => {
                println!("=== HTTP Configuration ===");
                Self::display_http_section(&config.http);
                println!();

                println!("=== Retry Configuration ===");
                Self::display_retry_section(&config.retry);
                println!();

                println!("=== Rate Limiting Configuration ===");
                Self::display_rate_limit_section(&config.rate_limit);
                println!();

                println!("=== Browser Emulation Configuration ===");
                Self::display_emulation_section(&config.emulation);
                println!();

                println!("=== Concurrency Configuration ===");
                Self::display_concurrency_section(&config.concurrency);

                if !config.source_overrides.is_empty() {
                    println!();
                    println!("=== Source Overrides ===");
                    for source_name in config.source_overrides.keys() {
                        println!("  - {}", source_name);
                    }
                }
            }
        }
        Ok(())
    }

    fn display_http_section(config: &HttpConfig) {
        println!("  Timeout: {}s", config.timeout_seconds);
        println!("  Connect Timeout: {}s", config.connect_timeout_seconds);
        println!("  Read Timeout: {}s", config.read_timeout_seconds);
        println!("  Max Redirects: {}", config.max_redirects);
        println!("  HTTP/2 Enabled: {}", config.http2);
        println!("  Cookies Enabled: {}", config.cookies);
        if let Some(ua) = &config.user_agent {
            println!("  User Agent: {}", ua);
        }
        if !config.default_headers.is_empty() {
            println!(
                "  Default Headers: {} configured",
                config.default_headers.len()
            );
        }
    }

    fn display_retry_section(config: &GlobalRetryConfig) {
        println!("  Max Attempts: {}", config.max_attempts);
        println!("  Base Delay: {}ms", config.base_delay_ms);
        println!("  Max Delay: {}s", config.max_delay_seconds);
        println!("  Backoff Multiplier: {:.1}x", config.backoff_multiplier);
        println!("  Jitter Enabled: {}", config.jitter);
        println!(
            "  Retry on Connection Error: {}",
            config.retry_on_connection_error
        );
        println!("  Retry on Timeout: {}", config.retry_on_timeout);
        println!(
            "  Retryable Status Codes: {:?}",
            config.retryable_status_codes
        );
    }

    fn display_rate_limit_section(config: &GlobalRateLimitConfig) {
        println!("  Enabled: {}", config.enabled);
        println!("  Requests per Second: {:.1}", config.requests_per_second);
        println!("  Burst Size: {}", config.burst_size);
        println!("  Strategy: {:?}", config.strategy);
        if !config.domain_limits.is_empty() {
            println!("  Domain Limits: {} configured", config.domain_limits.len());
        }
    }

    fn display_emulation_section(config: &EmulationConfig) {
        println!("  Strategy: {:?}", config.strategy);
        println!("  Rotate User Agents: {}", config.rotate_user_agents);
        println!("  Configured Browsers: {}", config.browsers.len());
        if !config.custom_user_agents.is_empty() {
            println!("  Custom User Agents: {}", config.custom_user_agents.len());
        }
    }

    fn display_concurrency_section(config: &ConcurrencyLimits) {
        println!(
            "  Max Concurrent Requests: {}",
            config.max_concurrent_requests
        );
        println!(
            "  Max Concurrent per Domain: {}",
            config.max_concurrent_per_domain
        );
        println!("  Max Connections: {}", config.max_connections);
        println!("  Max Idle Connections: {}", config.max_idle_connections);
        println!("  Idle Timeout: {}s", config.idle_timeout_seconds);
        println!("  HTTP/2 Multiplexing: {}", config.http2_multiplexing);
    }
}
