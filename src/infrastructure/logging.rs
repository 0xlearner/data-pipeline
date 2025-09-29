use anyhow::Result;
use std::io;
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;

/// Logging configuration
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub level: Level,
    pub format: LogFormat,
    pub output: LogOutput,
    pub include_timestamps: bool,
    pub include_thread_ids: bool,
    pub include_file_locations: bool,
    pub span_events: SpanEvents,
    pub filter: Option<String>,
}

/// Log format options
#[derive(Debug, Clone)]
pub enum LogFormat {
    Pretty,
    Json,
    Compact,
}

/// Log output options
#[derive(Debug, Clone)]
pub enum LogOutput {
    Stdout,
    Stderr,
    File(String),
}

/// Span event configuration
#[derive(Debug, Clone)]
pub enum SpanEvents {
    None,
    New,
    Enter,
    Exit,
    Close,
    Active,
    Full,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: Level::INFO,
            format: LogFormat::Pretty,
            output: LogOutput::Stdout,
            include_timestamps: true,
            include_thread_ids: false,
            include_file_locations: false,
            span_events: SpanEvents::None,
            filter: None,
        }
    }
}

/// Logging manager for centralized logging configuration
pub struct LoggingManager {
    config: LoggingConfig,
}

impl LoggingManager {
    /// Create a new logging manager
    pub fn new(config: LoggingConfig) -> Self {
        Self { config }
    }

    /// Create a default logging manager
    pub fn default() -> Self {
        Self::new(LoggingConfig::default())
    }

    /// Initialize the global logger
    pub fn init(&self) -> Result<()> {
        // Simple initialization using basic tracing-subscriber features
        let fmt_span = match self.config.span_events {
            SpanEvents::None => FmtSpan::NONE,
            SpanEvents::New => FmtSpan::NEW,
            SpanEvents::Enter => FmtSpan::ENTER,
            SpanEvents::Exit => FmtSpan::EXIT,
            SpanEvents::Close => FmtSpan::CLOSE,
            SpanEvents::Active => FmtSpan::ACTIVE,
            SpanEvents::Full => FmtSpan::FULL,
        };

        // Use a simple format-based approach
        match &self.config.output {
            LogOutput::Stdout => {
                tracing_subscriber::fmt()
                    .with_max_level(self.config.level)
                    .with_span_events(fmt_span)
                    .with_thread_ids(self.config.include_thread_ids)
                    .with_file(self.config.include_file_locations)
                    .with_line_number(self.config.include_file_locations)
                    .with_ansi(true)
                    .init();
            }
            LogOutput::Stderr => {
                tracing_subscriber::fmt()
                    .with_max_level(self.config.level)
                    .with_span_events(fmt_span)
                    .with_thread_ids(self.config.include_thread_ids)
                    .with_file(self.config.include_file_locations)
                    .with_line_number(self.config.include_file_locations)
                    .with_ansi(true)
                    .with_writer(io::stderr)
                    .init();
            }
            LogOutput::File(path) => {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                tracing_subscriber::fmt()
                    .with_max_level(self.config.level)
                    .with_span_events(fmt_span)
                    .with_thread_ids(self.config.include_thread_ids)
                    .with_file(self.config.include_file_locations)
                    .with_line_number(self.config.include_file_locations)
                    .with_ansi(false)
                    .with_writer(file)
                    .init();
            }
        }

        Ok(())
    }

    /// Get the current configuration
    pub fn config(&self) -> &LoggingConfig {
        &self.config
    }
}

/// Builder for logging configuration
pub struct LoggingConfigBuilder {
    config: LoggingConfig,
}

impl LoggingConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: LoggingConfig::default(),
        }
    }

    pub fn level(mut self, level: Level) -> Self {
        self.config.level = level;
        self
    }

    pub fn format(mut self, format: LogFormat) -> Self {
        self.config.format = format;
        self
    }

    pub fn output(mut self, output: LogOutput) -> Self {
        self.config.output = output;
        self
    }

    pub fn include_timestamps(mut self, include: bool) -> Self {
        self.config.include_timestamps = include;
        self
    }

    pub fn include_thread_ids(mut self, include: bool) -> Self {
        self.config.include_thread_ids = include;
        self
    }

    pub fn include_file_locations(mut self, include: bool) -> Self {
        self.config.include_file_locations = include;
        self
    }

    pub fn span_events(mut self, events: SpanEvents) -> Self {
        self.config.span_events = events;
        self
    }

    pub fn filter(mut self, filter: String) -> Self {
        self.config.filter = Some(filter);
        self
    }

    pub fn build(self) -> LoggingConfig {
        self.config
    }
}

impl Default for LoggingConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience functions for common logging setups
impl LoggingManager {
    /// Create a development-friendly logger
    pub fn for_development() -> Result<Self> {
        let config = LoggingConfigBuilder::new()
            .level(Level::DEBUG)
            .format(LogFormat::Pretty)
            .output(LogOutput::Stdout)
            .include_file_locations(true)
            .span_events(SpanEvents::Active)
            .build();

        let manager = Self::new(config);
        manager.init()?;
        Ok(manager)
    }

    /// Create a production-friendly logger
    pub fn for_production() -> Result<Self> {
        let config = LoggingConfigBuilder::new()
            .level(Level::INFO)
            .format(LogFormat::Json)
            .output(LogOutput::Stdout)
            .include_timestamps(true)
            .span_events(SpanEvents::None)
            .build();

        let manager = Self::new(config);
        manager.init()?;
        Ok(manager)
    }

    /// Create a file-based logger
    pub fn for_file(path: &str, level: Level) -> Result<Self> {
        let config = LoggingConfigBuilder::new()
            .level(level)
            .format(LogFormat::Json)
            .output(LogOutput::File(path.to_string()))
            .include_timestamps(true)
            .include_file_locations(true)
            .build();

        let manager = Self::new(config);
        manager.init()?;
        Ok(manager)
    }
}
