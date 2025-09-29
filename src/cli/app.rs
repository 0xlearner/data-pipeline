use anyhow::Result;
use std::env;
use tracing::Level;

/// Command line application configuration
#[derive(Debug, Clone)]
pub struct App {
    pub from_storage: bool,
    pub specific_source: Option<String>,
    pub batch_size: Option<usize>,
    pub log_level: Level,
}

impl App {
    /// Parse command line arguments and create App configuration
    pub fn from_args() -> Result<Self> {
        let args: Vec<String> = env::args().collect();

        // Check for storage flag
        let from_storage = args
            .iter()
            .any(|arg| arg == "--from-storage" || arg == "-s");

        // Check for specific source argument
        let specific_source = args
            .iter()
            .position(|arg| arg == "--source")
            .and_then(|pos| args.get(pos + 1))
            .map(|s| s.to_string());

        // Check for batch size argument
        let batch_size = args
            .iter()
            .position(|arg| arg == "--batch-size")
            .and_then(|pos| args.get(pos + 1))
            .and_then(|s| s.parse::<usize>().ok());

        // Check for log level argument
        let log_level = args
            .iter()
            .position(|arg| arg == "--log-level")
            .and_then(|pos| args.get(pos + 1))
            .and_then(|s| match s.to_lowercase().as_str() {
                "trace" => Some(Level::TRACE),
                "debug" => Some(Level::DEBUG),
                "info" => Some(Level::INFO),
                "warn" => Some(Level::WARN),
                "error" => Some(Level::ERROR),
                _ => None,
            })
            .unwrap_or(Level::INFO);

        Ok(App {
            from_storage,
            specific_source,
            batch_size,
            log_level,
        })
    }

    /// Display help information
    pub fn print_help() {
        println!("Data Pipeline - Multi-source data processing tool");
        println!();
        println!("USAGE:");
        println!("    data-pipeline [OPTIONS]");
        println!();
        println!("OPTIONS:");
        println!(
            "    -s, --from-storage          Process data from S3/MinIO storage instead of fetching from APIs"
        );
        println!("    --source <SOURCE>           Process only the specified source");
        println!("    --batch-size <SIZE>         Set batch size for processing large datasets");
        println!(
            "    --log-level <LEVEL>         Set logging level (trace, debug, info, warn, error)"
        );
        println!("    -h, --help                  Print this help message");
        println!();
        println!("AVAILABLE SOURCES:");
        println!("    kravemart                  Krave Mart API (JSON)");
        println!("    bazaarapp                  Bazaar App API (JSON)");
        println!("    dealcart                    DealCart API (JSON)");
        println!("    pandamart                   Pandamart GraphQL API (JSON)");
        println!("    naheed                      Naheed Store Website (HTML)");
        println!();
        println!("EXAMPLES:");
        println!("    data-pipeline                           # Process all sources from APIs");
        println!("    data-pipeline --from-storage            # Process all sources from storage");
        println!("    data-pipeline --source kravemart       # Process only Krave Mart");
        println!("    data-pipeline --batch-size 1000         # Use batch size of 1000");
        println!("    data-pipeline --log-level debug         # Enable debug logging");
    }

    /// Check if help was requested
    pub fn should_show_help() -> bool {
        let args: Vec<String> = env::args().collect();
        args.iter().any(|arg| arg == "--help" || arg == "-h")
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Validate source name if specified
        if let Some(ref source) = self.specific_source {
            let valid_sources = vec!["kravemart", "bazaarapp", "dealcart", "pandamart", "naheed"];
            if !valid_sources.contains(&source.as_str()) {
                return Err(anyhow::anyhow!(
                    "Invalid source '{}'. Valid sources are: {}",
                    source,
                    valid_sources.join(", ")
                ));
            }
        }

        // Validate batch size if specified
        if let Some(batch_size) = self.batch_size {
            if batch_size == 0 {
                return Err(anyhow::anyhow!("Batch size must be greater than 0"));
            }
            if batch_size > 10000 {
                return Err(anyhow::anyhow!(
                    "Batch size should not exceed 10000 for memory efficiency"
                ));
            }
        }

        Ok(())
    }

    /// Get the processing mode as a string for logging
    pub fn get_mode_description(&self) -> String {
        if self.from_storage {
            "Processing from S3/MinIO Storage".to_string()
        } else {
            "Fetching from APIs".to_string()
        }
    }

    /// Get the target description for logging
    pub fn get_target_description(&self) -> String {
        match &self.specific_source {
            Some(source) => format!("Processing specific source: {}", source),
            None => "Processing all sources".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_valid_source() {
        let app = App {
            from_storage: false,
            specific_source: Some("krave_mart".to_string()),
            batch_size: Some(1000),
            log_level: Level::INFO,
        };
        assert!(app.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_source() {
        let app = App {
            from_storage: false,
            specific_source: Some("invalid_source".to_string()),
            batch_size: None,
            log_level: Level::INFO,
        };
        assert!(app.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_batch_size() {
        let app = App {
            from_storage: false,
            specific_source: None,
            batch_size: Some(0),
            log_level: Level::INFO,
        };
        assert!(app.validate().is_err());
    }
}
