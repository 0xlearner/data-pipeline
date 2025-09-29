use anyhow::Result;
use dotenv;
use tracing::info;

use data_pipeline::cli::App;
use data_pipeline::infrastructure::LoggingManager;
use data_pipeline::pipeline::orchestrator::{PipelineOrchestrator, PipelineOptions};

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenv::dotenv().ok();

    // Parse command line arguments
    let app = App::from_args()?;

    // Check if help was requested
    if App::should_show_help() {
        App::print_help();
        return Ok(());
    }

    // Validate configuration
    app.validate()?;

    // Initialize logging with the specified level
    let logging_manager = LoggingManager::default();
    logging_manager.init()?;

    info!("🚀 Starting Multi-Source Data Pipeline");
    info!("🎯 Mode: {}", app.get_mode_description());
    info!("🎯 Target: {}", app.get_target_description());

    // Initialize the pipeline orchestrator
    let orchestrator = PipelineOrchestrator::new().await?;

    // Convert CLI app to pipeline options
    let options = PipelineOptions::from(&app);

    // Run the pipeline
    let result = orchestrator.run(&options).await?;

    // Display results summary
    if result.is_success() {
        info!("🎉 Pipeline completed successfully!");
        info!("📊 Success rate: {:.1}%", result.success_rate());
    } else {
        info!("⚠️ Pipeline completed with some failures");
        info!("📊 Success rate: {:.1}%", result.success_rate());
        info!("❌ Failed sources: {}", result.failed_source_names().join(", "));
    }

    Ok(())
}


