use anyhow::Result;
use data_pipeline::infrastructure::LoggingManager;
use data_pipeline::pipeline::orchestrator::{PipelineOrchestrator, PipelineOptions};
use dotenv;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenv::dotenv().ok();

    // Initialize logging
    let logging_manager = LoggingManager::default();
    logging_manager.init()?;

    info!("🧪 Testing Two-Stage HTML Processing");

    // Initialize the pipeline orchestrator
    let orchestrator = PipelineOrchestrator::new().await?;

    // Stage 1: Fetch HTML pages only
    info!("\n=== STAGE 1: Fetching HTML Pages ===");
    let fetch_options = PipelineOptions {
        from_storage: false,
        specific_source: Some("naheed".to_string()),
        batch_size: None,
        memory_efficient: false,
    };

    let fetch_result = orchestrator.run(&fetch_options).await?;
    
    info!("📄 Stage 1 Results:");
    info!("  - Total pages fetched: {}", fetch_result.total_products);
    info!("  - Successful sources: {}", fetch_result.successful_sources);
    info!("  - Failed sources: {}", fetch_result.failed_sources);

    if fetch_result.successful_sources == 0 {
        info!("❌ No HTML pages were fetched. Skipping Stage 2.");
        return Ok(());
    }

    // Wait a moment before Stage 2
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Stage 2: Process HTML pages from storage
    info!("\n=== STAGE 2: Processing HTML Pages from Storage ===");
    let process_options = PipelineOptions {
        from_storage: true,
        specific_source: Some("naheed".to_string()),
        batch_size: None,
        memory_efficient: false,
    };

    let process_result = orchestrator.run(&process_options).await?;
    
    info!("🔄 Stage 2 Results:");
    info!("  - Total products processed: {}", process_result.total_products);
    info!("  - Successful sources: {}", process_result.successful_sources);
    info!("  - Failed sources: {}", process_result.failed_sources);

    // Summary
    info!("\n=== TWO-STAGE PROCESSING SUMMARY ===");
    info!("✅ Stage 1 (Fetch): {} HTML pages stored", fetch_result.total_products);
    info!("✅ Stage 2 (Process): {} products extracted", process_result.total_products);
    
    if process_result.total_products > 0 {
        info!("🎉 Two-stage HTML processing completed successfully!");
        info!("📊 Conversion rate: {:.1} products per page", 
              process_result.total_products as f64 / fetch_result.total_products as f64);
    } else {
        info!("⚠️ No products were extracted from the stored HTML pages");
    }

    Ok(())
}
