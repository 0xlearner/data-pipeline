use anyhow::Result;
use std::sync::Arc;

use data_pipeline::adapters::{
    ApiSourceAdapter, HtmlSourceAdapter, MinioStorageAdapter, MinioStorageConfig,
    ProcessorAdapterFactory, UnifiedPipelineAdapter,
};
use data_pipeline::config::{ApiConfig, HtmlConfig};
use data_pipeline::storage::MinioStorage;
use data_pipeline::traits::{
    DataSource, ExecutionMode, OutputConfig, OutputDestination, Pipeline, PipelineContext, Storage,
    pipeline::HealthStatus, pipeline::OutputFormat,
};

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Trait-Based Architecture Demo");
    println!("==================================");

    // 1. Create storage backend using trait
    println!("\n📦 Setting up storage backend...");
    let storage_config = MinioStorageConfig {
        endpoint: "localhost:9000".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        bucket_name: "trait-demo".to_string(),
        region: None,
        use_ssl: false,
    };

    let storage_adapter = MinioStorageAdapter::from_config_async(storage_config).await?;
    println!("✅ Storage backend ready: {}", storage_adapter.name());

    // Check storage health
    let storage_health = storage_adapter.health_check().await?;
    println!(
        "   Health: {} ({}ms)",
        if storage_health.is_healthy {
            "✅ Healthy"
        } else {
            "❌ Unhealthy"
        },
        storage_health.response_time_ms
    );

    // 2. Create data sources using traits
    println!("\n🔌 Setting up data sources...");

    // API Source
    let api_config = ApiConfig::from_file("config/api/krave_mart.toml")?;
    let api_source = ApiSourceAdapter::new(api_config).await?;
    println!(
        "✅ API Source: {} ({:?})",
        api_source.name(),
        api_source.source_type()
    );

    // Check API source health
    let api_health = api_source.health_check().await?;
    println!(
        "   Health: {} ({}ms)",
        if api_health.is_healthy {
            "✅ Healthy"
        } else {
            "❌ Unhealthy"
        },
        api_health.response_time_ms.unwrap_or(0)
    );

    // HTML Source
    let html_config = HtmlConfig::from_file("config/sources/naheed.toml")?;
    let html_source = HtmlSourceAdapter::new(html_config).await?;
    println!(
        "✅ HTML Source: {} ({:?})",
        html_source.name(),
        html_source.source_type()
    );

    // Check HTML source health
    let html_health = html_source.health_check().await?;
    println!(
        "   Health: {} ({}ms)",
        if html_health.is_healthy {
            "✅ Healthy"
        } else {
            "❌ Unhealthy"
        },
        html_health.response_time_ms.unwrap_or(0)
    );

    // 3. Create processors using traits
    println!("\n⚙️ Setting up processors...");
    let processors = ProcessorAdapterFactory::create_standard_pipeline();
    for processor in &processors {
        println!(
            "✅ Processor: {} ({:?})",
            processor.name(),
            processor.processor_type()
        );
    }

    // 4. Create unified pipeline using traits
    println!("\n🔄 Setting up unified pipeline...");
    let minio_storage =
        MinioStorage::new("localhost:9000", "minioadmin", "minioadmin", "trait-demo")?;

    let mut pipeline =
        UnifiedPipelineAdapter::new("trait_based_pipeline".to_string(), Arc::new(minio_storage));

    // Add sources to pipeline
    pipeline.add_source("krave_mart".to_string(), Box::new(api_source));
    pipeline.add_source("naheed".to_string(), Box::new(html_source));

    println!("✅ Pipeline created: {}", pipeline.name());
    println!("   Sources: {:?}", pipeline.list_sources());

    // Validate pipeline
    pipeline.validate()?;
    println!("✅ Pipeline validation passed");

    // 5. Check pipeline health
    println!("\n🏥 Checking pipeline health...");
    let pipeline_health = pipeline.health_check().await?;
    println!(
        "Pipeline Health: {} ({:?})",
        if pipeline_health.is_healthy {
            "✅ Healthy"
        } else {
            "❌ Unhealthy"
        },
        pipeline_health.status
    );

    for check in &pipeline_health.checks {
        println!(
            "   {}: {} ({}ms)",
            check.name,
            if check.status == HealthStatus::Healthy {
                "✅"
            } else {
                "❌"
            },
            check.duration.as_millis()
        );
    }

    // 6. Execute pipeline using trait interface
    println!("\n🚀 Executing pipeline...");

    let context = PipelineContext {
        execution_id: uuid::Uuid::new_v4().to_string(),
        source_filters: vec!["krave_mart".to_string()], // Only process one source for demo
        processor_config: std::collections::HashMap::new(),
        output_config: OutputConfig {
            destinations: vec![OutputDestination::Storage {
                path: "trait-demo/".to_string(),
            }],
            format: OutputFormat::Json,
            compression: None,
            partitioning: None,
        },
        execution_mode: ExecutionMode::Batch,
        retry_config: data_pipeline::traits::RetryConfig::default(),
        timeout: Some(std::time::Duration::from_secs(300)),
        metadata: std::collections::HashMap::new(),
    };

    let result = pipeline.execute(context).await?;

    // 7. Display results
    println!("\n📊 Execution Results:");
    println!("   Status: {:?}", result.status);
    println!(
        "   Duration: {}ms",
        result.duration.unwrap_or_default().as_millis()
    );
    println!("   Records Processed: {}", result.total_records_processed);
    println!("   Records Output: {}", result.total_records_output);
    println!("   Success Rate: {:.2}%", result.success_rate() * 100.0);

    if !result.errors.is_empty() {
        println!("   Errors: {}", result.errors.len());
        for error in &result.errors {
            println!("     - {:?}: {}", error.error_type, error.message);
        }
    }

    println!("\n   Source Results:");
    for source_result in &result.sources_processed {
        println!(
            "     {}: {:?} ({} records in {}ms)",
            source_result.source_name,
            source_result.status,
            source_result.records_processed,
            source_result.duration.as_millis()
        );
    }

    println!("\n   Performance Metrics:");
    println!(
        "     Throughput: {:.2} records/sec",
        result.metrics.throughput_records_per_second
    );
    println!("     Error Rate: {:.2}%", result.metrics.error_rate * 100.0);

    // 8. Demonstrate trait polymorphism
    println!("\n🔄 Demonstrating trait polymorphism...");

    // Store sources in a vector of trait objects
    let api_adapter = ApiSourceAdapter::new(ApiConfig::from_file(
        "config/api/krave_mart.toml",
    )?).await?;
    let html_adapter = HtmlSourceAdapter::new(HtmlConfig::from_file(
        "config/sources/naheed.toml",
    )?).await?;

    let sources: Vec<Box<dyn DataSource>> = vec![
        Box::new(api_adapter),
        Box::new(html_adapter),
    ];

    for source in sources {
        println!("Source: {} ({:?})", source.name(), source.source_type());
        let metadata = source.metadata();
        println!(
            "  Description: {}",
            metadata.description.unwrap_or("N/A".to_string())
        );
        println!("  Operations: {:?}", metadata.supported_operations);

        let categories = source.get_categories().await?;
        println!("  Categories: {} available", categories.len());
    }

    println!("\n🎉 Trait-based architecture demo completed successfully!");
    println!("\n📋 Summary of Benefits:");
    println!("   ✅ Loose coupling between components");
    println!("   ✅ Easy to test with mock implementations");
    println!("   ✅ Extensible - new sources/processors can be added easily");
    println!("   ✅ Polymorphic - same interface for different implementations");
    println!("   ✅ Composable - mix and match components as needed");
    println!("   ✅ Type-safe - compile-time guarantees");

    Ok(())
}

// Helper function to demonstrate trait object usage
async fn process_any_source(source: &dyn DataSource) -> Result<()> {
    println!("Processing source: {}", source.name());

    // This works with any implementation of DataSource
    let health = source.health_check().await?;
    if health.is_healthy {
        let categories = source.get_categories().await?;
        println!("  Found {} categories", categories.len());

        // Could fetch data here
        // let data = source.fetch_all().await?;
    } else {
        println!(
            "  Source is unhealthy: {}",
            health.error_message.unwrap_or("Unknown error".to_string())
        );
    }

    Ok(())
}

// Helper function to demonstrate processor trait usage
async fn process_with_any_processor(
    processor: &dyn data_pipeline::traits::DataProcessor,
    _input: data_pipeline::traits::ProcessorInput,
) -> Result<()> {
    println!("Processing with: {}", processor.name());

    let metadata = processor.metadata();
    println!("  Supported inputs: {:?}", metadata.supported_input_types);
    println!("  Supported outputs: {:?}", metadata.supported_output_types);

    // This works with any implementation of DataProcessor
    if let Some(metrics) = metadata.performance_metrics {
        println!(
            "  Average processing time: {:.2}ms",
            metrics.average_processing_time_ms
        );
        println!(
            "  Throughput: {:.2} items/sec",
            metrics.throughput_items_per_second
        );
    }

    // Could process data here
    // let output = processor.process(input).await?;

    Ok(())
}
