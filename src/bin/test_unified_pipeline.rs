use anyhow::Result;
use data_pipeline::config::ApiConfig;
use data_pipeline::fetcher::UnifiedFetcher;
use data_pipeline::processor::{FieldClassifier, JsonFlattener, RuleNormalizer};
use polars::prelude::*;
use std::env;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    println!("=== UNIFIED MULTI-SOURCE PIPELINE TEST ===\n");
    
    // Get source from command line argument
    let args: Vec<String> = env::args().collect();
    let source = if args.len() > 1 {
        args[1].clone()
    } else {
        println!("Usage: cargo run --bin test_unified_pipeline <source>");
        println!("Available sources: krave_mart, bazaar_app");
        return Ok(());
    };
    
    let config_path = match source.as_str() {
        "krave_mart" => "src/configs/krave_mart.toml",
        "bazaar_app" => "src/configs/bazaar_app.toml",
        _ => {
            println!("❌ Unknown source: {}", source);
            println!("Available sources: krave_mart, bazaar_app");
            return Ok(());
        }
    };
    
    println!("🚀 Testing {} pipeline...\n", source);
    
    // Load configuration
    let config = ApiConfig::from_file(config_path)?;
    println!("✅ Loaded config for: {}", config.api.name);
    println!("   Method: {}", config.request.method);
    println!("   Base URL: {}", config.api.base_url);
    
    // Initialize components
    let fetcher = UnifiedFetcher::new(config.clone())?;
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;
    
    println!("\n=== Step 1: Fetching Data ===");
    
    // Fetch a limited amount of data for testing
    let raw_data = if source == "krave_mart" {
        // For KraveMart, fetch from one category
        let category_urls = config.build_category_urls();
        if let Some((category_name, url)) = category_urls.first() {
            info!("Testing with category: {}", category_name);
            fetcher.fetch_get_paginated(url).await?
        } else {
            Vec::new()
        }
    } else {
        // For BazaarApp, fetch from fruits-vegetables category (known to work)
        let test_slug = "fruits-vegetables";
        info!("Testing with category: {}", test_slug);
        fetcher.fetch_post_paginated(test_slug).await?
    };
    
    println!("✅ Fetched {} products", raw_data.len());
    
    if raw_data.is_empty() {
        println!("⚠️ No data fetched. Check API configuration and connectivity.");
        return Ok(());
    }
    
    println!("\n=== Step 2: Processing with Unified Pipeline ===");
    
    // Step 2.1: JSON Flattening
    println!("2.1 Flattening JSON to DataFrame...");
    let mut df = flattener.flatten_to_dataframe(&raw_data)?;
    println!("   ✅ Flattened to {} rows, {} columns", df.height(), df.width());
    println!("   Columns: {:?}", df.get_column_names());
    
    // Step 2.2: Field Classification
    println!("\n2.2 Applying field classification...");
    classifier.map_to_canonical_schema(&mut df)?;
    println!("   ✅ Classified fields to canonical schema");
    println!("   Columns: {:?}", df.get_column_names());
    
    // Step 2.3: Rule-based Normalization
    println!("\n2.3 Applying rule-based normalization...");
    normalizer.normalize_dataframe(&mut df)?;
    println!("   ✅ Normalized data");
    
    println!("\n=== Step 3: Data Quality Analysis ===");
    
    // Check required fields
    let required_fields = ["name", "sku", "cost_price", "mrp"];
    let mut missing_fields = Vec::new();
    let mut present_fields = Vec::new();
    
    for field in required_fields {
        if df.column(field).is_ok() {
            present_fields.push(field);
        } else {
            missing_fields.push(field);
        }
    }
    
    println!("✅ Present fields: {:?}", present_fields);
    if !missing_fields.is_empty() {
        println!("⚠️ Missing fields: {:?}", missing_fields);
    }
    
    // Data completeness analysis
    let total_rows = df.height();
    for field in &present_fields {
        if let Ok(col) = df.column(field) {
            let null_count = col.null_count();
            let completeness = ((total_rows - null_count) as f64 / total_rows as f64) * 100.0;
            println!("📊 {}: {:.1}% complete ({}/{} non-null)", 
                field, completeness, total_rows - null_count, total_rows);
        }
    }
    
    println!("\n=== Step 4: Sample Data Display ===");
    
    // Show sample of processed data
    let sample_df = df
        .clone()
        .lazy()
        .select([
            col("name"),
            col("sku"),
            col("cost_price"),
            col("mrp"),
        ])
        .limit(5)
        .collect()?;
    
    println!("🛍️ Sample processed products:");
    println!("{}", sample_df);
    
    println!("\n=== Step 5: Price Analysis ===");
    
    // Price statistics
    if df.column("cost_price").is_ok() && df.column("mrp").is_ok() {
        let price_stats = df
            .clone()
            .lazy()
            .select([
                col("cost_price").min().alias("min_cost"),
                col("cost_price").max().alias("max_cost"),
                col("cost_price").mean().alias("avg_cost"),
                col("mrp").min().alias("min_mrp"),
                col("mrp").max().alias("max_mrp"),
                col("mrp").mean().alias("avg_mrp"),
            ])
            .collect()?;
        
        println!("💰 Price Statistics:");
        if let (Ok(min_cost), Ok(max_cost), Ok(avg_cost)) = (
            price_stats.column("min_cost")?.get(0),
            price_stats.column("max_cost")?.get(0),
            price_stats.column("avg_cost")?.get(0),
        ) {
            println!("   Cost Price: Min={:.2}, Max={:.2}, Avg={:.2}", min_cost, max_cost, avg_cost);
        }
        
        if let (Ok(min_mrp), Ok(max_mrp), Ok(avg_mrp)) = (
            price_stats.column("min_mrp")?.get(0),
            price_stats.column("max_mrp")?.get(0),
            price_stats.column("avg_mrp")?.get(0),
        ) {
            println!("   MRP: Min={:.2}, Max={:.2}, Avg={:.2}", min_mrp, max_mrp, avg_mrp);
        }
    }
    
    println!("\n=== Step 6: Export Results ===");
    
    // Export to CSV
    let output_file = format!("{}_processed_sample.csv", source);
    let mut export_df = df
        .clone()
        .lazy()
        .select([
            col("name"),
            col("sku"),
            col("cost_price"),
            col("mrp"),
        ])
        .collect()?;
    
    let mut file = std::fs::File::create(&output_file)?;
    CsvWriter::new(&mut file).finish(&mut export_df)?;
    println!("✅ Exported sample data to: {}", output_file);
    
    println!("\n=== UNIFIED PIPELINE TEST COMPLETE ===");
    println!("🎉 Successfully processed {} products from {}!", df.height(), source);
    println!("🚀 The unified architecture works for both GET and POST APIs!");
    
    Ok(())
}
