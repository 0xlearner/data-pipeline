use serde_json::Value;
use std::fs;
use anyhow::Result;

#[path = "../processor/json_flattener.rs"]
mod json_flattener;

#[path = "../processor/field_classifier.rs"]
mod field_classifier;

#[path = "../processor/rule_normalizer.rs"]
mod rule_normalizer;

use json_flattener::JsonFlattener;
use field_classifier::FieldClassifier;
use rule_normalizer::RuleNormalizer;

fn main() -> Result<()> {
    println!("=== FULL PIPELINE TEST ===\n");
    
    // Read the JSON file
    let json_content = fs::read_to_string("krave_mart_api_response.json")?;
    let data: Value = serde_json::from_str(&json_content)?;
    
    // Simulate the HttpFetcher.extract_products logic exactly
    let mut all_products = Vec::new();

    if let Some(data_array) = data.get("data").and_then(|d| d.as_array()) {
        for data_item in data_array {
            if let Some(krave_mart_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                for product in krave_mart_products {
                    all_products.push(product.clone());
                }
            }
        }
    }
    
    println!("Extracted {} products from API response", all_products.len());
    
    // Initialize components exactly like main.rs
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;
    
    // Process data exactly like main.rs
    println!("\n1. Flattening to DataFrame...");
    let mut df = flattener.flatten_to_dataframe(&all_products)?;
    println!("   Rows after flattening: {}", df.height());
    
    // Apply ML classification
    println!("\n2. Applying field classification...");
    classifier.map_to_canonical_schema(&mut df)?;
    println!("   Rows after classification: {}", df.height());
    
    // Apply rule-based normalization
    println!("\n3. Applying rule normalization...");
    normalizer.normalize_dataframe(&mut df)?;
    println!("   Rows after normalization: {}", df.height());
    
    // Final summary
    println!("\n=== PIPELINE SUMMARY ===");
    println!("Input products: {}", all_products.len());
    println!("Final DataFrame rows: {}", df.height());
    println!("Final DataFrame columns: {:?}", df.get_column_names());
    
    if all_products.len() != df.height() {
        println!("⚠️  DISCREPANCY: {} products lost!", all_products.len() - df.height());
    } else {
        println!("✅ All products preserved through pipeline!");
    }
    
    // Show some sample final data
    if df.height() > 0 {
        println!("\nSample final data (first 3 rows):");
        let sample = df.head(Some(3));
        println!("{}", sample);
    }
    
    // Check for any null values in key columns
    println!("\n=== DATA QUALITY CHECK ===");
    
    if let Ok(cost_price_col) = df.column("cost_price") {
        let null_count = cost_price_col.null_count();
        println!("cost_price: {} null values out of {}", null_count, cost_price_col.len());
    }
    
    if let Ok(mrp_col) = df.column("mrp") {
        let null_count = mrp_col.null_count();
        println!("mrp: {} null values out of {}", null_count, mrp_col.len());
    }
    
    if let Ok(name_col) = df.column("name") {
        let null_count = name_col.null_count();
        println!("name: {} null values out of {}", null_count, name_col.len());
    }
    
    Ok(())
}
