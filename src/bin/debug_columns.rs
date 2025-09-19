use serde_json::json;
use anyhow::Result;

#[path = "../processor/json_flattener.rs"]
mod json_flattener;

#[path = "../processor/field_classifier.rs"]
mod field_classifier;

use json_flattener::JsonFlattener;
use field_classifier::FieldClassifier;

fn main() -> Result<()> {
    println!("=== DEBUGGING COLUMN MAPPING ISSUE ===\n");
    
    // Create sample data that shows the issue
    let sample_data = vec![
        json!({
            "product_id": 12345,
            "name": "Test Product",
            "sku": "TEST123",
            "sku_percent_off": "25% off",
            "cost_price": 100.0,
            "mrp": 150.0,
            "categories": [{"category_name": "Test Category"}]
        })
    ];
    
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    
    println!("1. Original sample data:");
    println!("   sku: TEST123");
    println!("   sku_percent_off: 25% off");
    
    // Step 1: Flatten to DataFrame
    println!("\n2. After JSON flattening:");
    let mut df = flattener.flatten_to_dataframe(&sample_data)?;
    
    println!("   Columns: {:?}", df.get_column_names());
    
    if let Ok(sku_col) = df.column("sku") {
        println!("   sku column: {:?}", sku_col);
    }
    
    if let Ok(sku_percent_off_col) = df.column("sku_percent_off") {
        println!("   sku_percent_off column: {:?}", sku_percent_off_col);
    }
    
    // Step 2: Apply field classification
    println!("\n3. After field classification:");
    classifier.map_to_canonical_schema(&mut df)?;
    
    println!("   Columns: {:?}", df.get_column_names());
    
    // Check what happened to each column
    if let Ok(sku_col) = df.column("sku") {
        println!("   sku column: {:?}", sku_col);
    }
    
    if let Ok(discount_col) = df.column("discount") {
        println!("   discount column: {:?}", discount_col);
    }
    
    if let Ok(sku_percent_off_col) = df.column("sku_percent_off") {
        println!("   sku_percent_off column (if still exists): {:?}", sku_percent_off_col);
    }
    
    // Show the full DataFrame
    println!("\n4. Full DataFrame:");
    println!("{}", df);
    
    // Let's also test the field classifier mappings directly
    println!("\n5. Field classifier mappings:");
    let test_mappings = vec![
        ("sku", vec![]),
        ("sku_percent_off", vec!["25% off".to_string()]),
        ("discount", vec![]),
        ("percent_off", vec![]),
    ];
    
    for (field_name, sample_values) in test_mappings {
        match classifier.classify_field(field_name, &sample_values) {
            Ok(canonical) => println!("   {} -> {}", field_name, canonical),
            Err(e) => println!("   {} -> ERROR: {}", field_name, e),
        }
    }
    
    Ok(())
}
