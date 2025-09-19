use serde_json::json;
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
    println!("=== VERIFYING COLUMN MAPPING FIX ===\n");
    
    // Create test data that demonstrates the fix
    let test_data = vec![
        json!({
            "product_id": 12345,
            "name": "Test Product 1",
            "sku": "SKU123",
            "sku_percent_off": "25% off",
            "cost_price": 100.0,
            "mrp": 150.0,
            "categories": [{"category_name": "Electronics"}]
        }),
        json!({
            "product_id": 67890,
            "name": "Test Product 2",
            "sku": "ABC789",
            "sku_percent_off": "40% off",
            "special_price": "80.00",
            "product_price": "120.00",
            "categories": [{"category_name": "Home & Garden"}]
        })
    ];
    
    // Initialize pipeline components
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;
    
    // Run the full pipeline
    println!("Running full pipeline...");
    let mut df = flattener.flatten_to_dataframe(&test_data)?;
    
    println!("\n1. After JSON flattening:");
    println!("   Columns: {:?}", df.get_column_names());
    
    classifier.map_to_canonical_schema(&mut df)?;
    
    println!("\n2. After field classification:");
    println!("   Columns: {:?}", df.get_column_names());
    
    normalizer.normalize_dataframe(&mut df)?;
    
    println!("\n3. After normalization:");
    println!("   Columns: {:?}", df.get_column_names());
    
    // Verify the final result
    println!("\n=== FINAL VERIFICATION ===");
    println!("{}", df);
    
    // Check specific columns
    println!("\n=== COLUMN CONTENT VERIFICATION ===");
    
    if let Ok(sku_col) = df.column("sku") {
        println!("✅ SKU column exists and contains: {:?}", sku_col);
    } else {
        println!("❌ SKU column missing!");
    }
    
    if let Ok(discount_col) = df.column("discount") {
        println!("✅ Discount column exists and contains: {:?}", discount_col);
    } else {
        println!("❌ Discount column missing!");
    }
    
    // Verify no cross-contamination
    let mut success = true;
    
    if let (Ok(sku_col), Ok(discount_col)) = (df.column("sku"), df.column("discount")) {
        // Check that SKU values are in SKU column (not discount)
        let sku_values = sku_col.str().unwrap().into_no_null_iter().collect::<Vec<_>>();
        let discount_values = discount_col.str().unwrap().into_no_null_iter().collect::<Vec<_>>();
        
        println!("\nSKU values: {:?}", sku_values);
        println!("Discount values: {:?}", discount_values);
        
        // Verify SKU values don't contain discount patterns
        for sku_val in &sku_values {
            if sku_val.contains("%") || sku_val.contains("off") {
                println!("❌ SKU column contains discount value: {}", sku_val);
                success = false;
            }
        }
        
        // Verify discount values don't contain SKU patterns
        for discount_val in &discount_values {
            if !discount_val.contains("%") && !discount_val.contains("off") && !discount_val.is_empty() {
                println!("❌ Discount column contains non-discount value: {}", discount_val);
                success = false;
            }
        }
    }
    
    if success {
        println!("\n🎉 SUCCESS: Column mapping is working correctly!");
        println!("   ✅ SKU values are in the SKU column");
        println!("   ✅ Discount values are in the discount column");
        println!("   ✅ No cross-contamination between columns");
    } else {
        println!("\n❌ FAILURE: Column mapping issues detected!");
    }
    
    Ok(())
}
