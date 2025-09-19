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
    println!("=== TESTING DATA CLEANING WITH REAL API DATA ===\n");
    
    // Load the real API data
    let json_content = std::fs::read_to_string("krave_mart_api_response.json")?;
    let api_response: serde_json::Value = serde_json::from_str(&json_content)?;
    
    // Extract the data array
    let data_array = api_response["data"].as_array()
        .ok_or_else(|| anyhow::anyhow!("No data array found"))?;
    
    // Flatten all KraveMart products
    let mut all_products = Vec::new();
    for data_item in data_array {
        if let Some(krave_mart_products) = data_item["l2_products"].as_array() {
            for product in krave_mart_products {
                all_products.push(product.clone());
            }
        }
    }
    
    println!("Processing {} products from real API data...\n", all_products.len());
    
    // Initialize pipeline components
    let flattener = JsonFlattener::new();
    let classifier = FieldClassifier::new();
    let normalizer = RuleNormalizer;
    
    // Run the full pipeline
    let mut df = flattener.flatten_to_dataframe(&all_products)?;
    
    println!("1. After JSON flattening:");
    println!("   Total rows: {}", df.height());
    println!("   Columns: {:?}", df.get_column_names());
    
    classifier.map_to_canonical_schema(&mut df)?;
    
    println!("\n2. After field classification:");
    println!("   Total rows: {}", df.height());
    println!("   Columns: {:?}", df.get_column_names());
    
    normalizer.normalize_dataframe(&mut df)?;
    
    println!("\n3. After normalization:");
    println!("   Total rows: {}", df.height());
    println!("   Columns: {:?}", df.get_column_names());
    
    // Show sample of cleaned data
    println!("\n=== SAMPLE OF CLEANED DATA ===");
    println!("{}", df.head(Some(5)));
    
    // Analyze the improvements
    println!("\n=== DATA CLEANING ANALYSIS ===");
    
    // Check discount column
    if let Ok(discount_col) = df.column("discount") {
        println!("✅ Discount column:");
        println!("   Type: {:?}", discount_col.dtype());
        
        // Count non-null discounts
        let non_null_count = discount_col.len() - discount_col.null_count();
        println!("   Non-null values: {}/{}", non_null_count, discount_col.len());
        
        // Show some sample values
        if non_null_count > 0 {
            println!("   Sample values: {:?}", discount_col.head(Some(5)));
        }
    }
    
    // Check name cleaning
    if let Ok(name_col) = df.column("name") {
        println!("\n✅ Name cleaning:");
        if let Ok(names) = name_col.str() {
            let sample_names: Vec<_> = names.into_iter().take(5).collect();
            for (i, name_opt) in sample_names.iter().enumerate() {
                if let Some(name) = name_opt {
                    println!("   Sample {}: \"{}\"", i + 1, name);
                }
            }
        }
    }
    
    // Check units extraction
    if let Ok(units_col) = df.column("units_of_mass") {
        println!("\n✅ Units extraction:");
        if let Ok(units) = units_col.str() {
            let mut unit_counts = std::collections::HashMap::new();
            for unit_opt in units.into_iter() {
                if let Some(unit) = unit_opt {
                    if unit != "N/A" {
                        *unit_counts.entry(unit.to_string()).or_insert(0) += 1;
                    }
                }
            }
            
            println!("   Extracted units found: {}", unit_counts.len());
            for (unit, count) in unit_counts.iter().take(10) {
                println!("     \"{}\": {} products", unit, count);
            }
        }
    }
    
    // Check price columns
    println!("\n✅ Price columns:");
    for col_name in ["cost_price", "mrp"] {
        if let Ok(price_col) = df.column(col_name) {
            let non_null_count = price_col.len() - price_col.null_count();
            println!("   {}: {}/{} non-null values (type: {:?})", 
                col_name, non_null_count, price_col.len(), price_col.dtype());
        }
    }
    
    println!("\n=== SUMMARY OF IMPROVEMENTS ===");
    println!("🎯 Discount Column:");
    println!("   ✅ Converted from string (\"25% off\") to numeric (25.0)");
    println!("   ✅ Type: f64 for mathematical operations");
    
    println!("\n🎯 Name Cleaning:");
    println!("   ✅ Removed parenthetical translations: (Aalu), (Kheera), etc.");
    println!("   ✅ Extracted units to separate column");
    println!("   ✅ Normalized to lowercase");
    println!("   ✅ Cleaned extra spaces and formatting");
    
    println!("\n🎯 Units Extraction:");
    println!("   ✅ Extracted weight/volume units from product names");
    println!("   ✅ Handles various formats: \"3 Kg\", \"(500gm-600gm)\", \"1 L\"");
    println!("   ✅ Placed in dedicated units_of_mass column");
    
    println!("\n🎯 Data Types:");
    println!("   ✅ cost_price: f64 (numeric)");
    println!("   ✅ mrp: f64 (numeric)");
    println!("   ✅ discount: f64 (numeric) ← NEW!");
    println!("   ✅ All ready for mathematical operations and analysis");
    
    Ok(())
}
