use serde_json::Value;
use std::fs;
use std::collections::HashSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== API DATA COMPARISON TOOL ===\n");
    
    // Read the saved JSON file
    let saved_json = fs::read_to_string("krave_mart_api_response.json")?;
    let saved_data: Value = serde_json::from_str(&saved_json)?;
    
    // Extract product IDs from saved data
    let mut saved_product_ids = HashSet::new();
    let mut saved_products = Vec::new();
    
    if let Some(data_array) = saved_data.get("data").and_then(|d| d.as_array()) {
        for data_item in data_array {
            if let Some(l2_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                for product in l2_products {
                    if let Some(product_id) = product.get("product_id").and_then(|id| id.as_u64()) {
                        saved_product_ids.insert(product_id);
                        saved_products.push((product_id, product.clone()));
                    }
                }
            }
        }
    }
    
    println!("Saved JSON file contains {} products", saved_product_ids.len());
    
    // Instructions for user to get live API data
    println!("\n=== INSTRUCTIONS ===");
    println!("To find the missing products, please:");
    println!("1. Run your pipeline and save the raw API response");
    println!("2. Save it as 'live_api_response.json'");
    println!("3. Run this tool again");
    println!();
    
    // Check if live data exists
    if let Ok(live_json) = fs::read_to_string("live_api_response.json") {
        println!("Found live_api_response.json, comparing...\n");
        
        let live_data: Value = serde_json::from_str(&live_json)?;
        let mut live_product_ids = HashSet::new();
        let mut live_products = Vec::new();
        
        if let Some(data_array) = live_data.get("data").and_then(|d| d.as_array()) {
            for data_item in data_array {
                if let Some(l2_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                    for product in l2_products {
                        if let Some(product_id) = product.get("product_id").and_then(|id| id.as_u64()) {
                            live_product_ids.insert(product_id);
                            live_products.push((product_id, product.clone()));
                        }
                    }
                }
            }
        }
        
        println!("Live API response contains {} products", live_product_ids.len());
        
        // Find differences
        let missing_in_live: Vec<_> = saved_product_ids.difference(&live_product_ids).collect();
        let new_in_live: Vec<_> = live_product_ids.difference(&saved_product_ids).collect();
        
        if missing_in_live.is_empty() && new_in_live.is_empty() {
            println!("✅ Both datasets contain exactly the same products!");
        } else {
            if !missing_in_live.is_empty() {
                println!("⚠️  Products in saved JSON but missing in live API ({}):", missing_in_live.len());
                for &product_id in &missing_in_live {
                    if let Some((_, product)) = saved_products.iter().find(|(id, _)| id == product_id) {
                        let name = product.get("name").and_then(|n| n.as_str()).unwrap_or("Unknown");
                        println!("  - ID {}: {}", product_id, name);
                    }
                }
                println!();
            }
            
            if !new_in_live.is_empty() {
                println!("ℹ️  New products in live API ({}):", new_in_live.len());
                for &product_id in &new_in_live {
                    if let Some((_, product)) = live_products.iter().find(|(id, _)| id == product_id) {
                        let name = product.get("name").and_then(|n| n.as_str()).unwrap_or("Unknown");
                        println!("  - ID {}: {}", product_id, name);
                    }
                }
            }
        }
    } else {
        println!("No live_api_response.json found.");
        println!("Please run your pipeline and save the raw API response as 'live_api_response.json'");
    }
    
    // Show some sample products from saved data
    println!("\n=== SAMPLE PRODUCTS FROM SAVED DATA ===");
    for (i, (product_id, product)) in saved_products.iter().take(5).enumerate() {
        let name = product.get("name").and_then(|n| n.as_str()).unwrap_or("Unknown");
        println!("{}. ID {}: {}", i + 1, product_id, name);
    }
    
    Ok(())
}
