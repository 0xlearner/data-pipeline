use serde_json::Value;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read the JSON file
    let json_content = fs::read_to_string("/home/zerox/rust-projects/data-pipeline/krave_mart_api_response.json")?;
    let data: Value = serde_json::from_str(&json_content)?;
    
    println!("Analyzing Krave Mart API Response...\n");
    
    // Get the count field from the root
    if let Some(count) = data.get("count") {
        println!("API Response Count field: {}", count);
    }
    
    // Navigate to the data array
    let mut total_krave_mart_product_arrays = 0;
    let mut total_products = 0;

    if let Some(data_array) = data.get("data").and_then(|d| d.as_array()) {
        println!("Number of data items: {}", data_array.len());

        for (index, data_item) in data_array.iter().enumerate() {
            if let Some(krave_mart_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                total_krave_mart_product_arrays += 1;
                let products_in_this_array = krave_mart_products.len();
                total_products += products_in_this_array;

                println!("Data item {}: {} products in KraveMart products array", index, products_in_this_array);

                // Show some details about the first few products
                if index == 0 && products_in_this_array > 0 {
                    println!("  Sample products from first KraveMart products array:");
                    for (i, product) in krave_mart_products.iter().take(3).enumerate() {
                        if let Some(name) = product.get("name").and_then(|n| n.as_str()) {
                            if let Some(product_id) = product.get("product_id") {
                                println!("    Product {}: ID={}, Name=\"{}\"", i + 1, product_id, name);
                            }
                        }
                    }
                    if products_in_this_array > 3 {
                        println!("    ... and {} more products", products_in_this_array - 3);
                    }
                }
            } else {
                println!("Data item {}: No KraveMart products array found", index);
            }
        }
    }

    println!("\n=== SUMMARY ===");
    println!("Total number of KraveMart product arrays: {}", total_krave_mart_product_arrays);
    println!("Total number of individual products: {}", total_products);
    
    // Verify against the count field
    if let Some(count) = data.get("count").and_then(|c| c.as_u64()) {
        if count as usize == total_products {
            println!("✅ Count field matches total products: {}", count);
        } else {
            println!("⚠️  Count field ({}) does not match total products ({})", count, total_products);
        }
    }
    
    Ok(())
}
