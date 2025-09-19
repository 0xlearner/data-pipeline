use serde_json::Value;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read the JSON file
    let json_content = fs::read_to_string("krave_mart_api_response.json")?;
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
    
    // Let's also check for pricing data patterns
    println!("\n=== PRICING DATA ANALYSIS ===");
    let mut products_with_primary_prices = 0;
    let mut products_with_fallback_prices = 0;
    let mut products_with_no_prices = 0;
    
    if let Some(data_array) = data.get("data").and_then(|d| d.as_array()) {
        for data_item in data_array.iter() {
            if let Some(krave_mart_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                for product in krave_mart_products.iter() {
                    let has_cost_price = product.get("cost_price").is_some() && !product.get("cost_price").unwrap().is_null();
                    let has_mrp = product.get("mrp").is_some() && !product.get("mrp").unwrap().is_null();
                    let has_special_price = product.get("special_price").is_some() && !product.get("special_price").unwrap().is_null();
                    let has_product_price = product.get("product_price").is_some() && !product.get("product_price").unwrap().is_null();
                    
                    if has_cost_price || has_mrp {
                        products_with_primary_prices += 1;
                    } else if has_special_price || has_product_price {
                        products_with_fallback_prices += 1;
                    } else {
                        products_with_no_prices += 1;
                    }
                }
            }
        }
    }
    
    println!("Products with primary price fields (cost_price/mrp): {}", products_with_primary_prices);
    println!("Products with only fallback price fields (special_price/product_price): {}", products_with_fallback_prices);
    println!("Products with no price data: {}", products_with_no_prices);

    // Let's simulate the pipeline extraction logic to find missing products
    println!("\n=== PIPELINE SIMULATION ===");
    let mut successful_extractions = 0;
    let mut failed_extractions = 0;
    let mut failed_products = Vec::new();

    if let Some(data_array) = data.get("data").and_then(|d| d.as_array()) {
        for (data_index, data_item) in data_array.iter().enumerate() {
            if let Some(krave_mart_products) = data_item.get("l2_products").and_then(|l| l.as_array()) {
                for (product_index, product) in krave_mart_products.iter().enumerate() {
                    // Simulate the pipeline's extract_fields_directly logic
                    let product_id = product.get("product_id").and_then(|v| v.as_u64());
                    let name = product.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();

                    if product_id.is_none() {
                        failed_extractions += 1;
                        failed_products.push(format!("Data[{}].KraveMart_products[{}]: Missing product_id", data_index, product_index));
                        continue;
                    }

                    if name.is_empty() {
                        failed_extractions += 1;
                        failed_products.push(format!("Data[{}].KraveMart_products[{}]: Missing/empty name (product_id: {})",
                            data_index, product_index, product_id.unwrap()));
                        continue;
                    }

                    successful_extractions += 1;
                }
            }
        }
    }

    println!("Successful extractions: {}", successful_extractions);
    println!("Failed extractions: {}", failed_extractions);

    if !failed_products.is_empty() {
        println!("\nFailed products:");
        for failure in failed_products {
            println!("  - {}", failure);
        }
    }

    if successful_extractions != total_products {
        println!("⚠️  Pipeline simulation shows {} successful vs {} total products", successful_extractions, total_products);
    } else {
        println!("✅ Pipeline simulation matches total products");
    }

    Ok(())
}
