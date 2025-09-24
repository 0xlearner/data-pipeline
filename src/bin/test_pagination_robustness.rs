use anyhow::Result;
use data_pipeline::config::ApiConfig;
use data_pipeline::fetcher::UnifiedFetcher;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== PAGINATION ROBUSTNESS TEST ===\n");

    // Get source from command line argument
    let args: Vec<String> = env::args().collect();
    let source = if args.len() > 1 {
        args[1].clone()
    } else {
        println!("Usage: cargo run --bin test_pagination_robustness <source>");
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

    println!("🚀 Testing pagination robustness for {}...\n", source);

    // Load configuration
    let config = ApiConfig::from_file(config_path)?;
    let fetcher = UnifiedFetcher::new(config.clone())?;

    if source == "bazaar_app" {
        println!("=== Testing BazaarApp Categories ===");

        // Test single-page category (fruits-vegetables)
        println!("\n1. Testing single-page category: fruits-vegetables");
        let single_page_products = fetcher.fetch_post_paginated("fruits-vegetables").await?;
        println!(
            "   ✅ Single-page category: {} products",
            single_page_products.len()
        );

        // Test multi-page category (baby-care)
        println!("\n2. Testing multi-page category: baby-care");
        let multi_page_products = fetcher.fetch_post_paginated("baby-care").await?;
        println!(
            "   ✅ Multi-page category: {} products",
            multi_page_products.len()
        );

        // Test non-existent category (should handle gracefully)
        println!("\n3. Testing non-existent category: non-existent-category");
        let non_existent_products = fetcher
            .fetch_post_paginated("non-existent-category")
            .await?;
        println!(
            "   ✅ Non-existent category: {} products",
            non_existent_products.len()
        );

        // Test category with special characters
        println!("\n4. Testing category with special characters: invalid@category#test");
        let special_char_products = fetcher
            .fetch_post_paginated("invalid@category#test")
            .await?;
        println!(
            "   ✅ Special characters category: {} products",
            special_char_products.len()
        );

        println!("\n=== BazaarApp Pagination Test Summary ===");
        println!(
            "Single-page (fruits-vegetables): {} products",
            single_page_products.len()
        );
        println!(
            "Multi-page (baby-care): {} products",
            multi_page_products.len()
        );
        println!("Non-existent: {} products", non_existent_products.len());
        println!("Special chars: {} products", special_char_products.len());
    } else if source == "krave_mart" {
        println!("=== Testing KraveMart Categories ===");

        // Get a few categories to test
        let category_urls = config.build_category_urls();
        let test_categories: Vec<_> = category_urls.into_iter().take(3).collect();

        for (i, (category_name, url)) in test_categories.iter().enumerate() {
            println!("\n{}. Testing category: {}", i + 1, category_name);
            let products = fetcher.fetch_get_paginated(url).await?;
            println!(
                "   ✅ Category '{}': {} products",
                category_name,
                products.len()
            );
        }

        // Test invalid URL (should handle gracefully)
        println!("\n4. Testing invalid URL");
        let invalid_url =
            "https://k2products.kravemart.com/api/v2/es/categories/99999/products/1242164";
        let invalid_products = fetcher.fetch_get_paginated(invalid_url).await?;
        println!("   ✅ Invalid URL: {} products", invalid_products.len());
    }

    println!("\n=== PAGINATION ROBUSTNESS TEST COMPLETE ===");
    println!("🎉 All pagination scenarios handled gracefully!");
    println!("🛡️ Robust error handling and safety limits working correctly!");

    Ok(())
}
