use anyhow::Result;
use data_pipeline::config::ApiConfig;
use data_pipeline::fetcher::UnifiedFetcher;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== PANDAMART CONFIGURATION TEST ===\n");

    // Load Pandamart configuration
    let config_path = "src/configs/pandamart.toml";

    println!("🔧 Loading Pandamart config from: {}", config_path);
    let config = ApiConfig::from_file(config_path)?;

    println!("✅ Config loaded successfully!");
    println!("   API Name: {}", config.api.name);
    println!("   Method: {}", config.request.method);
    println!("   Base URL: {}", config.api.base_url);
    println!(
        "   Endpoint: {}",
        config
            .request
            .endpoint
            .as_ref()
            .unwrap_or(&"None".to_string())
    );
    println!("   Full URL: {}", config.build_request_url());

    // Check GraphQL query
    if let Some(ref query) = config.request.graphql_query {
        println!("   GraphQL Query: {} characters", query.len());
        println!(
            "   Query starts with: {}",
            &query.chars().take(50).collect::<String>()
        );
    } else {
        println!("   ❌ No GraphQL query found!");
        return Ok(());
    }

    // Check variables
    if let Some(ref variables) = config.request.graphql_variables {
        println!("   GraphQL Variables:");
        for (key, value) in variables {
            println!("     {}: {:?}", key, value);
        }
    }

    println!("\n=== TESTING API REQUEST ===");

    // Initialize fetcher
    let fetcher = UnifiedFetcher::new(config.clone())?;

    // Test with a single category (fruits & vegetables)
    let test_category_id = "aa20e9c9-5c36-4a39-b9f2-513a291c677d";
    println!(
        "🚀 Testing GraphQL request for category: {}",
        test_category_id
    );

    match fetcher.fetch_graphql_single(test_category_id).await {
        Ok(products) => {
            println!("✅ SUCCESS! Fetched {} products", products.len());

            if !products.is_empty() {
                println!("\n=== SAMPLE PRODUCT ===");
                if let Some(first_product) = products.first() {
                    println!("{}", serde_json::to_string_pretty(first_product)?);
                }
            }
        }
        Err(e) => {
            println!("❌ FAILED: {}", e);

            // Let's also test the raw request building
            println!("\n=== DEBUG: Request Body ===");
            match fetcher.build_graphql_request_body(test_category_id) {
                Ok(body) => {
                    println!("Request body that would be sent:");
                    println!("{}", serde_json::to_string_pretty(&body)?);
                }
                Err(build_err) => {
                    println!("Failed to build request body: {}", build_err);
                }
            }
        }
    }

    println!("\n=== TEST COMPLETE ===");

    Ok(())
}
