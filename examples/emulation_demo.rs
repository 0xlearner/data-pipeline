use data_pipeline::http::HttpClientBuilder;
use wreq_util::Emulation;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 HTTP Client Emulation Demo");
    println!("=============================\n");

    // Demo 1: Fixed emulation
    println!("1. Fixed Firefox emulation:");
    let client_fixed = HttpClientBuilder::new()
        .emulation(Emulation::Firefox136)
        .build()?;

    for i in 1..=3 {
        let _client = client_fixed.create_raw_client()?;
        println!("   Request {}: Using Firefox136", i);
    }
    println!();

    // Demo 2: Modern browsers rotation
    println!("2. Modern browsers rotation:");
    let client_modern = HttpClientBuilder::new().modern_browsers().build()?;

    for i in 1..=5 {
        let _client = client_modern.create_raw_client()?;
        println!("   Request {}: Using rotating modern browser", i);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // Small delay to see rotation
    }
    println!();

    // Demo 3: Firefox variants only
    println!("3. Firefox variants rotation:");
    let client_firefox = HttpClientBuilder::new().firefox_variants().build()?;

    for i in 1..=3 {
        let _client = client_firefox.create_raw_client()?;
        println!("   Request {}: Using Firefox variant", i);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    println!();

    // Demo 4: Chrome variants only
    println!("4. Chrome variants rotation:");
    let client_chrome = HttpClientBuilder::new().chrome_variants().build()?;

    for i in 1..=3 {
        let _client = client_chrome.create_raw_client()?;
        println!("   Request {}: Using Chrome variant", i);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    println!();

    // Demo 5: Random emulation
    println!("5. Random emulation:");
    let client_random = HttpClientBuilder::new().random_emulation().build()?;

    for i in 1..=3 {
        let _client = client_random.create_raw_client()?;
        println!("   Request {}: Using random browser", i);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    println!();

    println!("✅ Demo completed! Each request now uses different browser emulations");
    println!("   This helps avoid detection and blocking by websites and APIs.");

    Ok(())
}
