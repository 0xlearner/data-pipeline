use data_pipeline::config::html_config::HtmlConfig;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Testing naheed.toml configuration loading...");

    let config_path = "config/sources/naheed.toml";

    // Check if file exists
    if !Path::new(config_path).exists() {
        println!("❌ Config file not found at: {}", config_path);
        return Ok(());
    }

    println!("✅ Config file found at: {}", config_path);

    // Try to read the raw content first
    match std::fs::read_to_string(config_path) {
        Ok(content) => {
            println!(
                "✅ Successfully read file content ({} bytes)",
                content.len()
            );

            // Try to parse as basic TOML first
            match toml::from_str::<toml::Value>(&content) {
                Ok(value) => {
                    println!("✅ Successfully parsed as basic TOML");
                    println!("📋 Top-level sections found:");
                    if let toml::Value::Table(table) = value {
                        for key in table.keys() {
                            println!("  - [{}]", key);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Failed to parse as basic TOML: {}", e);
                    return Ok(());
                }
            }

            // Now try to parse with our HtmlConfig structure
            println!("\n🔄 Attempting to parse with HtmlConfig structure...");
            match HtmlConfig::from_file(config_path) {
                Ok(config) => {
                    println!("✅ Successfully loaded HtmlConfig!");
                    println!("📋 Config details:");
                    println!("  - Site name: {}", config.site.name);
                    println!("  - Base URL: {}", config.site.base_url);
                    println!("  - Categories: {}", config.categories.len());

                    let enabled_categories = config.get_enabled_categories();
                    println!("  - Enabled categories: {}", enabled_categories.len());

                    if enabled_categories.len() > 0 {
                        println!("  - First few enabled categories:");
                        for (name, category) in enabled_categories.iter().take(3) {
                            println!("    * {} -> {}", name, category.name);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Failed to load HtmlConfig: {}", e);
                    println!("🔍 Error chain:");
                    let mut current_error: &dyn std::error::Error = &*e;
                    let mut level = 0;
                    loop {
                        println!("  {}: {}", level, current_error);
                        match current_error.source() {
                            Some(source) => {
                                current_error = source;
                                level += 1;
                            }
                            None => break,
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to read file: {}", e);
        }
    }

    Ok(())
}
