use std::env;
use std::time::{Duration, Instant};

use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url = env::var("RPC_URL")
        .unwrap_or_else(|_| "http://slc.rpc.orbitflare.com".to_string());
    let api_key = env::var("API_KEY").expect("API_KEY env var required");

    let full_url = format!("{}?api_key={}", rpc_url.trim_end_matches('/'), api_key);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });

    // Make 5 sequential requests and print headers for each
    for i in 1..=5 {
        let start = Instant::now();
        let resp = client.post(&full_url).json(&body).send().await?;
        let elapsed = start.elapsed();

        println!("=== Request {} ({:?}) ===", i, elapsed);
        println!("Status: {}", resp.status());

        // Print all response headers
        for (name, value) in resp.headers() {
            println!("  {}: {}", name, value.to_str().unwrap_or("<binary>"));
        }

        // Check body for errors
        let json: serde_json::Value = resp.json().await?;
        if let Some(err) = json.get("error") {
            println!("  RPC Error: {}", err);
        } else {
            println!("  Slot: {}", json["result"]);
        }

        // Small delay between requests
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Ok(())
}
