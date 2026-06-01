use std::env;

use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url = env::var("RPC_URL")
        .unwrap_or_else(|_| "http://slc.rpc.orbitflare.com".to_string());
    let api_key = env::var("API_KEY").expect("API_KEY env var required");

    let full_url = format!("{}?api_key={}", rpc_url.trim_end_matches('/'), api_key);

    let client = reqwest::Client::new();

    // Step 1: get the latest slot so we can query a recent finalized block
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });

    let resp: Value = client
        .post(&full_url)
        .json(&body)
        .send()
        .await?
        .json()
        .await?;

    let latest_slot = resp["result"]
        .as_u64()
        .expect("failed to parse getSlot result");

    // Query a finalized block (2 slots behind latest)
    let target_slot = latest_slot.saturating_sub(2);

    let body = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "getBlock",
        "params": [
            target_slot,
            {
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": false
            }
        ]
    });

    let resp: Value = client
        .post(&full_url)
        .json(&body)
        .send()
        .await?
        .json()
        .await?;

    println!("Slot: {}", target_slot);
    println!("Response: {}", serde_json::to_string_pretty(&resp)?);

    Ok(())
}
