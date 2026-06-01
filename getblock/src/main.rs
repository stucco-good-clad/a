use std::env;
use std::fs;
use std::path::Path;

use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url = env::var("RPC_URL")
        .unwrap_or_else(|_| "http://slc.rpc.orbitflare.com".to_string());
    let api_key = env::var("API_KEY").expect("API_KEY env var required");

    let full_url = format!("{}?api_key={}", rpc_url.trim_end_matches('/'), api_key);

    let client = reqwest::Client::new();

    // Step 1: get the latest slot
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

    let slot = resp["result"]
        .as_u64()
        .expect("failed to parse getSlot result");

    // Write the slot number so the workflow can read it
    fs::write("slot.txt", slot.to_string())?;

    let block_file = format!("{}.txt", slot);

    // Check if we already have this block cached
    if Path::new(&block_file).exists() {
        return Ok(());
    }

    // Query a finalized block (2 slots behind latest)
    let target_slot = slot.saturating_sub(2);

    let body = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "getBlock",
        "params": [
            target_slot,
            {
                "encoding": "json",
                "transactionDetails": "signatures",
                "rewards": false,
                "maxSupportedTransactionVersion": 0
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

    let text = serde_json::to_string_pretty(&resp)?;
    fs::write(&block_file, &text)?;

    Ok(())
}
