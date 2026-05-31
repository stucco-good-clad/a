fn main() {
    let endpoints = [
        "https://solana.publicnode.com",
        "https://solana-rpc.publicnode.com",
    ];

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });

    for url in endpoints {
        let resp = ureq::post(url)
            .set("Content-Type", "application/json")
            .send_json(&body)
            .unwrap();
        let val: serde_json::Value = resp.into_json().unwrap();
        println!("{}", val["result"]);
    }
}
