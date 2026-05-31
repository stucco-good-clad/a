use std::thread;
use ureq::Agent;

fn main() {
    let agent = Agent::new();
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;

    let endpoints = [
        "https://solana.publicnode.com",
        "https://solana-rpc.publicnode.com",
    ];

    let handles: Vec<_> = endpoints
        .iter()
        .map(|url| {
            let agent = agent.clone();
            let body = body.to_vec();
            let url = *url;
            thread::spawn(move || -> String {
                let resp = agent
                    .post(url)
                    .set("Content-Type", "application/json")
                    .send_bytes(&body)
                    .unwrap();
                let val: serde_json::Value = resp.into_json().unwrap();
                val["result"].to_string()
            })
        })
        .collect();

    for h in handles {
        println!("{}", h.join().unwrap());
    }
}
