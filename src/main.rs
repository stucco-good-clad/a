use std::env;
use std::fs::File;
use std::io::Write;
use std::thread;
use ureq::Agent;

const CHUNK: u64 = 500_000;
const RANGE: u64 = 2_000_000;

const PREFIXES: &[&str] = &[
    "ams", "fra", "lon", "ny", "va", "slc", "la", "jp", "sg",
];

fn rpc(agent: &Agent, key: &str, prefix: &str, body: &[u8]) -> serde_json::Value {
    let url = format!("http://{}.rpc.orbitflare.com", prefix);
    let resp = agent
        .post(&url)
        .set("x-api-key", key)
        .set("Content-Type", "application/json")
        .send_bytes(body)
        .unwrap_or_else(|e| panic!("{}: {}", prefix, e));
    serde_json::from_str(&resp.into_string().unwrap()).unwrap()
}

fn main() {
    let key = env::var("key_1").expect("key_1 not set");
    let agent = Agent::new();
    let slot_body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;

    let current: u64 = rpc(&agent, &key, "ams", slot_body)["result"]
        .as_u64()
        .unwrap();
    println!("current slot: {current}");

    let start = current.saturating_sub(RANGE);

    // Chunk into 500K ranges (max allowed by getBlocks)
    let mut chunks: Vec<(u64, u64)> = vec![];
    let mut lo = start;
    while lo < current {
        let hi = (lo + CHUNK).min(current);
        chunks.push((lo, hi));
        lo = hi + 1;
    }

    // Fire all chunks in parallel across different endpoints
    let mut blocks: Vec<u64> = vec![];
    let handles: Vec<_> = chunks
        .into_iter()
        .enumerate()
        .map(|(i, (lo, hi))| {
            let a = agent.clone();
            let k = key.clone();
            let p = PREFIXES[i % PREFIXES.len()];
            let body = serde_json::to_vec(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBlocks",
                "params": [lo, hi]
            }))
            .unwrap();
            thread::spawn(move || {
                let v = rpc(&a, &k, p, &body);
                v["result"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x| x.as_u64().unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    for h in handles {
        blocks.extend(h.join().unwrap());
    }

    blocks.sort();
    println!("blocks fetched: {}", blocks.len());

    let mut f = File::create("blocks.txt").unwrap();
    for b in &blocks {
        writeln!(f, "{b}").unwrap();
    }
    println!("saved to blocks.txt");
}
