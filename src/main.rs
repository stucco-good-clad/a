use std::env;
use std::thread;
use std::time::{Duration, Instant};
use ureq::Agent;

const ITERATIONS: u32 = 5;

const ENDPOINTS: &[&str] = &[
    "ams",
    "fra",
    "lon",
    "ny",
    "va",
    "slc",
    "la",
    "jp",
    "sg",
];

const RPC_BODY: &[u8] =
    br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;

#[derive(Default, Clone)]
struct Stats {
    min: Duration,
    max: Duration,
    total: Duration,
    successes: u32,
    failures: u32,
    count: u32,
}

impl Stats {
    fn record(&mut self, elapsed: Duration, ok: bool) {
        if ok {
            if self.count == 0 || elapsed < self.min {
                self.min = elapsed;
            }
            if elapsed > self.max {
                self.max = elapsed;
            }
            self.total += elapsed;
            self.successes += 1;
        } else {
            self.failures += 1;
        }
        self.count += 1;
    }

    fn avg(&self) -> Duration {
        if self.successes == 0 {
            return Duration::ZERO;
        }
        self.total / self.successes
    }
}

fn main() {
    let api_key = env::var("key_1").expect("key_1 environment variable not set");
    let agent = Agent::new();

    let mut all_stats = vec![Stats::default(); ENDPOINTS.len()];

    for _iter in 1..=ITERATIONS {
        let handles: Vec<_> = ENDPOINTS
            .iter()
            .enumerate()
            .map(|(i, prefix)| {
                let agent = agent.clone();
                let api_key = api_key.clone();
                let url = format!("http://{}.rpc.orbitflare.com", prefix);
                thread::spawn(move || -> (usize, Duration, bool) {
                    let start = Instant::now();
                    let ok = agent
                        .post(&url)
                        .set("x-api-key", &api_key)
                        .set("Content-Type", "application/json")
                        .send_bytes(RPC_BODY)
                        .is_ok();
                    let elapsed = start.elapsed();
                    (i, elapsed, ok)
                })
            })
            .collect();

        for h in handles {
            let (i, elapsed, ok) = h.join().unwrap();
            all_stats[i].record(elapsed, ok);
        }
    }

    // Print results
    println!("───┬──────────┬──────────┬──────────┬──────────┬───────");
    println!("   │   endpt  │   avg    │   min    │   max    │  ok%  ");
    println!("───┼──────────┼──────────┼──────────┼──────────┼───────");

    for (i, s) in all_stats.iter().enumerate() {
        let pct = if s.count > 0 {
            s.successes as f64 / s.count as f64 * 100.0
        } else {
            0.0
        };
        println!(
            "{:>2} │ {:>8} │ {:>8} │ {:>8} │ {:>8} │ {:>5.0}",
            i + 1,
            ENDPOINTS[i],
            fmt_dur(s.avg()),
            fmt_dur(s.min),
            fmt_dur(s.max),
            pct,
        );
    }
    println!("───┴──────────┴──────────┴──────────┴──────────┴───────");
}

fn fmt_dur(d: Duration) -> String {
    if d == Duration::ZERO {
        return "   -.--".into();
    }
    let micros = d.as_micros();
    if micros < 1000 {
        format!("{:>3} µs", micros)
    } else if micros < 1_000_000 {
        format!("{:>3}.{:02} ms", micros / 1000, (micros % 1000) / 10)
    } else {
        let ms = micros as f64 / 1000.0;
        format!("{:>6.1} ms", ms)
    }
}
