mod plugins;

use jetstreamer::{JetstreamerInvocation, JetstreamerRunner};
use plugins::DexSwapPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "./output".to_string());

    let invocation = JetstreamerRunner::new()
        .with_log_level("info")
        .with_plugin(Box::new(DexSwapPlugin::new(output_dir)))
        .with_clickhouse_dsn("")
        .parse_cli_args()?;

    match invocation {
        JetstreamerInvocation::Run(runner) => {
            runner.run()?;
        }
        JetstreamerInvocation::ListPlugins => {
            println!("dex-swap");
        }
    }

    Ok(())
}
