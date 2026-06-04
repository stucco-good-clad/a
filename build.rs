fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .compile_protos(
            &["proto/old-faithful.proto", "proto/solana-storage.proto"],
            &["proto/"],
        )?;
    Ok(())
}
