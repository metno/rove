fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/runner/runner.proto")?;
    // tonic_build::configure()
    //     .out_dir("src/coordinator")
    //     .compile(&["proto/coordinator.proto"], &["proto"])?;
    Ok(())
}
