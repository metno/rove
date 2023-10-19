fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("proto/rove.proto")?;
    // needed the extra flag to make docs.rs happy :(. we can probably switch
    // back to the commented version once they update their protoc
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["proto/rove.proto"], &["proto"])?;
    Ok(())
}
