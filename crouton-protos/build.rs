fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/catalog.proto")?;
    tonic_build::compile_protos("proto/replica.proto")?;
    Ok(())
}
