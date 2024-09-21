use std::io::Write;

static MOD_RS: &[u8] = b"
pub mod fileformat;
pub mod osmformat;
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = ["src/proto/fileformat.proto", "src/proto/osmformat.proto"];

    for path in &proto_files {
        println!("cargo:rerun-if-changed={}", path);
    }

    let out_dir = "src/proto".to_string();

    protobuf_codegen_pure::Codegen::new()
        .out_dir(&out_dir)
        .inputs(&proto_files)
        .include("src/proto")
        .run()?;

    std::fs::File::create(out_dir + "/mod.rs")?.write_all(MOD_RS)?;

    Ok(())
}
