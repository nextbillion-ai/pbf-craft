use anyhow;
use base16ct;
use md5::{Digest, Md5};
use std::{fs, io, path};

pub(crate) fn exists(filepath: &str) -> bool {
    let file = path::Path::new(filepath);
    file.exists()
}

pub(crate) fn checksum(filepath: &str) -> anyhow::Result<String> {
    let mut file = fs::File::open(filepath)?;
    let mut hasher = Md5::new();
    let _ = io::copy(&mut file, &mut hasher)?;
    let hash = hasher.finalize();
    let mut buf = [0u8; 32];
    let hex_hash = base16ct::lower::encode_str(&hash, &mut buf).map_err(|e| anyhow!(e))?;
    Ok(hex_hash.to_owned())
}
