use std::{fs::File, io::BufReader, ops::Deref, rc::Rc};

use quick_cache::unsync::Cache;

use super::raw_reader::PbfReader;
use super::traits::{BlobData, PbfRandomRead};

pub struct CachedReader {
    reader: PbfReader<BufReader<File>>,
    blob_cache: Cache<u64, Rc<BlobData>>,
}

impl CachedReader {
    pub fn new(reader: PbfReader<BufReader<File>>, cache_capacity: usize) -> Self {
        Self {
            reader,
            blob_cache: Cache::new(cache_capacity),
        }
    }
}

impl PbfRandomRead for CachedReader {
    fn read_blob_by_offset(&mut self, offset: u64) -> anyhow::Result<Rc<BlobData>> {
        match self.blob_cache.get(&offset) {
            Some(blob) => Ok(blob.clone()),
            None => {
                let blob = self.reader.read_blob_by_offset(offset)?;
                self.blob_cache.insert(offset, blob.clone());
                Ok(blob)
            }
        }
    }
}

impl Deref for CachedReader {
    type Target = PbfReader<BufReader<File>>;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}
