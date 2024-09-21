use std::fs::File;
use std::io::{BufReader, Read, Seek};

use byteorder::{self, ReadBytesExt};
use flate2::read::ZlibDecoder;

use crate::proto::fileformat::{Blob, BlobHeader};
use crate::proto::osmformat::{HeaderBlock, PrimitiveBlock};

pub enum DecodedBlob {
    OsmHeader(HeaderBlock),
    OsmData(PrimitiveBlock),
}

#[derive(Debug)]
pub struct RawBlob {
    header: BlobHeader,
    raw_blob: Vec<u8>,
}

impl RawBlob {
    pub fn decode(&self) -> anyhow::Result<DecodedBlob> {
        let decoded = match self.header.get_field_type() {
            "OSMHeader" => DecodedBlob::OsmHeader(self.decode_blob()?),
            "OSMData" => DecodedBlob::OsmData(self.decode_blob()?),
            _ => bail!("Unsupported header type: {}", self.header.get_field_type()),
        };
        Ok(decoded)
    }

    fn decode_blob<M: protobuf::Message>(&self) -> anyhow::Result<M> {
        let blob: Blob = protobuf::Message::parse_from_bytes(self.raw_blob.as_slice())?;
        let decoded: M = if blob.has_raw() {
            protobuf::Message::parse_from_bytes(blob.get_raw())?
        } else if blob.has_zlib_data() {
            let mut decoder = ZlibDecoder::new(blob.get_zlib_data());
            protobuf::Message::parse_from_reader(&mut decoder)?
        } else {
            bail!("Unsupported blob data type")
        };
        Ok(decoded)
    }
}

pub struct BlobReader<R: Read + Send> {
    reader: R,
    pub offset: u64,
    pub eof: bool,
}

impl<R: Read + Send> BlobReader<R> {
    pub fn new(reader: R) -> BlobReader<R> {
        Self {
            reader,
            offset: 0,
            eof: false,
        }
    }

    fn next_blob(&mut self) -> anyhow::Result<Option<RawBlob>> {
        let header_size = match self.reader.read_u32::<byteorder::BigEndian>() {
            Ok(n) => {
                self.offset += 4;
                n as u64
            }
            Err(ref err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.eof = true;
                return Ok(None);
            }
            Err(_) => {
                bail!("Unable to get next blob from PBF stream.");
            }
        };

        let header = self.read_blob_header(header_size)?;
        let raw_blob = self.read_blob(&header)?;
        Ok(Some(RawBlob { header, raw_blob }))
    }

    fn read_blob_header(&mut self, header_size: u64) -> anyhow::Result<BlobHeader> {
        let header: BlobHeader =
            protobuf::Message::parse_from_reader(&mut self.reader.by_ref().take(header_size))?;
        self.offset += header_size;
        Ok(header)
    }

    fn read_blob(&mut self, header: &BlobHeader) -> anyhow::Result<Vec<u8>> {
        let data_size = header.get_datasize() as usize;
        let mut bytes: Vec<u8> = Vec::with_capacity(data_size);
        let mut r = self.reader.by_ref().take(data_size as u64);
        match r.read_to_end(&mut bytes) {
            Ok(_) => {
                self.offset += data_size as u64;
                Ok(bytes)
            }
            Err(e) => bail!(e),
        }
    }
}

impl BlobReader<BufReader<File>> {
    pub fn seek(&mut self, offset: u64) -> anyhow::Result<()> {
        self.reader.seek(std::io::SeekFrom::Start(offset))?;
        self.offset = offset;
        Ok(())
    }

    pub fn rewind(&mut self) -> anyhow::Result<()> {
        self.reader.rewind()?;
        self.offset = 0;
        Ok(())
    }
}

impl<R: Read + Send> Iterator for BlobReader<R> {
    type Item = RawBlob;

    fn next(&mut self) -> Option<Self::Item> {
        if self.eof {
            None
        } else {
            match self.next_blob() {
                Ok(raw) => raw,
                Err(err) => {
                    panic!("{}", err);
                }
            }
        }
    }
}
