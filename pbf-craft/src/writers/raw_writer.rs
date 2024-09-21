use std::fs::File;
use std::io::{BufWriter, Write};
use std::mem;
use std::path::Path;

use byteorder::{self, WriteBytesExt};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use protobuf::Message;

use crate::codecs::block_builder::PrimitiveBuilder;
use crate::models::{Bound, Element};
use crate::proto::{fileformat, osmformat};

const MAX_BLOCK_ITEM_LENGTH: usize = 8000;

/// A writer for creating PBF files.
///
/// The `PbfWriter` struct provides functionality to write PBF data to an underlying writer.
/// It supports writing elements in either dense or non-dense format and can include optional
/// bounding box information.
///
/// Please note: According to the PBF specification, you should write the elements in the order of
/// Node, Way, Relation, and for all elements of each type, the IDs should be written in the order
/// of smallest to largest. PbfWriter writes elements in the order in which `write` is called, so it
/// is up to the programmer to make sure that elements are written in the proper order.
///
/// # Type Parameters
///
/// * `W` - A type that implements the `Write` trait, which is used to write the PBF data.
///
/// # Example
///
/// ```rust
/// use pbf_craft::models::{Element, Node};
/// use pbf_craft::writers::PbfWriter;
///
/// let mut writer = PbfWriter::from_path("resources/output.pbf", true).unwrap();
/// writer.write(Element::Node(Node::default())).unwrap();
/// writer.finish().unwrap();
/// ```
pub struct PbfWriter<W: Write> {
    writer: W,
    use_dense: bool,
    bbox: Option<Bound>,
    cache: Vec<Element>,
    has_writen_header: bool,
}

impl PbfWriter<BufWriter<File>> {
    /// Creates a new `PbfWriter` from a file path.
    ///
    /// # Parameters
    ///
    /// * `path` - The path to the file to write the PBF data to.
    /// * `use_dense` - A boolean value indicating whether to use dense format for writing nodes.
    ///
    pub fn from_path<P: AsRef<Path>>(path: P, use_dense: bool) -> anyhow::Result<Self> {
        let f = File::create(path)?;
        let writer = BufWriter::new(f);
        Ok(Self::new(writer, use_dense))
    }
}

impl<W: Write> PbfWriter<W> {
    /// Creates a new `PbfWriter` from an existing writer.
    ///
    /// # Parameters
    ///
    /// * `writer` - The writer to use for writing the PBF data. It should implement the `Write`
    ///              trait, which is used to write the PBF data
    /// * `use_dense` - A boolean value indicating whether to use dense format for writing nodes.
    ///
    pub fn new(writer: W, use_dense: bool) -> PbfWriter<W> {
        Self {
            writer,
            use_dense,
            bbox: None,
            cache: Vec::new(),
            has_writen_header: false,
        }
    }

    fn build_raw_blob(&mut self, raw: Vec<u8>) -> anyhow::Result<fileformat::Blob> {
        let raw_size = raw.len();
        let mut zlib_encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        zlib_encoder.write_all(raw.as_slice())?;
        let compressed = zlib_encoder.finish()?;

        let mut blob = fileformat::Blob::new();
        blob.set_zlib_data(compressed);
        blob.set_raw_size(raw_size as i32);
        Ok(blob)
    }

    /// Sets the bounding box for the PBF file.
    ///
    /// If you want to include a bounding box in the PBF file, you set it before writing any elements.
    ///
    pub fn set_bbox(&mut self, bbox: Bound) {
        self.bbox = Some(bbox);
    }

    fn write_header(&mut self) -> anyhow::Result<()> {
        let mut header_block = osmformat::HeaderBlock::new();
        header_block
            .required_features
            .push("OsmSchema-V0.6".to_string());
        if self.use_dense {
            header_block
                .required_features
                .push("DenseNodes".to_string());
        }

        if let Some(bbox) = &self.bbox {
            let mut header_bbox = osmformat::HeaderBBox::new();
            header_bbox.set_left(bbox.left);
            header_bbox.set_right(bbox.right);
            header_bbox.set_top(bbox.top);
            header_bbox.set_bottom(bbox.bottom);
            header_block.set_bbox(header_bbox);
            header_block.set_source(bbox.origin.clone());
        }

        let blob = self.build_raw_blob(header_block.write_to_bytes()?)?;
        self.write_blob(blob, "OSMHeader")?;
        self.has_writen_header = true;
        Ok(())
    }

    /// Writes an element.
    ///
    /// Please note: According to the PBF specification, you should write the elements in the order of
    /// Node, Way, Relation, and for all elements of each type, the IDs should be written in the order
    /// of smallest to largest. PbfWriter writes elements in the order in which `write` is called, so it
    /// is up to the programmer to make sure that elements are written in the proper order.
    ///
    pub fn write(&mut self, element: Element) -> anyhow::Result<()> {
        self.cache.push(element);
        if self.cache.len() >= MAX_BLOCK_ITEM_LENGTH {
            self.write_to_block()?;
        }
        Ok(())
    }

    fn write_to_block(&mut self) -> anyhow::Result<()> {
        if !self.has_writen_header {
            self.write_header()?;
        }
        let block_builder = PrimitiveBuilder::new();
        let cache = mem::replace(&mut self.cache, Vec::new());
        let block = block_builder.build(cache, self.use_dense);

        let blob = self.build_raw_blob(block.write_to_bytes()?)?;
        self.write_blob(blob, "OSMData")?;
        Ok(())
    }

    fn write_blob(&mut self, blob: fileformat::Blob, blob_type: &str) -> anyhow::Result<()> {
        let blob_bytes = blob.write_to_bytes()?;

        let mut header = fileformat::BlobHeader::new();
        header.set_datasize(blob_bytes.len() as i32);
        header.set_field_type(blob_type.to_owned());
        let header_bytes = header.write_to_bytes()?;

        self.writer
            .write_u32::<byteorder::BigEndian>(header_bytes.len() as u32)?;
        self.writer.write_all(header_bytes.as_slice())?;
        self.writer.write_all(blob_bytes.as_slice())?;

        Ok(())
    }

    /// Finishes writing the PBF file.
    ///
    /// This method should be called after writing all elements to the PBF file.
    ///
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.write_to_block()?;
        self.writer.flush()?;
        Ok(())
    }
}
