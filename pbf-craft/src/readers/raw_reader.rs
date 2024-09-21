use rayon::prelude::*;

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::rc::Rc;

use super::traits::{BlobData, PbfRandomRead};
use crate::codecs::blob::{BlobReader, DecodedBlob};
use crate::codecs::block_decorators::{HeaderReader, PrimitiveReader};
use crate::models::{Element, ElementType};

/// A reader for Protocolbuffer Binary Format (PBF) files.
///
/// The `PbfReader` struct provides functionality to read and process PBF files,
/// which are commonly used for storing OpenStreetMap (OSM) data. It wraps around
/// a `BlobReader` to handle the low-level reading of blobs from the input source.
///
/// # Type Parameters
///
/// * `R` - A type that implements the `Read` and `Send` traits. This is typically
///   a file or a network stream from which the PBF data is read.
///
/// # Example
///
/// ```rust
/// use pbf_craft::readers::PbfReader;
///
/// let mut reader = PbfReader::from_path("resources/andorra-latest.osm.pbf").unwrap();
/// reader.read(|header, element| {
///     if let Some(header_reader) = header {
///         // Process header
///     }
///     if let Some(element) = element {
///         // Process element
///     }
/// }).unwrap();
/// ```
pub struct PbfReader<R: Read + Send> {
    blob_reader: BlobReader<R>,
}

impl<R: Read + Send> PbfReader<R> {
    /// Creates a new `PbfReader` instance with the specified reader which implements `Read` and `Send` traits.
    pub fn new(reader: R) -> PbfReader<R> {
        Self {
            blob_reader: BlobReader::new(reader),
        }
    }

    pub fn read_next_blob(&mut self) -> Option<BlobData> {
        if self.blob_reader.eof {
            None
        } else {
            let offset = self.blob_reader.offset;
            match self.blob_reader.next() {
                Some(blob) => match blob.decode().expect("Failed to decode block.") {
                    DecodedBlob::OsmHeader(_) => {
                        return Some(BlobData {
                            nodes: Vec::with_capacity(0),
                            ways: Vec::with_capacity(0),
                            relations: Vec::with_capacity(0),
                            offset,
                        })
                    }
                    DecodedBlob::OsmData(data) => {
                        let decorator = PrimitiveReader::new(data);
                        let (nodes, ways, relations) = decorator.get_all_elements();
                        return Some(BlobData {
                            nodes,
                            ways,
                            relations,
                            offset,
                        });
                    }
                },
                None => None,
            }
        }
    }

    /// Reads and processes header and elements using the provided callback function.
    ///
    /// This is a single-threaded method where all elements are iterated over one by one
    /// in the order recorded in the PBF file.
    ///
    /// # Arguments
    ///
    /// * `callback` - A mutable closure that takes two optional arguments:
    ///     - `Option<HeaderReader>`: Some if a header is decoded, None otherwise.
    ///     - `Option<Element>`: Some if an element is decoded, None otherwise.
    ///
    /// # Returns
    ///
    /// * `anyhow::Result<()>` - Returns an Ok result if all blobs are processed successfully,
    ///   or an error if any blob decoding fails.
    ///
    /// # Errors
    ///
    /// This function will return an error if any PBF decoding fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// use pbf_craft::readers::PbfReader;
    ///
    /// let mut reader = PbfReader::from_path("resources/andorra-latest.osm.pbf").unwrap();
    /// reader.read(|header, element| {
    ///     if let Some(header_reader) = header {
    ///         // Process header
    ///     }
    ///     if let Some(element) = element {
    ///         // Process element
    ///     }
    /// }).unwrap();
    /// ```
    pub fn read<F>(&mut self, mut callback: F) -> anyhow::Result<()>
    where
        F: FnMut(Option<HeaderReader>, Option<Element>),
    {
        for blob in &mut self.blob_reader {
            match blob.decode()? {
                DecodedBlob::OsmHeader(b) => {
                    let header_reader = HeaderReader::new(b);
                    callback(Some(header_reader), None);
                }
                DecodedBlob::OsmData(data) => {
                    let decorator = PrimitiveReader::new(data);
                    decorator.for_each_element(|el| callback(None, Some(el)));
                }
            }
        }
        Ok(())
    }

    /// Finds elements in parallel.
    ///
    /// # Arguments
    ///
    /// * `inclination` - An optional reference to an `ElementType` that specifies the type of elements to find.
    ///                   If `None`, all element types are considered.
    /// * `callback` - A closure that takes a reference to an `Element` and returns a boolean indicating
    ///                whether the element should be included in the result. The closure must be `Send` and `Sync`.
    ///
    /// # Returns
    ///
    /// * `anyhow::Result<Vec<Element>>` - Returns a vector of elements that match the criteria specified
    ///   by the callback function. If an error occurs during PBF decoding, an error is returned.
    ///
    /// # Errors
    ///
    /// This function will return an error if any PBF decoding fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// use pbf_craft::models::ElementType;
    /// use pbf_craft::readers::PbfReader;
    ///
    /// let mut reader = PbfReader::from_path("resources/andorra-latest.osm.pbf").unwrap();
    /// let elements = reader.par_find(Some(&ElementType::Node), |element| {
    ///     // Filter logic for nodes
    ///     true
    /// }).unwrap();
    /// ```
    pub fn par_find<F>(
        self,
        inclination: Option<&ElementType>,
        callback: F,
    ) -> anyhow::Result<Vec<Element>>
    where
        F: Fn(&Element) -> bool + Send + Sync,
    {
        let result = self
            .blob_reader
            .par_bridge()
            .filter_map(
                |blob| match blob.decode().expect("decode raw blob failed.") {
                    DecodedBlob::OsmHeader(_) => None,
                    DecodedBlob::OsmData(b) => Some(PrimitiveReader::new(b)),
                },
            )
            .filter_map(|p| {
                if let Some(element_type) = inclination {
                    let result = match element_type {
                        ElementType::Node => p
                            .get_nodes()
                            .into_iter()
                            .map(|i| Element::Node(i))
                            .filter(&callback)
                            .collect::<Vec<Element>>(),
                        ElementType::Way => p
                            .get_ways()
                            .into_iter()
                            .map(|i| Element::Way(i))
                            .filter(&callback)
                            .collect::<Vec<Element>>(),
                        ElementType::Relation => p
                            .get_relations()
                            .into_iter()
                            .map(|i| Element::Relation(i))
                            .filter(&callback)
                            .collect::<Vec<Element>>(),
                    };
                    Some(result)
                } else {
                    let (nodes, ways, relations) = p.get_all_elements();
                    let mut filterd_nodes: Vec<Element> = nodes
                        .into_iter()
                        .map(|i| Element::Node(i))
                        .filter(&callback)
                        .collect();
                    let mut filterd_ways: Vec<Element> = ways
                        .into_iter()
                        .map(|i| Element::Way(i))
                        .filter(&callback)
                        .collect();
                    let mut filterd_relations: Vec<Element> = relations
                        .into_iter()
                        .map(|i| Element::Relation(i))
                        .filter(&callback)
                        .collect();

                    filterd_nodes.append(&mut filterd_ways);
                    filterd_nodes.append(&mut filterd_relations);
                    Some(filterd_nodes)
                }
            })
            .reduce(
                || Vec::new(),
                |mut a, mut b| {
                    a.append(&mut b);
                    a
                },
            );

        Ok(result)
    }
}

impl PbfReader<BufReader<File>> {
    /// Creates a new `PbfReader` instance with the specified file path.
    pub fn from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let f = File::open(path)?;
        let reader = BufReader::new(f);
        Ok(Self::new(reader))
    }

    /// Rewinds the reader to the beginning of the file.
    pub fn rewind(&mut self) -> anyhow::Result<()> {
        self.blob_reader.rewind()
    }
}

impl PbfRandomRead for PbfReader<BufReader<File>> {
    fn read_blob_by_offset(&mut self, offset: u64) -> anyhow::Result<Rc<BlobData>> {
        self.blob_reader.seek(offset)?;
        let data = self
            .read_next_blob()
            .ok_or(anyhow!("no blob data found."))?;
        Ok(Rc::new(data))
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use test::Bencher;
//
//     #[bench]
//     fn bench_read(b: &mut Bencher) {
//         b.iter(|| {
//             let mut reader = PbfReader::from_path("./tests/andorra-latest.osm.pbf").unwrap();
//             let _ = reader.read(|el| {});
//         });
//     }
//
//     #[bench]
//     fn bench_par_read(b: &mut Bencher) {
//         b.iter(|| {
//             let reader = PbfReader::from_path("./tests/andorra-latest.osm.pbf").unwrap();
//             let (tx, rx) = mpsc::channel();
//             reader.par_read(tx);
//             loop {
//                 match rx.recv().expect("error") {
//                     None => break,
//                     Some(el) => {
//                         // println!("{:?}", el);
//                     }
//                 }
//             }
//         });
//     }
// }
