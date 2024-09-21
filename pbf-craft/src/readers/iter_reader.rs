use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use super::raw_reader::PbfReader;
use super::traits::BlobData;
use crate::models::{Element, ElementType};

/// A reader that provides an iterable interface for reading PBF data.
///
/// The `IterableReader` struct allows for sequential reading of PBF data by iterating over blobs
/// and elements. It is generic over a type `R` that implements the `Read` and `Send` traits, which
/// provide the necessary methods for reading PBF data from a source.
///
/// # Type Parameters
///
/// * `R` - A type that implements the `Read` and `Send` traits, providing methods for reading PBF data.
///
/// # Example
///
/// ```rust
/// use pbf_craft::models::{Element, ElementType};
/// use pbf_craft::readers::IterableReader;
///
/// let mut reader = IterableReader::from_path("resources/andorra-latest.osm.pbf").unwrap();
/// for element in reader {
///    // Process the element
/// }
/// ```
pub struct IterableReader<R: Read + Send> {
    pbf_reader: PbfReader<R>,
    current_blob: Option<BlobData>,
    current_element_type: ElementType,
    current_element_index: usize,
}

impl<R: Read + Send> IterableReader<R> {
    /// Creates a new `IterableReader` from a raw pbf reader.
    pub fn new(mut pbf_reader: PbfReader<R>) -> Self {
        Self {
            current_blob: pbf_reader.read_next_blob(),
            current_element_type: ElementType::Node,
            current_element_index: 0,
            pbf_reader,
        }
    }

    fn next_element(&mut self) -> Option<Element> {
        loop {
            if let Some(blob) = &self.current_blob {
                if ElementType::Node == self.current_element_type {
                    if self.current_element_index < blob.nodes.len() {
                        let node = blob.nodes.get(self.current_element_index).unwrap();
                        self.current_element_index += 1;
                        return Some(Element::Node(node.clone()));
                    } else {
                        self.current_element_type = ElementType::Way;
                        self.current_element_index = 0;
                    }
                }
                if ElementType::Way == self.current_element_type {
                    if self.current_element_index < blob.ways.len() {
                        let way = blob.ways.get(self.current_element_index).unwrap();
                        self.current_element_index += 1;
                        return Some(Element::Way(way.clone()));
                    } else {
                        self.current_element_type = ElementType::Relation;
                        self.current_element_index = 0;
                    }
                }
                if ElementType::Relation == self.current_element_type {
                    if self.current_element_index < blob.relations.len() {
                        let relation = blob.relations.get(self.current_element_index).unwrap();
                        self.current_element_index += 1;
                        return Some(Element::Relation(relation.clone()));
                    } else {
                        self.current_blob = self.pbf_reader.read_next_blob();
                        self.current_element_type = ElementType::Node;
                        self.current_element_index = 0;
                    }
                }
            } else {
                return None;
            }
        }
    }
}

impl<R: Read + Send> Iterator for IterableReader<R> {
    type Item = Element;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_element()
    }
}

impl IterableReader<BufReader<File>> {
    /// Creates a new `IterableReader` from a file path.
    pub fn from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let pbf_reader = PbfReader::from_path(path)?;
        Ok(Self::new(pbf_reader))
    }
}
