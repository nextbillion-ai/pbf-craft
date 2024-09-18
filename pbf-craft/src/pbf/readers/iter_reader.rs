use std::{
    fs::File,
    io::{BufReader, Read},
};

use super::raw_reader::PbfReader;
use super::traits::BlobData;
use crate::models::{Element, ElementType};

pub struct IterableReader<R: Read + Send> {
    pbf_reader: PbfReader<R>,
    current_blob: Option<BlobData>,
    current_element_type: ElementType,
    current_element_index: usize,
}

impl<R: Read + Send> IterableReader<R> {
    pub fn new(mut pbf_reader: PbfReader<R>) -> Self {
        Self {
            current_blob: pbf_reader.read_next_blob(),
            current_element_type: ElementType::Node,
            current_element_index: 0,
            pbf_reader,
        }
    }

    pub fn next_element(&mut self) -> Option<Element> {
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
    pub fn from_path(path: &str) -> anyhow::Result<Self> {
        let pbf_reader = PbfReader::from_path(path)?;
        Ok(Self::new(pbf_reader))
    }
}
