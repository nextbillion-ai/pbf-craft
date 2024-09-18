use rayon::prelude::*;

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::rc::Rc;

use super::traits::{BlobData, PbfRandomRead};
use crate::models::{Element, ElementType, Tag};
use crate::pbf::codecs::blob::{BlobReader, DecodedBlob};
use crate::pbf::codecs::block_decorators::{HeaderReader, PrimitiveReader};

pub struct PbfReader<R: Read + Send> {
    blob_reader: BlobReader<R>,
}

impl<R: Read + Send> PbfReader<R> {
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

    pub fn max_ids(self) -> anyhow::Result<(i64, i64, i64)> {
        let result = self
            .blob_reader
            .par_bridge()
            .filter_map(
                |blob| match blob.decode().expect("decode raw blob failed.") {
                    DecodedBlob::OsmHeader(_) => None,
                    DecodedBlob::OsmData(b) => Some(PrimitiveReader::new(b)),
                },
            )
            .map(|p| {
                let (nodes, ways, relations) = p.get_all_elements();
                let node_id = nodes.into_iter().map(|e| e.id).max().or(Some(0)).unwrap();
                let way_id = ways.into_iter().map(|e| e.id).max().or(Some(0)).unwrap();
                let relation_id = relations
                    .into_iter()
                    .map(|e| e.id)
                    .max()
                    .or(Some(0))
                    .unwrap();

                (node_id, way_id, relation_id)
            })
            .reduce(
                || (0, 0, 0),
                |a, b| (a.0.max(b.0), a.1.max(b.1), a.2.max(b.2)),
            );

        Ok(result)
    }

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

    pub fn find_all_by_id(self, element_type: &ElementType, element_id: i64) -> Vec<Element> {
        self.par_find(None, |element| match (element, &element_type) {
            (Element::Node(node), ElementType::Node) => node.id == element_id,
            (Element::Way(way), ElementType::Node) => {
                for way_node in &way.way_nodes {
                    if way_node.id == element_id {
                        return true;
                    }
                }
                return false;
            }
            (Element::Way(way), ElementType::Way) => way.id == element_id,
            (Element::Relation(relation), ElementType::Relation) => relation.id == element_id,
            (Element::Relation(relation), _) => {
                for member in &relation.members {
                    if member.member_id == element_id && member.member_type.eq(element_type) {
                        return true;
                    }
                }
                return false;
            }
            _ => false,
        })
        .expect("read pbf failed")
    }

    pub fn find_all_by_tag(self, key: &Option<String>, value: &Option<String>) -> Vec<Element> {
        self.par_find(None, |element| match element {
            Element::Node(node) => Self::does_tag_match(&node.tags, &key, &value),
            Element::Way(way) => Self::does_tag_match(&way.tags, &key, &value),
            Element::Relation(relation) => Self::does_tag_match(&relation.tags, &key, &value),
        })
        .expect("read pbf failed")
    }

    fn does_tag_match(tags: &Vec<Tag>, key: &Option<String>, value: &Option<String>) -> bool {
        for tag in tags {
            match (key, value) {
                (Some(k), Some(v)) => {
                    if tag.key.contains(k) && tag.value.contains(v) {
                        return true;
                    }
                }
                (Some(k), None) => {
                    if tag.key.contains(k) {
                        return true;
                    }
                }
                (None, Some(v)) => {
                    if tag.value.contains(v) {
                        return true;
                    }
                }
                (None, None) => return true,
            }
        }
        false
    }
}

impl PbfReader<BufReader<File>> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let f = File::open(path)?;
        let reader = BufReader::new(f);
        Ok(Self::new(reader))
    }

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
