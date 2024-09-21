use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::Bound;
use std::str;

use anyhow;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::cached_reader::CachedReader;
use super::raw_reader::PbfReader;
use super::traits::PbfRandomRead;
use crate::models::{Element, ElementType, Node, Relation, Way};
use crate::utils::file;

fn get_index_path_from_pbf_path(pbf_path: &str) -> String {
    let mut index_path = pbf_path.to_owned();
    let last_dot_index = index_path.rfind('.').unwrap();
    index_path.replace_range(last_dot_index..pbf_path.len(), ".pif");
    return index_path;
}

struct PbfIndex {
    node_index: BTreeMap<i64, u64>,
    way_index: BTreeMap<i64, u64>,
    relation_index: BTreeMap<i64, u64>,
}

impl PbfIndex {
    pub fn new(pbf_file: &str) -> anyhow::Result<Self> {
        if !pbf_file.ends_with(".pbf") {
            bail!("It's not a .pbf file")
        }

        let index_file_path = get_index_path_from_pbf_path(pbf_file);
        // Calculating the checksum of the pbf file...
        let checksum = file::checksum(pbf_file)?;

        if file::exists(&index_file_path) {
            // PBF index file already exists
            let (pi, checksum_in_file) = PbfIndex::load_from_file(&index_file_path)?;
            if checksum.eq(&checksum_in_file) {
                // The checksum is consistent. The index loading is complete
                return Ok(pi);
            }
        }

        let pbf_index = PbfIndex::load_from_pbf_file(pbf_file)?;
        pbf_index.persist(&index_file_path, &checksum)?;

        Ok(pbf_index)
    }

    fn load_from_file(index_path: &str) -> anyhow::Result<(PbfIndex, String)> {
        let mut node_index: BTreeMap<i64, u64> = BTreeMap::new();
        let mut way_index: BTreeMap<i64, u64> = BTreeMap::new();
        let mut relation_index: BTreeMap<i64, u64> = BTreeMap::new();

        let index_file = File::open(index_path)?;
        let mut reader = BufReader::new(index_file);

        let mut md5_buf = [0u8; 32];
        reader.read_exact(&mut md5_buf)?;
        let checksum = str::from_utf8(&md5_buf)?;

        loop {
            let write_type = reader.read_u8()?;
            if write_type == 0 {
                break;
            }

            let id = reader.read_i64::<LittleEndian>()?;
            let offset = reader.read_u64::<LittleEndian>()?;
            match write_type {
                1 => node_index.insert(id, offset),
                2 => way_index.insert(id, offset),
                3 => relation_index.insert(id, offset),
                _ => bail!("Unsupported write type"),
            };
        }

        Ok((
            PbfIndex {
                node_index,
                way_index,
                relation_index,
            },
            checksum.to_string(),
        ))
    }

    fn load_from_pbf_file(pbf_file_path: &str) -> anyhow::Result<PbfIndex> {
        // Indexing...
        let mut node_index: BTreeMap<i64, u64> = BTreeMap::new();
        let mut way_index: BTreeMap<i64, u64> = BTreeMap::new();
        let mut relation_index: BTreeMap<i64, u64> = BTreeMap::new();

        let mut reader = PbfReader::from_path(pbf_file_path)?;
        while let Some(blob_data) = reader.read_next_blob() {
            if blob_data.nodes.len() > 0 {
                let last = blob_data.nodes.last().unwrap();
                node_index.insert(last.id, blob_data.offset);
            }
            if blob_data.ways.len() > 0 {
                let last = blob_data.ways.last().unwrap();
                way_index.insert(last.id, blob_data.offset);
            }
            if blob_data.relations.len() > 0 {
                let last = blob_data.relations.last().unwrap();
                relation_index.insert(last.id, blob_data.offset);
            }
        }

        let index_instance = PbfIndex {
            node_index,
            way_index,
            relation_index,
        };
        // Indexing completed
        Ok(index_instance)
    }

    pub fn get_offset(&self, element_type: &ElementType, element_id: i64) -> Option<u64> {
        let cursor = match element_type {
            ElementType::Node => self.node_index.lower_bound(Bound::Included(&element_id)),
            ElementType::Way => self.way_index.lower_bound(Bound::Included(&element_id)),
            ElementType::Relation => self
                .relation_index
                .lower_bound(Bound::Included(&element_id)),
        };
        match cursor.peek_next() {
            Some((_, offset)) => Some(*offset),
            None => None,
        }
    }

    fn persist(&self, index_path: &str, checksum: &str) -> anyhow::Result<()> {
        // Saving the index to file...
        let index_file = File::create(index_path)?;
        let mut writer = BufWriter::new(index_file);
        // write checksum
        writer.write_all(checksum.as_bytes())?;
        // write index
        Self::persist_index_map(&mut writer, &self.node_index, 1)?;
        Self::persist_index_map(&mut writer, &self.way_index, 2)?;
        Self::persist_index_map(&mut writer, &self.relation_index, 3)?;

        // write an end symbol
        writer.write_u8(0)?;
        writer.flush()?;
        // Saving completed
        Ok(())
    }

    fn persist_index_map(
        writer: &mut BufWriter<File>,
        index_map: &BTreeMap<i64, u64>,
        write_type: u8,
    ) -> anyhow::Result<()> {
        for (eid, offset) in index_map.iter() {
            writer.write_u8(write_type)?;
            writer.write_i64::<LittleEndian>(*eid)?;
            writer.write_u64::<LittleEndian>(*offset)?;
        }
        Ok(())
    }
}

/// A reader that provides indexed access to PBF file.
///
/// The `IndexedReader` struct allows for efficient random access to PBF file by using an index.
/// It is generic over a type `T` that implements the `PbfRandomRead` trait, which provides the
/// necessary methods for reading PBF data.
///
/// # Type Parameters
///
/// * `T` - A type that implements the `PbfRandomRead` trait, providing methods for random access
///         reading of PBF data.
///
/// # Fields
///
/// * `pbf_reader` - An instance of type `T` that is used to read the PBF data.
/// * `pbf_index` - An instance of `PbfIndex` that provides the index for efficient random access.
///
/// # Example
///
/// ```rust
/// use pbf_craft::models::ElementType;
/// use pbf_craft::readers::IndexedReader;
///
/// let mut indexed_reader = IndexedReader::from_path("resources/andorra-latest.osm.pbf").unwrap();
/// let result = indexed_reader.find(&ElementType::Node, 4254529698).unwrap();
/// if let Some(ec) = result {
///    println!("Found element: {:?}", ec);
/// }
/// ```
///
/// If you want to read elements of a PBF file frequently, then the version with caching
/// will make reading more efficient
///
/// ```rust
/// use pbf_craft::models::ElementType;
/// use pbf_craft::readers::IndexedReader;
///
/// let mut indexed_reader = IndexedReader::from_path_with_cache("resources/andorra-latest.osm.pbf", 1000).unwrap();
/// let element_list = indexed_reader.get_with_deps(&ElementType::Way, 1055523837).unwrap();
/// ```
///
pub struct IndexedReader<T: PbfRandomRead> {
    pbf_reader: T,
    pbf_index: PbfIndex,
}

impl IndexedReader<PbfReader<BufReader<File>>> {
    /// Creates a new `IndexedReader` instance from a PBF file.
    pub fn from_path(pbf_file: &str) -> anyhow::Result<IndexedReader<PbfReader<BufReader<File>>>> {
        let pbf_index = PbfIndex::new(pbf_file)?;
        let pbf_reader = PbfReader::from_path(pbf_file)?;
        Ok(IndexedReader {
            pbf_index,
            pbf_reader,
        })
    }
}

impl IndexedReader<CachedReader> {
    /// Creates a new `IndexedReader` instance from a PBF file with a cache.
    ///
    /// # Parameters
    ///
    /// * pbf_file - A path to the PBF file.
    /// * cache_capacity - The capacity of the cache. The cache is used to store the parsed Blob from the PBF file.
    ///                    By default, a Blob contains about 8000 elements. Please decide the appropriate capacity
    ///                    according to your memory size.
    ///
    pub fn from_path_with_cache(
        pbf_file: &str,
        cache_capacity: usize,
    ) -> anyhow::Result<IndexedReader<CachedReader>> {
        let pbf_index = PbfIndex::new(pbf_file)?;
        let pbf_reader = PbfReader::from_path(pbf_file)?;
        let cached_reader = CachedReader::new(pbf_reader, cache_capacity);
        Ok(IndexedReader {
            pbf_index,
            pbf_reader: cached_reader,
        })
    }
}

impl<T: PbfRandomRead> IndexedReader<T> {
    /// Finds an node by its ID.
    pub fn find_node(&mut self, node_id: i64) -> anyhow::Result<Option<Node>> {
        let has_offset = self.pbf_index.get_offset(&ElementType::Node, node_id);
        if has_offset.is_none() {
            return Ok(None);
        }
        let offset = has_offset.unwrap();
        let blob_data = self.pbf_reader.read_blob_by_offset(offset)?;
        let node = blob_data.nodes.iter().find(|node| node.id == node_id);
        match node {
            Some(n) => Ok(Some(n.clone())),
            None => Ok(None),
        }
    }

    /// Finds nodes by their IDs.
    ///
    /// `find_nodes` is more efficient than calling `find_node` multiple times when you have a batch of node IDs.
    ///
    pub fn find_nodes(&mut self, node_ids: &[i64]) -> anyhow::Result<Vec<Node>> {
        let offsets: HashSet<u64> = node_ids
            .into_iter()
            .filter_map(|id| self.pbf_index.get_offset(&ElementType::Node, *id))
            .collect();
        let result: Vec<Node> = offsets
            .into_iter()
            .flat_map(|offset| {
                let blob_data = self.pbf_reader.read_blob_by_offset(offset).unwrap();
                let nodes: Vec<Node> = blob_data
                    .nodes
                    .iter()
                    .filter(|node| node_ids.contains(&node.id))
                    .map(|node| node.clone())
                    .collect();
                nodes
            })
            .collect();
        Ok(result)
    }

    /// Finds a way by its ID.
    pub fn find_way(&mut self, way_id: i64) -> anyhow::Result<Option<Way>> {
        let has_offset = self.pbf_index.get_offset(&ElementType::Way, way_id);
        if has_offset.is_none() {
            return Ok(None);
        }
        let offset = has_offset.unwrap();
        let blob_data = self.pbf_reader.read_blob_by_offset(offset)?;
        let way = blob_data.ways.iter().find(|way| way.id == way_id);
        match way {
            Some(w) => Ok(Some(w.clone())),
            None => Ok(None),
        }
    }

    /// Finds ways by their IDs.
    ///
    /// `find_ways` is more efficient than calling `find_way` multiple times when you have a batch of way IDs.
    ///
    pub fn find_ways(&mut self, way_ids: &[i64]) -> anyhow::Result<Vec<Way>> {
        let offsets: HashSet<u64> = way_ids
            .into_iter()
            .filter_map(|id| self.pbf_index.get_offset(&ElementType::Way, *id))
            .collect();
        let result: Vec<Way> = offsets
            .into_iter()
            .flat_map(|offset| {
                let blob_data = self.pbf_reader.read_blob_by_offset(offset).unwrap();
                let ways: Vec<Way> = blob_data
                    .ways
                    .iter()
                    .filter(|way| way_ids.contains(&way.id))
                    .map(|way| way.clone())
                    .collect();
                ways
            })
            .collect();
        Ok(result)
    }

    /// Finds a relation by its ID.
    pub fn find_relation(&mut self, relation_id: i64) -> anyhow::Result<Option<Relation>> {
        let has_offset = self
            .pbf_index
            .get_offset(&ElementType::Relation, relation_id);
        if has_offset.is_none() {
            return Ok(None);
        }
        let offset = has_offset.unwrap();
        let blob_data = self.pbf_reader.read_blob_by_offset(offset)?;
        let rel = blob_data
            .relations
            .iter()
            .find(|relation| relation.id == relation_id);
        match rel {
            Some(r) => Ok(Some(r.clone())),
            None => Ok(None),
        }
    }

    /// Finds relations by their IDs.
    ///
    /// `find_relations` is more efficient than calling `find_relation` multiple times when you have a batch of relation IDs.
    ///
    pub fn find_relations(&mut self, relation_ids: &[i64]) -> anyhow::Result<Vec<Relation>> {
        let offsets: HashSet<u64> = relation_ids
            .into_iter()
            .filter_map(|id| self.pbf_index.get_offset(&ElementType::Relation, *id))
            .collect();
        let result: Vec<Relation> = offsets
            .into_iter()
            .flat_map(|offset| {
                let blob_data = self.pbf_reader.read_blob_by_offset(offset).unwrap();
                let relations: Vec<Relation> = blob_data
                    .relations
                    .iter()
                    .filter(|relation| relation_ids.contains(&relation.id))
                    .map(|relation| relation.clone())
                    .collect();
                relations
            })
            .collect();
        Ok(result)
    }

    /// Finds an element by its type and ID.
    pub fn find(
        &mut self,
        element_type: &ElementType,
        element_id: i64,
    ) -> anyhow::Result<Option<Element>> {
        let target = match element_type {
            ElementType::Node => {
                let t = self.find_node(element_id)?;
                if t.is_none() {
                    return Ok(None);
                }
                Element::Node(t.unwrap())
            }
            ElementType::Way => {
                let t = self.find_way(element_id)?;
                if t.is_none() {
                    return Ok(None);
                }
                Element::Way(t.unwrap())
            }
            ElementType::Relation => {
                let t = self.find_relation(element_id)?;
                if t.is_none() {
                    return Ok(None);
                }
                Element::Relation(t.unwrap())
            }
        };
        Ok(Some(target))
    }

    /// Finds an element with its dependencies.
    ///
    /// When you want to get a Way, this method will also return the Nodes that the Way contains.
    /// When you want to get a Relation, this method will also return the Nodes, Ways, and Relations
    /// that the Relation contains.
    /// So, if you use this method to get a Node, it will be the same as calling `find_node`.
    ///
    /// It is highly recommended to use `IndexedReader::from_path_with_cache` to create an `IndexedReader` instance
    /// when you need to read elements with dependencies frequently.
    ///
    pub fn get_with_deps(
        &mut self,
        element_type: &ElementType,
        element_id: i64,
    ) -> anyhow::Result<Vec<Element>> {
        match element_type {
            ElementType::Node => {
                let node = self.find_node(element_id)?;
                if node.is_none() {
                    return Ok(Vec::with_capacity(0));
                }
                let node = node.unwrap();
                Ok(vec![Element::Node(node)])
            }
            ElementType::Way => self.get_way_with_deps(element_id),
            ElementType::Relation => self.get_relation_with_deps(element_id),
        }
    }

    fn get_way_with_deps(&mut self, way_id: i64) -> anyhow::Result<Vec<Element>> {
        let way = self.find_way(way_id)?;
        if way.is_none() {
            return Ok(Vec::with_capacity(0));
        }
        let way = way.unwrap();
        let node_ids: Vec<i64> = way.way_nodes.iter().map(|way_node| way_node.id).collect();
        let nodes = self.find_nodes(&node_ids)?;

        let mut result: Vec<Element> = vec![Element::Way(way)];
        result.extend(nodes.into_iter().map(|node| Element::Node(node)));
        Ok(result)
    }

    fn get_relation_with_deps(&mut self, relation_id: i64) -> anyhow::Result<Vec<Element>> {
        let mut result = Vec::new();

        let relation = self.find_relation(relation_id)?;
        if relation.is_none() {
            return Ok(Vec::with_capacity(0));
        }
        let relation = relation.unwrap();
        result.push(Element::Relation(relation.clone()));

        let node_ids: Vec<i64> = relation
            .members
            .iter()
            .filter_map(|member| {
                if member.member_type == ElementType::Node {
                    Some(member.member_id)
                } else {
                    None
                }
            })
            .collect();
        self.find_nodes(node_ids.as_slice())?
            .into_iter()
            .for_each(|node| result.push(Element::Node(node)));

        let way_ids: Vec<i64> = relation
            .members
            .iter()
            .filter_map(|member| {
                if member.member_type == ElementType::Way {
                    Some(member.member_id)
                } else {
                    None
                }
            })
            .collect();
        result = way_ids
            .into_iter()
            .map(|way_id| self.get_way_with_deps(way_id).unwrap())
            .fold(result, |mut acc, x| {
                acc.extend(x);
                acc
            });

        let relation_ids: Vec<i64> = relation
            .members
            .iter()
            .filter_map(|member| {
                if member.member_type == ElementType::Relation {
                    Some(member.member_id)
                } else {
                    None
                }
            })
            .collect();
        result = relation_ids
            .into_iter()
            .map(|relation_id| self.get_relation_with_deps(relation_id).unwrap())
            .fold(result, |mut acc, x| {
                acc.extend(x);
                acc
            });

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_from_pbf_file() {
        let pbf_file = "./resources/andorra-latest.osm.pbf";
        let index_file = PbfIndex::load_from_pbf_file(pbf_file).unwrap();

        let r1 = index_file.get_offset(&ElementType::Node, 52263877);
        let r2 = index_file.get_offset(&ElementType::Node, 52263878);
        assert_eq!(r1, Some(171));
        assert_eq!(r2, Some(49494));
    }

    #[test]
    fn test_index_from_file() {
        let index_file = "./resources/andorra-latest.osm.pif";
        let (pbf_index, checksum) = PbfIndex::load_from_file(index_file).unwrap();
        assert_eq!(&checksum, "ba8a2a59183a49c3e624246b8e8138a5");

        let r1 = pbf_index.get_offset(&ElementType::Node, 52263877);
        let r2 = pbf_index.get_offset(&ElementType::Node, 52263878);
        assert_eq!(r1, Some(171));
        assert_eq!(r2, Some(49494));
    }

    #[test]
    fn test_index_reader_read() {
        let pbf_file = "./resources/andorra-latest.osm.pbf";
        let mut indexed_reader = IndexedReader::from_path(pbf_file).unwrap();
        let target_op = indexed_reader.find(&ElementType::Node, 4254529698).unwrap();
        let target = target_op.unwrap();
        if let Element::Node(node) = target {
            assert_eq!(node.id, 4254529698);
        } else {
            assert!(false);
        }

        let target_op = indexed_reader.find(&ElementType::Way, 1055523837).unwrap();
        let target = target_op.unwrap();
        if let Element::Way(way) = target {
            assert_eq!(way.id, 1055523837);
        } else {
            assert!(false);
        }
    }
}
