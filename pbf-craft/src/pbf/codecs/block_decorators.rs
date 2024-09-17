use std::collections::HashMap;

use super::field::FieldCodec;
use crate::models::{
    BaseElement, Bound, ElementContainer, ElementType, Node, OsmUser, Relation, RelationMember,
    Tag, Way, WayNode,
};
use crate::pbf::proto::osmformat;
use crate::pbf::proto::osmformat::Relation_MemberType;

pub struct HeaderReader {
    header: osmformat::HeaderBlock,
}

impl HeaderReader {
    pub fn new(header: osmformat::HeaderBlock) -> Self {
        Self { header }
    }

    pub fn meta(&self) -> HashMap<String, String> {
        let supported_features: Vec<&str> = vec!["OsmSchema-V0.6", "DenseNodes"];
        let mut unsupported: Vec<String> = Vec::new();
        for feature in self.header.get_required_features() {
            if !supported_features.contains(&&feature[..]) {
                unsupported.push(feature.to_owned());
            }
        }
        if unsupported.len() > 0 {
            panic!(
                "PBF file contains unsupported features: {}",
                unsupported.join(", ")
            );
        }
        let mut meta: HashMap<String, String> = HashMap::new();

        let optional_features = self.header.get_optional_features();
        if optional_features.contains(&"LocationsOnWays".to_string()) {
            meta.insert("way_node.location_included".to_string(), "true".to_string());
        } else {
            meta.insert(
                "way_node.location_included".to_string(),
                "false".to_string(),
            );
        }
        meta
    }

    pub fn bound(&self) -> Option<Bound> {
        if self.header.has_bbox() {
            let bbox = self.header.get_bbox();
            Some(Bound {
                left: bbox.get_left(),
                right: bbox.get_right(),
                top: bbox.get_top(),
                bottom: bbox.get_bottom(),
                origin: self.header.get_source().to_owned(),
            })
        } else {
            None
        }
    }
}

pub struct PrimitiveReader {
    block: osmformat::PrimitiveBlock,
    decoder: FieldCodec,
}

impl PrimitiveReader {
    pub fn new(block: osmformat::PrimitiveBlock) -> Self {
        Self {
            decoder: FieldCodec::new_with_block(&block),
            block,
        }
    }

    pub fn get_nodes(&self) -> Vec<Node> {
        let mut nodes: Vec<Node> = Vec::new();
        for group in self.block.get_primitivegroup() {
            if group.has_dense() {
                let mut gdn = self.process_dense(group.get_dense());
                nodes.append(&mut gdn);
            }
            let mut gn = self.process_nodes(group.get_nodes());
            nodes.append(&mut gn);
        }
        nodes
    }

    pub fn get_ways(&self) -> Vec<Way> {
        let mut ways: Vec<Way> = Vec::new();
        for group in self.block.get_primitivegroup() {
            let mut gw = self.process_ways(group.get_ways());
            ways.append(&mut gw);
        }
        ways
    }

    pub fn get_relations(&self) -> Vec<Relation> {
        let mut relations: Vec<Relation> = Vec::new();
        for group in self.block.get_primitivegroup() {
            let mut gr = self.process_relations(group.get_relations());
            relations.append(&mut gr);
        }
        relations
    }

    pub fn get_all_elements(&self) -> (Vec<Node>, Vec<Way>, Vec<Relation>) {
        let mut nodes: Vec<Node> = Vec::new();
        let mut ways: Vec<Way> = Vec::new();
        let mut relations: Vec<Relation> = Vec::new();

        for group in self.block.get_primitivegroup() {
            if group.has_dense() {
                let mut gdn = self.process_dense(group.get_dense());
                nodes.append(&mut gdn);
            }
            let mut gn = self.process_nodes(group.get_nodes());
            nodes.append(&mut gn);

            let mut gw = self.process_ways(group.get_ways());
            ways.append(&mut gw);

            let mut gr = self.process_relations(group.get_relations());
            relations.append(&mut gr);
        }

        (nodes, ways, relations)
    }

    pub fn for_each_element<F: FnMut(ElementContainer)>(&self, mut callback: F) {
        for group in self.block.get_primitivegroup() {
            if group.has_dense() {
                let nodes = self.process_dense(group.get_dense());
                for node in nodes {
                    callback(ElementContainer::Node(node));
                }
            }
            let nodes = self.process_nodes(group.get_nodes());
            for node in nodes {
                callback(ElementContainer::Node(node));
            }

            let ways = self.process_ways(group.get_ways());
            for way in ways {
                callback(ElementContainer::Way(way));
            }

            let relations = self.process_relations(group.get_relations());
            for relation in relations {
                callback(ElementContainer::Relation(relation));
            }
        }
    }

    fn process_dense(&self, dense: &osmformat::DenseNodes) -> Vec<Node> {
        let mut dense_info_iter = DenseInfoIterator::new(dense.get_denseinfo());
        let mut id_iter = dense.get_id().into_iter();
        let mut lat_iter = dense.get_lat().into_iter();
        let mut lon_iter = dense.get_lon().into_iter();

        let mut kv_iter = dense.get_keys_vals().into_iter();

        let mut result = Vec::with_capacity(dense.id.len());
        let mut node_id: i64 = 0;
        let mut latitude: i64 = 0;
        let mut longitude: i64 = 0;
        loop {
            match (
                id_iter.next(),
                lat_iter.next(),
                lon_iter.next(),
                dense_info_iter.next(),
            ) {
                (Some(id), Some(lat), Some(lon), Some(info)) => {
                    node_id += id;
                    latitude += lat;
                    longitude += lon;
                    let mut node = Node {
                        id: node_id,
                        version: info.version,
                        timestamp: Some(self.decoder.decode_timestamp(info.timestamp)),
                        changeset_id: info.changeset,
                        user: Some(OsmUser {
                            id: info.uid,
                            name: self.decoder.decode_string(info.user_sid as usize),
                        }),
                        latitude: self.decoder.decode_latitude(latitude),
                        longitude: self.decoder.decode_longitude(longitude),
                        visible: info.visible,
                        tags: Vec::new(),
                    };

                    loop {
                        let key_index_op = kv_iter.next();
                        let key = match key_index_op {
                            None => break,
                            Some(0) => break,
                            Some(&key_index) => self.decoder.decode_string(key_index as usize),
                        };
                        let value_index_op = kv_iter.next();
                        let value = match value_index_op {
                            None => panic!("The PBF DenseInfo keys/values list contains a key with no corresponding value."),
                            Some(&value_index) => self.decoder.decode_string(value_index as usize)
                        };
                        node.tags.push(Tag { key, value });
                    }

                    result.push(node);
                }
                (None, None, None, None) => break,
                _ => {
                    panic!("dense size error");
                }
            }
        }
        result
    }

    fn build_base_element(&self, id: i64, tags: Vec<Tag>, info: &osmformat::Info) -> BaseElement {
        BaseElement {
            id,
            tags,
            version: info.get_version(),
            timestamp: Some(self.decoder.decode_timestamp(info.get_timestamp())),
            changeset_id: info.get_changeset(),
            user: Some(OsmUser {
                id: info.get_uid(),
                name: self.decoder.decode_string(info.get_user_sid() as usize),
            }),
            visible: true,
        }
    }

    fn process_tags(&self, keys: &[u32], vals: &[u32]) -> Vec<Tag> {
        let mut key_iter = keys.into_iter();
        let mut val_iter = vals.into_iter();
        let mut tags: Vec<Tag> = Vec::new();
        loop {
            match (key_iter.next(), val_iter.next()) {
                (Some(&key_index), Some(&val_index)) => {
                    let key = self.decoder.decode_string(key_index as usize);
                    let value = self.decoder.decode_string(val_index as usize);
                    tags.push(Tag { key, value })
                }
                (None, None) => break,
                _ => panic!("process_nodes key val size error"),
            }
        }
        tags
    }

    fn process_nodes(&self, nodes: &[osmformat::Node]) -> Vec<Node> {
        nodes
            .into_iter()
            .map(|elm| {
                let tags = self.process_tags(elm.get_keys(), elm.get_vals());
                let base_el = if elm.has_info() {
                    let info = elm.get_info();
                    self.build_base_element(elm.get_id(), tags, info)
                } else {
                    BaseElement::new_with_tags(elm.get_id(), tags)
                };
                let mut node: Node = base_el.into();
                node.latitude = self.decoder.decode_latitude(elm.get_lat());
                node.longitude = self.decoder.decode_longitude(elm.get_lon());
                node
            })
            .collect()
    }

    fn process_ways(&self, ways: &[osmformat::Way]) -> Vec<Way> {
        ways.into_iter()
            .map(|elm| {
                let tags = self.process_tags(elm.get_keys(), elm.get_vals());
                let base_el = if elm.has_info() {
                    let info = elm.get_info();
                    self.build_base_element(elm.get_id(), tags, info)
                } else {
                    BaseElement::new_with_tags(elm.get_id(), tags)
                };
                let mut way: Way = base_el.into();

                let mut node_id: i64 = 0;
                let mut lat: i64 = 0;
                let mut lon: i64 = 0;
                let mut ref_iter = elm.get_refs().into_iter();
                let mut lat_iter = elm.get_lat().into_iter();
                let mut lon_iter = elm.get_lon().into_iter();
                loop {
                    match (ref_iter.next(), lat_iter.next(), lon_iter.next()) {
                        (Some(&ref_delta), Some(&lat_delta), Some(&lon_delta)) => {
                            node_id += ref_delta;
                            lat += lat_delta;
                            lon += lon_delta;
                            way.way_nodes.push(WayNode::new(
                                node_id,
                                self.decoder.decode_latitude(lat),
                                self.decoder.decode_longitude(lon),
                            ));
                        }
                        (Some(&ref_delta), None, None) => {
                            node_id += ref_delta;
                            way.way_nodes.push(WayNode::new_without_coords(node_id));
                        }
                        (None, None, None) => break,
                        _ => panic!("process_ways refs size error"),
                    }
                }

                way
            })
            .collect()
    }

    fn process_relations(&self, relations: &[osmformat::Relation]) -> Vec<Relation> {
        relations
            .into_iter()
            .map(|elm| {
                let tags = self.process_tags(elm.get_keys(), elm.get_vals());
                let base_el = if elm.has_info() {
                    let info = elm.get_info();
                    self.build_base_element(elm.get_id(), tags, info)
                } else {
                    BaseElement::new_with_tags(elm.get_id(), tags)
                };
                let mut relation: Relation = base_el.into();
                relation.members = self.build_relation_members(
                    elm.get_memids(),
                    elm.get_types(),
                    elm.get_roles_sid(),
                );
                relation
            })
            .collect()
    }

    fn build_relation_members(
        &self,
        member_ids: &[i64],
        member_types: &[Relation_MemberType],
        member_roles: &[i32],
    ) -> Vec<RelationMember> {
        let mut mid_iter = member_ids.into_iter();
        let mut role_iter = member_roles.into_iter();
        let mut type_iter = member_types.into_iter();

        let mut result: Vec<RelationMember> = Vec::new();
        let mut member_id: i64 = 0;
        loop {
            match (mid_iter.next(), role_iter.next(), type_iter.next()) {
                (Some(mid), Some(&role), Some(mem_type)) => {
                    member_id += mid;
                    let member_type = match mem_type {
                        Relation_MemberType::NODE => ElementType::Node,
                        Relation_MemberType::WAY => ElementType::Way,
                        Relation_MemberType::RELATION => ElementType::Relation,
                    };
                    let member = RelationMember {
                        member_id,
                        member_type,
                        role: self.decoder.decode_string(role as usize),
                    };
                    result.push(member);
                }
                (None, None, None) => break,
                _ => panic!("build_relation_members size error"),
            }
        }
        result
    }
}

pub struct DenseInfoItem {
    version: i32,
    timestamp: i64,
    changeset: i64,
    uid: i32,
    user_sid: i32,
    visible: bool,
}

pub struct DenseInfoIterator<'a> {
    version_iter: std::slice::Iter<'a, i32>,
    timestamp_iter: std::slice::Iter<'a, i64>,
    changeset_iter: std::slice::Iter<'a, i64>,
    uid_iter: std::slice::Iter<'a, i32>,
    user_sid_iter: std::slice::Iter<'a, i32>,
    visible_iter: std::slice::Iter<'a, bool>,
    timestamp: i64,
    changeset: i64,
    uid: i32,
    user_sid: i32,
}

impl<'a> DenseInfoIterator<'a> {
    fn new(info: &'a osmformat::DenseInfo) -> DenseInfoIterator<'a> {
        DenseInfoIterator {
            version_iter: info.get_version().iter(),
            timestamp_iter: info.get_timestamp().iter(),
            changeset_iter: info.get_changeset().iter(),
            uid_iter: info.get_uid().iter(),
            user_sid_iter: info.get_user_sid().iter(),
            visible_iter: info.get_visible().iter(),
            timestamp: 0,
            changeset: 0,
            uid: 0,
            user_sid: 0,
        }
    }
}

impl<'a> Iterator for DenseInfoIterator<'a> {
    type Item = DenseInfoItem;

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.version_iter.next(),
            self.timestamp_iter.next(),
            self.changeset_iter.next(),
            self.uid_iter.next(),
            self.user_sid_iter.next(),
            self.visible_iter.next(),
        ) {
            (
                Some(&version),
                Some(d_timestamp),
                Some(d_changeset),
                Some(d_uid),
                Some(d_user_sid),
                visible,
            ) => {
                self.timestamp += *d_timestamp;
                self.changeset += *d_changeset;
                self.uid += *d_uid;
                self.user_sid += *d_user_sid;
                Some(DenseInfoItem {
                    version,
                    timestamp: self.timestamp,
                    changeset: self.changeset,
                    uid: self.uid,
                    user_sid: self.user_sid,
                    visible: *visible.unwrap_or(&true),
                })
            }
            _ => None,
        }
    }
}
