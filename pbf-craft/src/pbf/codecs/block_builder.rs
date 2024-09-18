use std::collections::HashMap;

use protobuf::RepeatedField;

use super::field::FieldCodec;
use crate::models::{Element, ElementType, Node, Relation, Tag, Way};
use crate::pbf::proto::osmformat;

struct StringTableBuilder {
    strings: Vec<String>,
    id_map: HashMap<String, usize>,
}

impl StringTableBuilder {
    pub fn new() -> Self {
        Self {
            strings: Vec::new(),
            id_map: HashMap::new(),
        }
    }
    pub fn add(&mut self, string: String) -> i32 {
        if self.id_map.contains_key(&string) {
            return (*self.id_map.get(&string).unwrap()) as i32;
        }
        self.strings.push(string.clone());
        let id = self.strings.len() - 1;
        self.id_map.insert(string, id);
        id as i32
    }

    pub fn to_string_table(self) -> osmformat::StringTable {
        let string_bytes: Vec<Vec<u8>> = self
            .strings
            .into_iter()
            .map(|string| string.as_bytes().to_vec())
            .collect();
        let mut string_table = osmformat::StringTable::new();
        string_table.set_s(RepeatedField::from_vec(string_bytes));
        string_table
    }
}

pub struct PrimitiveBuilder {
    block: osmformat::PrimitiveBlock,
    codec: FieldCodec,
    string_table: StringTableBuilder,
}

impl PrimitiveBuilder {
    pub fn new() -> Self {
        let block = osmformat::PrimitiveBlock::new();
        Self {
            codec: FieldCodec::new(block.get_granularity(), block.get_date_granularity()),
            block,
            string_table: StringTableBuilder::new(),
        }
    }

    fn encode_dense_nodes(&mut self, nodes: Vec<Node>) -> osmformat::DenseNodes {
        let mut dense_info = osmformat::DenseInfo::new();
        let mut dense = osmformat::DenseNodes::new();

        let mut previous_id = 0;
        let mut previous_lat = self.codec.encode_latitude(0);
        let mut previous_lon = self.codec.encode_latitude(0);
        let mut previous_changeset = 0;
        let mut previous_timestamp = 0;
        let mut previous_uid = 0;
        let mut previous_sid = 0;

        for node in nodes {
            dense.id.push(node.id - previous_id);

            let lat = self.codec.encode_latitude(node.latitude);
            let lon = self.codec.encode_longitude(node.longitude);
            dense.lat.push(lat - previous_lat);
            dense.lon.push(lon - previous_lon);

            dense_info
                .changeset
                .push(node.changeset_id - previous_changeset);
            dense_info.version.push(node.version);
            dense_info.visible.push(true);

            previous_timestamp = if let Some(timestamp) = node.timestamp {
                let tt = self.codec.encode_timestamp(timestamp);
                dense_info.timestamp.push(tt - previous_timestamp);
                tt
            } else {
                let tt = 0i64;
                dense_info.timestamp.push(tt - previous_timestamp);
                tt
            };

            (previous_uid, previous_sid) = if let Some(user) = node.user {
                dense_info.uid.push(user.id - previous_uid);
                let user_sid = self.string_table.add(user.name);
                dense_info.user_sid.push(user_sid - previous_sid);
                (user.id, user_sid)
            } else {
                dense_info.uid.push(0 - previous_uid);
                let user_sid = self.string_table.add("".to_string());
                dense_info.user_sid.push(user_sid - previous_sid);
                (0, user_sid)
            };

            for tag in node.tags {
                dense.keys_vals.push(self.string_table.add(tag.key));
                dense.keys_vals.push(self.string_table.add(tag.value));
            }
            dense.keys_vals.push(0);

            previous_id = node.id;
            previous_lat = lat;
            previous_lon = lon;
            previous_changeset = node.changeset_id;
        }
        dense.set_denseinfo(dense_info);
        dense
    }

    fn encode_tags(&mut self, tags: Vec<Tag>) -> (Vec<u32>, Vec<u32>) {
        let mut keys: Vec<u32> = Vec::new();
        let mut vals: Vec<u32> = Vec::new();
        for tag in tags {
            keys.push(self.string_table.add(tag.key) as u32);
            vals.push(self.string_table.add(tag.value) as u32);
        }
        (keys, vals)
    }

    fn encode_nodes(&mut self, nodes: Vec<Node>) -> Vec<osmformat::Node> {
        nodes
            .into_iter()
            .map(|node| -> osmformat::Node {
                let mut osm_node = osmformat::Node::new();
                osm_node.set_id(node.id);
                osm_node.set_lat(self.codec.encode_latitude(node.latitude));
                osm_node.set_lon(self.codec.encode_longitude(node.longitude));

                let (keys, vals) = self.encode_tags(node.tags);
                osm_node.set_keys(keys);
                osm_node.set_vals(vals);

                let mut info = osmformat::Info::new();
                info.set_changeset(node.changeset_id);
                info.set_version(node.version);
                info.set_visible(node.visible);
                if let Some(timestamp) = node.timestamp {
                    info.set_timestamp(self.codec.encode_timestamp(timestamp));
                } else {
                    info.set_timestamp(0);
                }
                if let Some(user) = node.user {
                    info.set_uid(user.id);
                    let sid = self.string_table.add(user.name);
                    info.set_user_sid(sid as u32);
                } else {
                    info.set_uid(0);
                    let sid = self.string_table.add("".to_string());
                    info.set_user_sid(sid as u32);
                }

                osm_node
            })
            .collect()
    }

    fn add_nodes(&mut self, nodes: Vec<Node>, use_dense: bool) {
        let mut group = osmformat::PrimitiveGroup::new();
        if use_dense {
            let dense = self.encode_dense_nodes(nodes);
            group.set_dense(dense);
        } else {
            let encoded_nodes = self.encode_nodes(nodes);
            group.set_nodes(RepeatedField::from_vec(encoded_nodes))
        }
        self.block.primitivegroup.push(group);
    }

    fn add_ways(&mut self, ways: Vec<Way>) {
        let encoded_ways: Vec<osmformat::Way> = ways
            .into_iter()
            .map(|way| {
                let mut osm_way = osmformat::Way::new();
                osm_way.set_id(way.id);

                let mut prev_ref_id = 0;
                osm_way.set_refs(
                    way.way_nodes
                        .into_iter()
                        .map(|way_node| {
                            let difference = way_node.id - prev_ref_id;
                            prev_ref_id = way_node.id;
                            difference
                        })
                        .collect(),
                );

                let (keys, vals) = self.encode_tags(way.tags);
                osm_way.set_keys(keys);
                osm_way.set_vals(vals);

                let mut info = osmformat::Info::new();
                info.set_changeset(way.changeset_id);
                info.set_version(way.version);
                info.set_visible(way.visible);
                if let Some(timestamp) = way.timestamp {
                    info.set_timestamp(self.codec.encode_timestamp(timestamp));
                } else {
                    info.set_timestamp(0);
                }
                if let Some(user) = way.user {
                    info.set_uid(user.id);
                    let sid = self.string_table.add(user.name);
                    info.set_user_sid(sid as u32);
                } else {
                    info.set_uid(0);
                    let sid = self.string_table.add("".to_string());
                    info.set_user_sid(sid as u32);
                }
                osm_way.set_info(info);

                osm_way
            })
            .collect();

        let mut group = osmformat::PrimitiveGroup::new();
        group.set_ways(RepeatedField::from_vec(encoded_ways));
        self.block.primitivegroup.push(group);
    }

    fn add_relations(&mut self, relations: Vec<Relation>) {
        let encoded_relations: Vec<osmformat::Relation> = relations
            .into_iter()
            .map(|relation| {
                let mut osm_relation = osmformat::Relation::new();
                osm_relation.set_id(relation.id);

                let mut prev_member_id = 0i64;
                for member in relation.members {
                    osm_relation.memids.push(member.member_id - prev_member_id);
                    prev_member_id = member.member_id;

                    osm_relation
                        .roles_sid
                        .push(self.string_table.add(member.role));
                    let osm_member_type = match member.member_type {
                        ElementType::Node => osmformat::Relation_MemberType::NODE,
                        ElementType::Way => osmformat::Relation_MemberType::WAY,
                        ElementType::Relation => osmformat::Relation_MemberType::RELATION,
                    };
                    osm_relation.types.push(osm_member_type);
                }

                let (keys, vals) = self.encode_tags(relation.tags);
                osm_relation.set_keys(keys);
                osm_relation.set_vals(vals);

                let mut info = osmformat::Info::new();
                info.set_changeset(relation.changeset_id);
                info.set_version(relation.version);
                info.set_visible(relation.visible);
                if let Some(timestamp) = relation.timestamp {
                    info.set_timestamp(self.codec.encode_timestamp(timestamp));
                } else {
                    info.set_timestamp(0);
                }
                if let Some(user) = relation.user {
                    info.set_uid(user.id);
                    let sid = self.string_table.add(user.name);
                    info.set_user_sid(sid as u32);
                } else {
                    info.set_uid(0);
                    let sid = self.string_table.add("".to_string());
                    info.set_user_sid(sid as u32);
                }
                osm_relation.set_info(info);

                osm_relation
            })
            .collect();

        let mut group = osmformat::PrimitiveGroup::new();
        group.set_relations(RepeatedField::from_vec(encoded_relations));
        self.block.primitivegroup.push(group);
    }

    pub fn build(
        mut self,
        elements: Vec<Element>,
        use_dense: bool,
    ) -> osmformat::PrimitiveBlock {
        let mut nodes = Vec::new();
        let mut ways = Vec::new();
        let mut relations = Vec::new();
        for element in elements {
            match element {
                Element::Node(node) => nodes.push(node),
                Element::Way(way) => ways.push(way),
                Element::Relation(relation) => relations.push(relation),
            }
        }
        if nodes.len() > 0 {
            self.add_nodes(nodes, use_dense);
        }
        if ways.len() > 0 {
            self.add_ways(ways);
        }
        if relations.len() > 0 {
            self.add_relations(relations);
        }

        self.block
            .set_stringtable(self.string_table.to_string_table());
        self.block
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build() {
        let builder = PrimitiveBuilder::new();
        println!(
            "{}, {}",
            builder.block.get_granularity(),
            builder.block.get_date_granularity()
        );
        assert!(true);
    }
}
