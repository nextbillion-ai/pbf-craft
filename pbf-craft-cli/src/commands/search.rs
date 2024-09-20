use std::str::FromStr;

use clap::Args;
use colored_json::prelude::*;

use pbf_craft::models::{Element, ElementType, Tag};
use pbf_craft::pbf::readers::{IndexedReader, PbfReader};

#[derive(Args, Debug)]
pub struct SearchCommand {
    /// element type: node, way, relation
    #[clap(long, value_parser)]
    eltype: Option<String>,

    /// element id
    #[clap(long, value_parser)]
    elid: Option<i64>,

    /// tag key
    #[clap(long, value_parser)]
    tagkey: Option<String>,

    /// tag value
    #[clap(long, value_parser)]
    tagvalue: Option<String>,

    #[clap(long, value_parser)]
    pair: Option<Vec<i64>>,

    /// file path
    #[clap(short, long, value_parser)]
    file: String,

    /// The default value is true. If true, it will match exactly the only element. If false, all associated elements will be matched.
    #[clap(short, long, value_parser)]
    exact: Option<bool>,
}

impl SearchCommand {
    pub fn run(self) {
        let result = if let (Some(eltype), Some(elid)) = (&self.eltype, &self.elid) {
            blue!("Searching ");
            dark_yellow!("{} ", &self.file);
            blue!("for ");
            dark_yellow!("{}#{} ", eltype, elid);
            println!("...");

            let element_type_result = ElementType::from_str(eltype);
            if let Err(err) = element_type_result {
                eprintln!("{}", err);
                return;
            }
            let element_type = element_type_result.unwrap();

            if self.exact.is_none() || self.exact.unwrap() == true {
                let mut indexed_reader =
                    IndexedReader::from_path(&self.file).expect("Indexed reader loading failed");
                let find_result = indexed_reader.find(&element_type, *elid).unwrap();
                match find_result {
                    Some(ec) => {
                        let mut list = Vec::new();
                        list.push(ec);
                        list
                    }
                    None => Vec::with_capacity(0),
                }
            } else {
                let reader = PbfReader::from_path(&self.file).unwrap();
                reader
                    .par_find(None, |element| match (element, &element_type) {
                        (Element::Node(node), ElementType::Node) => node.id == *elid,
                        (Element::Way(way), ElementType::Node) => {
                            for way_node in &way.way_nodes {
                                if way_node.id == *elid {
                                    return true;
                                }
                            }
                            return false;
                        }
                        (Element::Way(way), ElementType::Way) => way.id == *elid,
                        (Element::Relation(relation), ElementType::Relation) => {
                            relation.id == *elid
                        }
                        (Element::Relation(relation), _) => {
                            for member in &relation.members {
                                if member.member_id == *elid && member.member_type.eq(&element_type)
                                {
                                    return true;
                                }
                            }
                            return false;
                        }
                        _ => false,
                    })
                    .expect("read pbf failed")
            }
        } else if self.tagkey.is_some() || self.tagvalue.is_some() {
            blue!("Searching ");
            dark_yellow!("{} ", &self.file);
            blue!("for ");
            dark_yellow!(
                "elements of tag key {:?} and tag value {:?} ",
                &self.tagkey,
                &self.tagvalue
            );
            println!("...");
            let reader = PbfReader::from_path(&self.file).unwrap();
            reader
                .par_find(None, |element| match element {
                    Element::Node(node) => does_tag_match(&node.tags, &self.tagkey, &self.tagvalue),
                    Element::Way(way) => does_tag_match(&way.tags, &self.tagkey, &self.tagvalue),
                    Element::Relation(relation) => {
                        does_tag_match(&relation.tags, &self.tagkey, &self.tagvalue)
                    }
                })
                .expect("read pbf failed")
        } else if self.pair.is_some() {
            let node_ids = self.pair.unwrap();
            if node_ids.len() < 2 {
                panic!("At least two nodes are required");
            }
            let first = node_ids[0];
            let second = node_ids[1];
            blue!("Searching ");
            dark_yellow!("{} ", &self.file);
            blue!("for ");
            dark_yellow!("ways containing the node pair of {} and {} ", first, second);
            println!("...");
            let reader = PbfReader::from_path(&self.file).unwrap();
            reader
                .par_find(Some(&ElementType::Way), |el| {
                    if let Element::Way(way) = el {
                        return way.way_nodes.iter().any(|ref_node| ref_node.id == first)
                            && way.way_nodes.iter().any(|ref_node| ref_node.id == second);
                    }
                    return false;
                })
                .expect("node pair error")
        } else {
            yellow!("Your input is incorrect");
            Vec::with_capacity(0)
        };

        println!(
            "{}",
            serde_json::to_string_pretty(&result)
                .unwrap()
                .to_colored_json_auto()
                .unwrap()
        );
        println!("{} elemets found", result.len());
    }
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
