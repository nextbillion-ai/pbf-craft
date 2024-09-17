use std::str::FromStr;

use clap::Args;
use colored_json::prelude::*;
use serde_json;

use pbf_craft::models::{ElementContainer, ElementType};
use pbf_craft::pbf::readers::IndexedReader;

#[derive(Args, Debug)]
pub struct GetCommand {
    /// element type: node, way, relation
    #[clap(long, value_parser)]
    eltype: String,

    /// element id
    #[clap(long, value_parser)]
    elid: i64,

    /// file path
    #[clap(short, long, value_parser)]
    file: String,

    /// cache size
    #[clap(short, long, value_parser, default_value_t = 1000)]
    cache_size: usize,
}

impl GetCommand {
    pub fn run(self) {
        let mut indexed_reader = IndexedReader::from_path_with_cache(&self.file, self.cache_size)
            .expect("Indexed reader loading failed");

        let element_type_result = ElementType::from_str(self.eltype.as_str());
        if let Err(err) = element_type_result {
            eprintln!("{}", err);
            return;
        }
        let element_type = element_type_result.unwrap();

        blue!("Searching ");
        dark_yellow!("{} ", &self.file);
        blue!("for ");
        dark_yellow!("{}#{} ", self.eltype, self.elid);
        blue!("with dependencies");
        println!("...");

        let result: Vec<ElementContainer> = indexed_reader
            .get_with_deps(&element_type, self.elid)
            .unwrap();

        println!(
            "{}",
            serde_json::to_string_pretty(&result)
                .unwrap()
                .to_colored_json_auto()
                .unwrap()
        );
    }
}
