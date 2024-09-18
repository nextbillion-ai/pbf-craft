use std::fs::File;

use clap::Args;
use csv;
use serde::{Deserialize, Serialize};

use pbf_craft::models::{Element, ElementType};
use pbf_craft::pbf::readers::IterableReader;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiffType {
    Add,
    Modify,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElementDiff {
    pub element_type: ElementType,
    pub element_id: i64,
    pub diff_type: DiffType,
}

#[derive(Args)]
pub struct DiffCommand {
    /// source pbf path
    #[clap(short, long, value_parser)]
    source: String,

    /// target pbf path
    #[clap(short, long, value_parser)]
    target: String,

    /// output path
    #[clap(short, long, value_parser, default_value = "./diff.csv")]
    output: String,
}

impl DiffCommand {
    pub fn run(self) {
        let mut diff_csv =
            csv::WriterBuilder::new().from_writer(File::create(&self.output).unwrap());

        let mut source = IterableReader::from_path(&self.source)
            .expect(&format!("No such file: {}", self.source))
            .into_iter();
        let mut target = IterableReader::from_path(&self.target)
            .expect(&format!("No such file: {}", self.target))
            .into_iter();

        let mut source_element_cnt = source.next();
        let mut target_element_cnt = target.next();

        loop {
            match (&source_element_cnt, &target_element_cnt) {
                (Some(source_element), Some(target_element)) => {
                    match (source_element, target_element) {
                        (
                            Element::Node(source_element),
                            Element::Node(target_element),
                        ) => {
                            if source_element.id == target_element.id {
                                if source_element != target_element {
                                    diff_csv
                                        .serialize(ElementDiff {
                                            element_type: ElementType::Node,
                                            element_id: source_element.id,
                                            diff_type: DiffType::Modify,
                                        })
                                        .unwrap();
                                }
                                source_element_cnt = source.next();
                                target_element_cnt = target.next();
                            } else if source_element.id < target_element.id {
                                diff_csv
                                    .serialize(ElementDiff {
                                        element_type: ElementType::Node,
                                        element_id: source_element.id,
                                        diff_type: DiffType::Delete,
                                    })
                                    .unwrap();
                                source_element_cnt = source.next();
                            } else {
                                diff_csv
                                    .serialize(ElementDiff {
                                        element_type: ElementType::Node,
                                        element_id: target_element.id,
                                        diff_type: DiffType::Add,
                                    })
                                    .unwrap();
                                target_element_cnt = target.next();
                            }
                        }
                        (Element::Node(source_element), Element::Way(_)) => {
                            diff_csv
                                .serialize(ElementDiff {
                                    element_type: ElementType::Node,
                                    element_id: source_element.id,
                                    diff_type: DiffType::Delete,
                                })
                                .unwrap();
                            source_element_cnt = source.next();
                        }
                        (Element::Way(_), Element::Node(target_element)) => {
                            diff_csv
                                .serialize(ElementDiff {
                                    element_type: ElementType::Node,
                                    element_id: target_element.id,
                                    diff_type: DiffType::Add,
                                })
                                .unwrap();
                            target_element_cnt = target.next();
                        }
                        (
                            Element::Way(source_element),
                            Element::Way(target_element),
                        ) => {
                            if source_element.id == target_element.id {
                                if source_element != target_element {
                                    diff_csv
                                        .serialize(ElementDiff {
                                            element_type: ElementType::Way,
                                            element_id: source_element.id,
                                            diff_type: DiffType::Modify,
                                        })
                                        .unwrap();
                                }
                                source_element_cnt = source.next();
                                target_element_cnt = target.next();
                            } else if source_element.id < target_element.id {
                                diff_csv
                                    .serialize(ElementDiff {
                                        element_type: ElementType::Way,
                                        element_id: source_element.id,
                                        diff_type: DiffType::Delete,
                                    })
                                    .unwrap();
                                source_element_cnt = source.next();
                            } else {
                                diff_csv
                                    .serialize(ElementDiff {
                                        element_type: ElementType::Way,
                                        element_id: target_element.id,
                                        diff_type: DiffType::Add,
                                    })
                                    .unwrap();
                                target_element_cnt = target.next();
                            }
                        }
                        (Element::Way(source_way), Element::Relation(_)) => {
                            diff_csv
                                .serialize(ElementDiff {
                                    element_type: ElementType::Way,
                                    element_id: source_way.id,
                                    diff_type: DiffType::Delete,
                                })
                                .unwrap();
                            source_element_cnt = source.next();
                        }
                        (Element::Relation(_), Element::Way(target_way)) => {
                            diff_csv
                                .serialize(ElementDiff {
                                    element_type: ElementType::Way,
                                    element_id: target_way.id,
                                    diff_type: DiffType::Add,
                                })
                                .unwrap();
                            target_element_cnt = target.next();
                        }
                        (
                            Element::Relation(source_element),
                            Element::Relation(target_element),
                        ) => {
                            if source_element.id == target_element.id {
                                if source_element != target_element {
                                    diff_csv
                                        .serialize(ElementDiff {
                                            element_type: ElementType::Relation,
                                            element_id: source_element.id,
                                            diff_type: DiffType::Modify,
                                        })
                                        .unwrap();
                                }
                                source_element_cnt = source.next();
                                target_element_cnt = target.next();
                            } else if source_element.id < target_element.id {
                                diff_csv
                                    .serialize(ElementDiff {
                                        element_type: ElementType::Relation,
                                        element_id: source_element.id,
                                        diff_type: DiffType::Delete,
                                    })
                                    .unwrap();
                                source_element_cnt = source.next();
                            } else {
                                diff_csv
                                    .serialize(ElementDiff {
                                        element_type: ElementType::Relation,
                                        element_id: target_element.id,
                                        diff_type: DiffType::Add,
                                    })
                                    .unwrap();
                                target_element_cnt = target.next();
                            }
                        }
                        (Element::Relation(_), Element::Node(target_node)) => {
                            diff_csv
                                .serialize(ElementDiff {
                                    element_type: ElementType::Node,
                                    element_id: target_node.id,
                                    diff_type: DiffType::Add,
                                })
                                .unwrap();
                            target_element_cnt = target.next();
                        }
                        (Element::Node(source_node), Element::Relation(_)) => {
                            diff_csv
                                .serialize(ElementDiff {
                                    element_type: ElementType::Node,
                                    element_id: source_node.id,
                                    diff_type: DiffType::Delete,
                                })
                                .unwrap();
                            source_element_cnt = source.next();
                        }
                    }
                }
                (Some(source_element), None) => {
                    let (element_type, element_id) = source_element.get_meta();
                    diff_csv
                        .serialize(ElementDiff {
                            element_type,
                            element_id,
                            diff_type: DiffType::Delete,
                        })
                        .unwrap();
                }
                (None, Some(target_element)) => {
                    let (element_type, element_id) = target_element.get_meta();
                    diff_csv
                        .serialize(ElementDiff {
                            element_type,
                            element_id,
                            diff_type: DiffType::Add,
                        })
                        .unwrap();
                }
                (None, None) => break,
            }
        }

        diff_csv.flush().unwrap();
        println!("Diff file created: ./{}", &self.output);
    }
}
