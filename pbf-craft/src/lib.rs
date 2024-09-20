//! This crate provides functionality for reading, writing, and processing OSM PBF data.
//!
//! It contains a variety of PBF readers for a variety of scenarios. For example, a PBF
//! reader with an index can locate and read elements more efficiently. It also provides
//! a PBF writer that can write PBF data to a file.
//!
//! Since this crate uses the btree_cursors feature, it requires you to use the **nightly**
//! version of rust.
//!
//! # Modules
//!
//! * `models` - Contains data structures used in PBF processing.
//! * `pbf` - Provides functionality for reading and writing PBF data.
//!
//! # Features
//!
//! * `btree_cursors` - Enables the use of BTree cursors for efficient data access.
//!
//! # Example
//!
//! Read PBF data from a file:
//!
//! ```rust
//! use pbf::readers::raw_reader::PbfReader;
//!
//! let reader = PbfReader::from_path("path/to/osm.pbf").unwrap();
//! reader.read(|header, element| {
//!     if let Some(header_reader) = header {
//!         // Process header
//!     }
//!     if let Some(element) = element {
//!         // Process element
//!     }
//! }).unwrap();
//! ```
//!
//! Read PBF data with dependencies:
//!
//! ```rust
//! use pbf_craft::models::ElementType;
//! use pbf_craft::pbf::readers::IndexedReader;
//!
//! let mut indexed_reader = IndexedReader::from_path_with_cache("path/to/osm.pbf", 1000).unwarp();
//! let element_list = indexed_reader.get_with_deps(&ElementType::Way, 12345678).unwrap();
//! ```
//!
//! Write PBF data to a file:
//!
//! ```rust
//! use pbf_craft::models::{Element, Node};
//! use pbf_craft::pbf::writers::PbfWriter;
//!
//! let mut writer = PbfWriter::from_path("path/to/osm.pbf", true).unwrap();
//! writer.write(Element::Node(Node::default())).unwrap();
//! writer.finish().unwarp();
//! ```
//!

#![feature(btree_cursors)]

pub mod models;
pub mod pbf;
mod utils;

#[macro_use]
extern crate anyhow;
