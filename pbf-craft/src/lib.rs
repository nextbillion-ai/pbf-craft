//! This crate provides functionality for reading, writing, and processing OSM PBF data.
//!
//! It contains a variety of PBF readers for a variety of scenarios. For example, a PBF
//! reader with an index can locate and read elements more efficiently. It also provides
//! a PBF writer that can write PBF data to a file.
//!
//! Since this crate uses the btree_cursors feature, it requires you to use the **nightly**
//! version of rust.
//!
//! # Example
//!
//! Read PBF data from a file:
//!
//! ```rust
//! use pbf_craft::readers::PbfReader;
//!
//! let mut reader = PbfReader::from_path("resources/andorra-latest.osm.pbf").unwrap();
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
//! use pbf_craft::readers::IndexedReader;
//!
//! let mut indexed_reader =
//!     IndexedReader::from_path_with_cache("resources/andorra-latest.osm.pbf", 1000).unwrap();
//! let element_list = indexed_reader.get_with_deps(&ElementType::Way, 12345678).unwrap();
//! ```
//!
//! Write PBF data to a file:
//!
//! ```rust
//! use pbf_craft::models::{Element, Node};
//! use pbf_craft::writers::PbfWriter;
//!
//! let mut writer = PbfWriter::from_path("resources/output.osm.pbf", true).unwrap();
//! writer.write(Element::Node(Node::default())).unwrap();
//! writer.finish().unwrap();
//! ```
//!

#![feature(btree_cursors)]

mod codecs;
/// Contains models for elements of OpenStreetMap data.
pub mod models;
/// Contains readers for reading PBF data.
pub mod readers;
mod utils;
/// Contains writers for writing PBF data.
pub mod writers;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}

#[macro_use]
extern crate anyhow;
