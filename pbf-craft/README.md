# pbf-craft

A Rust library for reading and writing OpenStreetMap PBF file format.

It contains a variety of PBF readers for a variety of scenarios. For example, a PBF
reader with an index can locate and read elements more efficiently. It also provides
a PBF writer that can write PBF data to a file.

Since this crate uses the btree_cursors feature, it requires you to use the **nightly**
version of rust.

- Written in pure Rust
- Provides an indexing feature to the PBF to greatly improve read performance.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
pbf-craft = "0.9"
```

Reading a PBF file:

```rust
use pbf_craft::readers::PbfReader;

let mut reader = PbfReader::from_path("resources/andorra-latest.osm.pbf").unwrap();
reader.read(|header, element| {
    if let Some(header_reader) = header {
        // Process header
    }
    if let Some(element) = element {
        // Process element
    }
}).unwrap();
```

Finding an element using the index feature. `IndexedReader` creates an index file for the PBF file, which allows you to quickly locate and retrieve an element when looking for it using its ID. `IndexedReader` has an cache option, with which you can fetch a element with its dependencies more efficiently.

```rust
use pbf_craft::models::ElementType;
use pbf_craft::readers::IndexedReader;

let mut indexed_reader = IndexedReader::from_path_with_cache("resources/andorra-latest.osm.pbf", 1000).unwrap();
let node = indexed_reader.find(&ElementType::Node, 12345678).unwrap();
let element_list = indexed_reader.get_with_deps(&ElementType::Way, 1055523837).unwrap();
```

Writing a PBF file:

```rust
use pbf_craft::models::{Element, Node};
use pbf_craft::writers::PbfWriter;

let mut writer = PbfWriter::from_path("resources/output.osm.pbf", true).unwrap();
writer.write(Element::Node(Node::default())).unwrap();
writer.finish().unwrap();
```
