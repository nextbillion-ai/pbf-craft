# pbf-craft

A Rust library and command-line tool for reading and writing OpenSteetMap PBF file format.

- Written in pure Rust
- Provides an indexing feature to the PBF to greatly improve read performance.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
pbf-craft = "0.1"
```

Reading a PBF file:

```rust
use pbf_craft::models::{Element, ElementType};
use pbf_craft::pbf::readers::PbfReader;

let reader = PbfReader::from_path("./tokyo.pbf").unwrap();
reader
    .par_find(Some(&ElementType::Way), |el| {
        if let Element::Way(way) = el {
            return way.way_nodes.iter().any(|ref_node| ref_node.id == first)
                && way.way_nodes.iter().any(|ref_node| ref_node.id == second);
        }
        return false;
    })
    .expect("node pair error")
```

Finding an element using the index feature. `IndexedReader` creates an index file for the PBF file, which allows you to quickly locate and retrieve an element when looking for it using its ID.

```rust
use pbf_craft::models::{Element, ElementType};
use pbf_craft::pbf::readers::IndexedReader;

let mut indexed_reader = IndexedReader::from_path("./tokyo.pbf").unwarp();
let node = indexed_reader.find(&ElementType::Node, 12345678).unwrap();
```
