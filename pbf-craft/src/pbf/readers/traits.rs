use std::rc::Rc;

use crate::models::{Node, Relation, Way};

pub struct BlobData {
    pub nodes: Vec<Node>,
    pub ways: Vec<Way>,
    pub relations: Vec<Relation>,
    pub offset: u64,
}

pub trait PbfRandomRead {
    fn read_blob_by_offset(&mut self, offset: u64) -> anyhow::Result<Rc<BlobData>>;
}
