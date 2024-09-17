use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bound {
    pub left: i64,
    pub right: i64,
    pub top: i64,
    pub bottom: i64,
    pub origin: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OsmUser {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ElementContainer {
    Node(Node),
    Way(Way),
    Relation(Relation),
}

impl ElementContainer {
    pub fn get_meta(&self) -> (ElementType, i64) {
        match self {
            ElementContainer::Node(e) => (ElementType::Node, e.id),
            ElementContainer::Way(e) => (ElementType::Way, e.id),
            ElementContainer::Relation(e) => (ElementType::Relation, e.id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ElementType {
    Node,
    Way,
    Relation,
}

impl FromStr for ElementType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "node" => Ok(ElementType::Node),
            "way" => Ok(ElementType::Way),
            "relation" => Ok(ElementType::Relation),
            _ => Err(anyhow!("Illegal element_type: {}", s)),
        }
    }
}

#[derive(Debug, Default)]
pub struct BaseElement {
    pub id: i64,
    pub version: i32,
    pub timestamp: Option<DateTime<Utc>>,
    pub user: Option<OsmUser>,
    pub changeset_id: i64,
    pub visible: bool,
    pub tags: Vec<Tag>,
}

impl BaseElement {
    pub fn new_with_tags(id: i64, tags: Vec<Tag>) -> Self {
        Self {
            id,
            tags,
            visible: true,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct Node {
    pub id: i64,
    pub version: i32,
    pub timestamp: Option<DateTime<Utc>>,
    pub user: Option<OsmUser>,
    pub changeset_id: i64,
    pub latitude: i64,
    pub longitude: i64,
    pub visible: bool,
    pub tags: Vec<Tag>,
}

impl From<BaseElement> for Node {
    fn from(el: BaseElement) -> Self {
        Self {
            id: el.id,
            version: el.version,
            timestamp: el.timestamp,
            user: el.user,
            changeset_id: el.changeset_id,
            visible: el.visible,
            tags: el.tags,
            latitude: 0,
            longitude: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct Way {
    pub id: i64,
    pub version: i32,
    pub timestamp: Option<DateTime<Utc>>,
    pub user: Option<OsmUser>,
    pub changeset_id: i64,
    pub visible: bool,
    pub tags: Vec<Tag>,
    pub way_nodes: Vec<WayNode>,
}

impl From<BaseElement> for Way {
    fn from(el: BaseElement) -> Self {
        Self {
            id: el.id,
            version: el.version,
            timestamp: el.timestamp,
            user: el.user,
            changeset_id: el.changeset_id,
            visible: el.visible,
            tags: el.tags,
            way_nodes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct WayNode {
    pub id: i64,
    pub latitude: Option<i64>,
    pub longitude: Option<i64>,
}

impl WayNode {
    pub fn new_without_coords(id: i64) -> Self {
        Self {
            id,
            latitude: None,
            longitude: None,
        }
    }

    pub fn new(id: i64, latitude: i64, longitude: i64) -> Self {
        Self {
            id,
            latitude: Some(latitude),
            longitude: Some(longitude),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct Relation {
    pub id: i64,
    pub version: i32,
    pub timestamp: Option<DateTime<Utc>>,
    pub user: Option<OsmUser>,
    pub changeset_id: i64,
    pub visible: bool,
    pub tags: Vec<Tag>,
    pub members: Vec<RelationMember>,
}

impl From<BaseElement> for Relation {
    fn from(el: BaseElement) -> Self {
        Self {
            id: el.id,
            version: el.version,
            timestamp: el.timestamp,
            user: el.user,
            changeset_id: el.changeset_id,
            visible: el.visible,
            tags: el.tags,
            members: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationMember {
    pub member_id: i64,
    pub member_type: ElementType,
    pub role: String,
}
