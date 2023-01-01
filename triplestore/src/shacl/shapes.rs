use oxrdf::{Literal, NamedNode};
use polars_core::series::Series;
use representation::RDFNodeType;
use crate::shacl::constraints::Constraint;

pub enum Severity {
    INFO,
    WARNING,
    VIOLATION,
}

pub enum Shape {
    NodeShape(NodeShape),
    PropertyShape(PropertyShape)
}

pub struct NodeShape {
    pub(crate) target_declarations: Vec<TargetDeclaration>,
    pub(crate) property_shapes: Vec<PropertyShape>
}

pub struct PropertyShape {
    pub(crate) path: Path,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) constraints: Vec<Constraint>
}

pub enum Path {
    Predicate(NamedNode),
    Inverse(Box<Path>),
    Sequence(Vec<Box<Path>>),
    Alternative(Vec<Box<Path>>),
    ZeroOrMore(Box<Path>),
    OneOrMore(Box<Path>),
    ZeroOrOne(Box<Path>)
}

pub enum NamedNodeOrLiteral {
    NamedNode(NamedNode),
    Literal(Literal)
}

pub struct TargetNodes {
    pub series:Series,
    pub rdf_node_type: RDFNodeType
}

pub enum TargetDeclaration {
    TargetNodes(TargetNodes),
    TargetClass(NamedNode),
    TargetSubjectsOf(NamedNode),
    TargetObjectsOf(NamedNode)
}