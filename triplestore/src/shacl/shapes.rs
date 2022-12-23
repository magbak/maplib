use oxrdf::{Literal, NamedNode};
use polars_core::series::Series;
use representation::RDFNodeType;
use crate::shacl::constraints::Constraint;

pub enum Shape {
    NodeShape(NodeShape),
    PropertyShape(PropertyShape)
}

pub enum Severity {
    INFO,
    WARNING,
    VIOLATION,
}

pub struct NodeShape {
    target_declarations: Vec<TargetDeclaration>,
    property_shapes: Vec<PropertyShape>
}

pub struct PropertyShape {
    path: Path,
    name: Option<String>,
    description: Option<String>,
    constraints: Vec<Constraint>
}

pub enum Path {
    Predicate(NamedNode),
    Inverse(Box<Path>),
    Sequence(Box<Path>, Box<Path>),
    Alternative(Box<Path>, Box<Path>),
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