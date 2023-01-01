use oxrdf::{Literal, NamedNode};
use crate::shacl::shapes::{NamedNodeOrLiteral, NodeShape, PropertyShape, Shape};

pub enum Constraint {
    Class(ClassConstraint),
    DataType(DataTypeConstraint),
    NodeKind(NodeKindConstraint),
    MinCount(MinCountConstraint),
    MaxCount(MaxCountConstraint),
    MinExclusive(MinExclusiveConstraint),
    MaxExclusive(MaxExclusiveConstraint),
    MinInclusive(MinInclusiveConstraint),
    MaxInclusive(MaxInclusiveConstraint),
    MinLength(MinLengthConstraint),
    MaxLength(MaxLengthConstraint),
    Pattern(PatternConstraint),
    LanguageIn(LanguageInConstraint),
    UniqueLang(UniqueLangConstraint),
    Equals(EqualsConstraint),
    Disjoint(DisjointConstraint),
    LessThan(LessThanConstraint),
    LessThanOrEquals(LessThanOrEqualsConstraint),
    Not(NotConstraint),
    And(AndConstraint),
    Or(OrConstraint),
    Xone(XoneConstraint),
    Node(NodeConstraint),
    Property(PropertyConstraint),
    QualifiedValueShape(QualifiedValueShapeConstraint),
    QualifiedMinCount(QualifiedMinCountConstraint),
    QualifiedMaxCount(QualifiedMaxCountConstraint),
    Closed(ClosedConstraint),
    IgnoredProperties(IgnoredPropertiesConstraint),
    HasValue(HasValueConstraint),
    In(InConstraint),
}

pub struct ClassConstraint {
    pub class: NamedNode
}
pub struct DataTypeConstraint {
    pub data_type:NamedNode
}

pub enum NodeKind {
    BlankNode,
    IRI,
    Literal,
    BlankNodeOrIRI,
    IRIOrLiteral,
    BlankNodeOrLiteral
}

pub struct NodeKindConstraint {
    pub node_kind: NodeKind
}
pub struct MinCountConstraint {
    pub min_count: i32
}
pub struct MaxCountConstraint {
    pub max_count: i32
}
pub struct MinExclusiveConstraint {
    pub min_exclusive: Literal
}
pub struct MaxExclusiveConstraint {
    pub max_exclusive: Literal
}
pub struct MinInclusiveConstraint {
    pub min_inclusive: Literal
}
pub struct MaxInclusiveConstraint {
    pub max_inclusive: Literal
}
pub struct MinLengthConstraint {
    pub min_length: i32
}
pub struct MaxLengthConstraint {
    pub max_length: i32
}
pub struct PatternConstraint {
    pub pattern: String,
    pub flags: String
}
pub struct LanguageInConstraint {
    pub tags: Vec<String>
}
pub struct UniqueLangConstraint {}
pub struct EqualsConstraint {
    pub other_predicate: NamedNode
}
pub struct DisjointConstraint {
    pub other_predicate: NamedNode
}
pub struct LessThanConstraint {
    pub other_predicate: NamedNode
}
pub struct LessThanOrEqualsConstraint {
    pub other_predicate: NamedNode
}
pub struct NotConstraint {
    pub shape: Shape
}
pub struct AndConstraint {
    pub shapes: Vec<Shape>
}
pub struct OrConstraint {
    pub shapes: Vec<Shape>
}
pub struct XoneConstraint {
    pub shapes: Vec<Shape>
}
pub struct NodeConstraint {
    pub node_shape: NodeShape
}
pub struct PropertyConstraint {
    pub property_shape: PropertyShape
}
pub struct QualifiedValueShapeConstraint {

}
pub struct QualifiedMinCountConstraint {}
pub struct QualifiedMaxCountConstraint {}
pub struct ClosedConstraint {}
pub struct IgnoredPropertiesConstraint {
    pub predicates: Vec<NamedNode>
}
pub struct HasValueConstraint {
    pub value: NamedNodeOrLiteral
}
pub struct InConstraint {
    pub values: Vec<NamedNodeOrLiteral>
}