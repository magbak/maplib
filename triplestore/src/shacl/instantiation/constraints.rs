use super::Triplestore;
use crate::shacl::constraints::{
    ClassConstraint, Constraint, DataTypeConstraint, NodeKind, NodeKindConstraint,
};
use crate::shacl::errors::ShaclError;
use oxrdf::NamedNode;
use polars::prelude::concat;
use polars_core::datatypes::AnyValue;
use representation::RDFNodeType;
use std::collections::HashMap;

const SHACL_CLASS: &str = "http://www.w3.org/ns/shacl#class";
const SHACL_DATATYPE: &str = "http://www.w3.org/ns/shacl#datatype";
const SHACL_NODE_KIND: &str = "http://www.w3.org/ns/shacl#nodeKind";
const SHACL_NODE_KIND_BLANK_NODE: &str = "http://www.w3.org/ns/shacl#BlankNode";
const SHACL_NODE_KIND_IRI: &str = "http://www.w3.org/ns/shacl#IRI";
const SHACL_NODE_KIND_LITERAL: &str = "http://www.w3.org/ns/shacl#Literal";
const SHACL_NODE_KIND_BLANK_NODE_OR_IRI: &str = "http://www.w3.org/ns/shacl#BlankNodeOrIRI";
const SHACL_NODE_KIND_BLANK_NODE_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#BlankNodeOrLiteral";
const SHACL_NODE_KIND_IRI_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#IRIOrLiteral";

impl Triplestore {
    fn get_constraints_map(
        &self,
        props_map: &HashMap<&str, (&str, &str)>,
    ) -> Result<HashMap<&str, Vec<Constraint>>, ShaclError> {
        let mut class_map = self.get_iri_valued_constraints_map(SHACL_CLASS, &|x| {
            Ok(Constraint::Class(ClassConstraint {
                class: NamedNode::new(x).map_err(|x| ShaclError::IriParseError(x))?,
            }))
        })?;
        let mut datatype_map = self.get_iri_valued_constraints_map(SHACL_DATATYPE, &|x| {
            Ok(Constraint::DataType(DataTypeConstraint {
                data_type: NamedNode::new(x).map_err(|x| ShaclError::IriParseError(x))?,
            }))
        })?;
        let mut nodekind_map = self.get_iri_valued_constraints_map(SHACL_NODE_KIND, &|x| {
            Ok(Constraint::NodeKind(NodeKindConstraint {
                node_kind: if x == &SHACL_NODE_KIND_BLANK_NODE {
                    NodeKind::BlankNode
                } else if x == &SHACL_NODE_KIND_LITERAL {
                    NodeKind::Literal
                } else if x == &SHACL_NODE_KIND_IRI {
                    NodeKind::IRI
                } else if x == &SHACL_NODE_KIND_IRI_OR_LITERAL {
                    NodeKind::IRIOrLiteral
                }  else if x == &SHACL_NODE_KIND_BLANK_NODE_OR_IRI {
                    NodeKind::BlankNodeOrIRI
                }  else if x == &SHACL_NODE_KIND_BLANK_NODE_OR_LITERAL {
                    NodeKind::BlankNodeOrLiteral
                } else {
                    return Err(ShaclError::InvalidNodeKindError(x.to_string()))
                },
            }))
        })?;
        Ok()
    }

    fn get_iri_valued_constraints_map(
        &self,
        constraint_uri: &str,
        constraint_func: &dyn Fn(&str) -> Result<Constraint, ShaclError>,
    ) -> Result<HashMap<&str, Vec<Constraint>>, ShaclError> {
        let mut out_map: HashMap<&str, Vec<Constraint>> = HashMap::new();
        if let Some(map) = self.df_map.get(constraint_uri) {
            if let Some(tt) = map.get(&RDFNodeType::IRI) {
                let lfs = tt
                    .get_lazy_frames()
                    .map_err(|x| ShaclError::TriplestoreError(x))?;
                let df = concat(lfs, true, true).unwrap().collect().unwrap();
                let mut subj_iter = df.column("subject").unwrap().iter();
                let mut obj_iter = df.column("object").unwrap().iter();
                for _ in 0..df.height() {
                    let subj_str = if let Some(AnyValue::Utf8(subj_str)) = subj_iter.next() {
                        subj_str
                    } else {
                        panic!("");
                    };
                    let obj_str = if let Some(AnyValue::Utf8(obj_str)) = obj_iter.next() {
                        obj_str
                    } else {
                        //TODO: bad trg
                        panic!("");
                    };
                    let out_ctr = constraint_func(obj_str)?;
                    if let Some(v) = out_map.get_mut(subj_str) {
                        v.push(out_ctr);
                    } else {
                        out_map.insert(subj_str, vec![out_ctr]);
                    }
                }
            }
        }
        Ok(out_map)
    }

    fn get_literal_valued_constraints_map(
        &self,
        constraint_uri: &str,
        constraint_func: &dyn Fn(&str) -> Constraint,
    ) -> Result<HashMap<&str, Vec<Constraint>>, ShaclError> {
        let mut out_map: HashMap<&str, Vec<Constraint>> = HashMap::new();
        if let Some(map) = self.df_map.get(constraint_uri) {
            if let Some(tt) = map.get(&RDFNodeType::IRI) {
                let lfs = tt
                    .get_lazy_frames()
                    .map_err(|x| ShaclError::TriplestoreError(x))?;
                let df = concat(lfs, true, true).unwrap().collect().unwrap();
                let mut subj_iter = df.column("subject").unwrap().iter();
                let mut obj_iter = df.column("object").unwrap().iter();
                for _ in 0..df.height() {
                    let subj_str = if let Some(AnyValue::Utf8(subj_str)) = subj_iter.next() {
                        subj_str
                    } else {
                        panic!("");
                    };
                    let obj_str = if let Some(AnyValue::Utf8(obj_str)) = obj_iter.next() {
                        obj_str
                    } else {
                        //TODO: bad trg
                        panic!("");
                    };
                    let out_ctr = constraint_func(obj_str);
                    if let Some(v) = out_map.get_mut(subj_str) {
                        v.push(out_ctr);
                    } else {
                        out_map.insert(subj_str, vec![out_ctr]);
                    }
                }
            }
        }
        Ok(out_map)
    }
}
