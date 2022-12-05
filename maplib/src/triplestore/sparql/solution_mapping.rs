use std::collections::{HashMap, HashSet};
use polars::prelude::LazyFrame;
use crate::mapping::RDFNodeType;

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub columns: HashSet<String>,
    pub rdf_node_types: HashMap<String, RDFNodeType>
}

impl SolutionMappings {
    pub fn new(mappings: LazyFrame, columns:HashSet<String>, datatypes: HashMap<String, RDFNodeType>) -> SolutionMappings {
        SolutionMappings {
            mappings,
            columns,
            rdf_node_types: datatypes
        }
    }
}