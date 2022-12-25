mod constraints;
mod errors;
mod shapes;
mod instantiation;

use super::Triplestore;
use crate::shacl::errors::ShaclError;
use crate::shacl::shapes::{Path, Shape, TargetDeclaration, TargetNodes};
use oxrdf::vocab::rdf::{NIL, TYPE};
use oxrdf::NamedNode;
use polars::prelude::{col, concat, lit};
use polars_core::frame::DataFrame;
use polars_core::prelude::AnyValue;
use polars_core::utils::concat_df;
use representation::RDFNodeType;
use std::collections::HashMap;

impl Triplestore {
    pub fn validate(&self) -> Result<DataFrame, ShaclError> {
        let shapes = self.get_shape_graph()?;
        let mut reports = vec![];
        for s in shapes {
            reports.push(self.validate_shape(&s))
        }
        Ok(concat_df(reports.as_slice()).unwrap())
    }

    fn validate_shape(&self, shape: &Shape) -> DataFrame {

    }

}
