mod constraints;
mod errors;
mod shapes;
mod instantiation;

use super::Triplestore;
use crate::shacl::errors::ShaclError;
use crate::shacl::shapes::{NodeShape};
use polars_core::frame::DataFrame;
use polars_core::utils::concat_df;

impl Triplestore {
    pub fn validate(&self) -> Result<DataFrame, ShaclError> {
        let shapes = self.get_shape_graph()?;
        let mut reports = vec![];
        for s in shapes {
            reports.push(self.validate_node_shape(&s))
        }
        Ok(concat_df(reports.as_slice()).unwrap())
    }

    fn validate_node_shape(&self, node_shape: &NodeShape) -> DataFrame {

    }

}
