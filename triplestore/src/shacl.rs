mod constraints;
mod errors;
mod shapes;

use oxrdf::vocab::rdf::TYPE;
use polars::prelude::{col, concat, lit};
use polars_core::frame::DataFrame;
use polars_core::utils::concat_df;
use representation::RDFNodeType;
use crate::shacl::errors::ShaclError;
use crate::shacl::shapes::Shape;
use super::Triplestore;

const SHACL_NODE_SHAPE:&str = "http://www.w3.org/ns/shacl#NodeShape";

impl Triplestore {
    pub fn validate(&self) -> Result<DataFrame, ShaclError>  {
        let shapes = self.get_shape_graph();
        let mut reports = vec![];
        for s in shapes {
           reports.push(self.validate_shape(s))
        }
        Ok(concat_df(reports).unwrap())
    }

    fn get_shape_graph(&self) -> Result<Vec<Shape>, ShaclError> {
        //["subject"]
        let node_shapes_df = self.get_node_shape_df()?;
        //["subject", "object"]
        let names_df = self.get_property_df()?;

        Ok()
    }

    fn get_node_shape_df(&self) -> Result<Option<DataFrame>, ShaclError> {
        let mut node_shapes = vec![];
        if let Some(subjects) = self.df_map.get(TYPE.as_str()) {
            if let Some(blank_table) = subjects.get(&RDFNodeType::BlankNode) {
                for f in blank_table.get_lazy_frames()? {
                    node_shapes.push(f.filter(col("object").eq(lit(SHACL_NODE_SHAPE))).select("subject"))
                }
            }
        }
        if node_shapes.is_empty() {
            return Ok(None);
        }
        let node_shapes_df = concat(node_shapes, true, true).unwrap().collect().unwrap();
        Ok(Some(node_shapes_df))
    }

    fn validate_shape(&self, shape: &Shape) -> DataFrame {

    }


}

