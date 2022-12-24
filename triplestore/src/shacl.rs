mod constraints;
mod errors;
mod shapes;

use std::collections::HashMap;
use oxrdf::NamedNode;
use oxrdf::vocab::rdf::TYPE;
use polars::prelude::{col, concat, lit};
use polars_core::frame::DataFrame;
use polars_core::prelude::AnyValue;
use polars_core::utils::concat_df;
use representation::RDFNodeType;
use crate::shacl::errors::ShaclError;
use crate::shacl::shapes::{Shape, TargetDeclaration, TargetNodes};
use super::Triplestore;

const SHACL_NODE_SHAPE:&str = "http://www.w3.org/ns/shacl#NodeShape";
const SHACL_TARGET_NODE:&str = "http://www.w3.org/ns/shacl#targetNode";
const SHACL_TARGET_CLASS:&str = "http://www.w3.org/ns/shacl#targetClass";
const SHACL_TARGETS_SUBJECTS_OF:&str = "http://www.w3.org/ns/shacl#targetsSubjectsOf";
const SHACL_TARGETS_OBJECTS_OF:&str = "http://www.w3.org/ns/shacl#targetsObjectsOf";


impl Triplestore {
    pub fn validate(&self) -> Result<DataFrame, ShaclError>  {
        let shapes = self.get_shape_graph()?;
        let mut reports = vec![];
        for s in shapes {
           reports.push(self.validate_shape(&s))
        }
        Ok(concat_df(reports.as_slice()).unwrap())
    }

    fn get_shape_graph(&self) -> Result<Vec<Shape>, ShaclError> {
        //["subject"]
        let node_shapes_df = self.get_node_shape_df()?;
        //["subject", "object"]
        let target_declaration_df = self.get_target_declarations()?;
        //["subject", "object"]

        Ok(())
    }

    fn get_node_shape_df(&self) -> Result<Option<DataFrame>, ShaclError> {
        let mut node_shapes = vec![];
        if let Some(subjects) = self.df_map.get(TYPE.as_str()) {
            if let Some(blank_table) = subjects.get(&RDFNodeType::BlankNode) {
                for f in blank_table.get_lazy_frames().map_err(|x|ShaclError::TriplestoreError(x))? {
                    node_shapes.push(f.filter(col("object").eq(lit(SHACL_NODE_SHAPE))).select([col("subject")]))
                }
            }
        }
        if node_shapes.is_empty() {
            return Ok(None);
        }
        let node_shapes_df = concat(node_shapes, true, true).unwrap().collect().unwrap();
        Ok(Some(node_shapes_df))
    }

    fn get_target_declarations(&self) -> Result<HashMap<String, Vec<TargetDeclaration>>, ShaclError> {
        let mut declarations_map: HashMap<String, Vec<TargetDeclaration>> = HashMap::new();
        if let Some(target_node_map) = self.df_map.get(SHACL_TARGET_NODE) {
            for (rdf_node_type,v) in target_node_map {
                for lf in v.get_lazy_frames().map_err(|x|ShaclError::TriplestoreError(x))? {
                    let dfs = lf.collect().unwrap().partition_by(vec!["subject"]).unwrap();
                    for df in dfs {
                        let subject = df.column("subject").unwrap().get(0);
                        let subject_string;
                        if let AnyValue::Utf8(subject) = subject {
                            subject_string = subject.to_string();
                        } else {
                            panic!("Subject of sh:targetNode was not represented as expected");
                        }
                        let trg = TargetNodes { series: df.column("object").unwrap().clone(), rdf_node_type:rdf_node_type.clone() };
                        let decl = TargetDeclaration::TargetNodes(trg);
                        if let Some(v) = declarations_map.get_mut(&subject_string) {
                            v.push(decl);
                        } else {
                            declarations_map.insert(subject_string, vec![decl]);
                        }
                    }
                }
            }
        }
        self.find_non_instance_target_declaration(&mut declarations_map, SHACL_TARGET_CLASS, &|x|TargetDeclaration::TargetClass(x))?;
        self.find_non_instance_target_declaration(&mut declarations_map, SHACL_TARGETS_SUBJECTS_OF, &|x|TargetDeclaration::TargetSubjectsOf(x))?;
        self.find_non_instance_target_declaration(&mut declarations_map, SHACL_TARGETS_OBJECTS_OF, &|x|TargetDeclaration::TargetObjectsOf(x))?;

        Ok(declarations_map)
    }

    fn find_non_instance_target_declaration(&self, declarations_map:&mut HashMap<String, Vec<TargetDeclaration>>, property_uri: &str, func:&dyn Fn(NamedNode)->TargetDeclaration) -> Result<(), ShaclError>  {
        if let Some(target_class_map) = self.df_map.get(property_uri) {
            if let Some(tt) = target_class_map.get(&RDFNodeType::IRI) {
                for lf in tt.get_lazy_frames().map_err(|x|ShaclError::TriplestoreError(x))? {
                    let df = lf.collect().unwrap();
                    let mut subject_iter = df.column("subject").unwrap().iter();
                    let mut object_iter = df.column("object").unwrap().iter();
                    for _ in 0..df.height() {
                        let subj = subject_iter.next();
                        let obj = object_iter.next();
                        let subj_string;
                        if let Some(AnyValue::Utf8(subj_str)) = subj {
                            subj_string = subj_str.to_string();
                        } else {
                            panic!("Subj always string");
                        }
                        let obj_string;
                        if let Some(AnyValue::Utf8(obj_str)) = obj {
                            obj_string = obj_str.to_string();
                        } else {
                            panic!("Obj always string")
                        }
                        let decl = func(NamedNode::new(obj_string).map_err(|x|ShaclError::IriParseError(x))?);
                        if let Some(v) = declarations_map.get_mut(&subj_string) {
                            v.push(decl);
                        } else {
                            declarations_map.insert(subj_string, vec![decl]);
                        }
                    }
                }
            }
        }
        Ok(())
    }


    fn validate_shape(&self, shape: &Shape) -> DataFrame {

    }

}

