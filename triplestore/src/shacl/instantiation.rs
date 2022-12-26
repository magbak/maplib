use super::Triplestore;
use crate::shacl::errors::ShaclError;
use crate::shacl::shapes::NodeShape;
use crate::shacl::shapes::{Path, PropertyShape, Shape, TargetDeclaration, TargetNodes};
use oxrdf::vocab::rdf::{NIL, TYPE};
use oxrdf::NamedNode;
use polars::prelude::{col, concat, lit};
use polars_core::frame::DataFrame;
use polars_core::prelude::AnyValue;
use polars_core::utils::concat_df;
use representation::RDFNodeType;
use std::collections::HashMap;
use std::hash::Hash;
use crate::shacl::constraints::Constraint;

const SHACL_NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SHACL_TARGET_NODE: &str = "http://www.w3.org/ns/shacl#targetNode";
const SHACL_TARGET_CLASS: &str = "http://www.w3.org/ns/shacl#targetClass";
const SHACL_TARGETS_SUBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetsSubjectsOf";
const SHACL_TARGETS_OBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetsObjectsOf";
const SHACL_PROPERTY: &str = "http://www.w3.org/ns/shacl#property";
const SHACL_ALTERNATIVE_PATH: &str = "http://www.w3.org/ns/shacl#alternativePath";
const SHACL_SEQUENCE_PATH: &str = "http://www.w3.org/ns/shacl#alternativePath";
const SHACL_ZERO_OR_MORE_PATH: &str = "http://www.w3.org/ns/shacl#zeroOrMorePath";
const SHACL_ONE_OR_MORE_PATH: &str = "http://www.w3.org/ns/shacl#oneOrMorePath";
const SHACL_ZERO_OR_ONE_PATH: &str = "http://www.w3.org/ns/shacl#zeroOrOnePath";
const SHACL_FIRST: &str = "http://www.w3.org/ns/shacl#first";
const SHACL_REST: &str = "http://www.w3.org/ns/shacl#rest";

impl Triplestore {
    pub(crate) fn get_shape_graph(&self) -> Result<Vec<NodeShape>, ShaclError> {
        //["subject"]
        let node_shapes_df = self.get_node_shape_df()?;
        //["subject", "object"]
        let mut target_declaration_map = self.get_target_declarations()?;
        //["subject", "object"]
        let mut properties_map = self.get_properties_map()?;
        if let Some(node_shapes_df) = node_shapes_df {
            let mut node_iter = node_shapes_df.column("subject").unwrap().iter();
            let mut node_shapes = vec![];
            for _ in 0..node_shapes_df.height() {
                let node = node_iter.next();
                let node_str;
                if let Some(AnyValue::Utf8(s)) = node {
                    node_str = s
                } else {
                    panic!("Should never happen");
                }
                let target_declarations =
                    if let Some(trgs) = target_declaration_map.remove(node_str) {
                        trgs
                    } else {
                        panic!("Should also never happen");
                    };

                let property_shapes = if let Some(pshapes) = properties_map.remove(node_str) {
                    pshapes
                } else {
                    return Err(ShaclError::NodeShapeMissingProperties);
                };
                let node_shape = NodeShape {
                    target_declarations,
                    property_shapes,
                };
                node_shapes.push(node_shape);
            }
            //Warn about orphans!
            Ok(node_shapes)
        } else {
            //Warn about orphans!
            Ok(vec![])
        }
    }

    fn get_node_shape_df(&self) -> Result<Option<DataFrame>, ShaclError> {
        let mut node_shapes = vec![];
        if let Some(subjects) = self.df_map.get(TYPE.as_str()) {
            if let Some(blank_table) = subjects.get(&RDFNodeType::BlankNode) {
                for f in blank_table
                    .get_lazy_frames()
                    .map_err(|x| ShaclError::TriplestoreError(x))?
                {
                    node_shapes.push(
                        f.filter(col("object").eq(lit(SHACL_NODE_SHAPE)))
                            .select([col("subject")]),
                    )
                }
            }
        }
        if node_shapes.is_empty() {
            return Ok(None);
        }
        let node_shapes_df = concat(node_shapes, true, true).unwrap().collect().unwrap();
        Ok(Some(node_shapes_df))
    }

    fn get_target_declarations(
        &self,
    ) -> Result<HashMap<String, Vec<TargetDeclaration>>, ShaclError> {
        let mut declarations_map: HashMap<String, Vec<TargetDeclaration>> = HashMap::new();
        if let Some(target_node_map) = self.df_map.get(SHACL_TARGET_NODE) {
            for (rdf_node_type, v) in target_node_map {
                for lf in v
                    .get_lazy_frames()
                    .map_err(|x| ShaclError::TriplestoreError(x))?
                {
                    let dfs = lf.collect().unwrap().partition_by(vec!["subject"]).unwrap();
                    for df in dfs {
                        let subject = df.column("subject").unwrap().get(0);
                        let subject_string;
                        if let AnyValue::Utf8(subject) = subject {
                            subject_string = subject.to_string();
                        } else {
                            panic!("Subject of sh:targetNode was not represented as expected");
                        }
                        let trg = TargetNodes {
                            series: df.column("object").unwrap().clone(),
                            rdf_node_type: rdf_node_type.clone(),
                        };
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
        self.find_non_instance_target_declaration(
            &mut declarations_map,
            SHACL_TARGET_CLASS,
            &|x| TargetDeclaration::TargetClass(x),
        )?;
        self.find_non_instance_target_declaration(
            &mut declarations_map,
            SHACL_TARGETS_SUBJECTS_OF,
            &|x| TargetDeclaration::TargetSubjectsOf(x),
        )?;
        self.find_non_instance_target_declaration(
            &mut declarations_map,
            SHACL_TARGETS_OBJECTS_OF,
            &|x| TargetDeclaration::TargetObjectsOf(x),
        )?;

        Ok(declarations_map)
    }

    fn find_non_instance_target_declaration(
        &self,
        declarations_map: &mut HashMap<String, Vec<TargetDeclaration>>,
        property_uri: &str,
        func: &dyn Fn(NamedNode) -> TargetDeclaration,
    ) -> Result<(), ShaclError> {
        if let Some(target_class_map) = self.df_map.get(property_uri) {
            if let Some(tt) = target_class_map.get(&RDFNodeType::IRI) {
                for lf in tt
                    .get_lazy_frames()
                    .map_err(|x| ShaclError::TriplestoreError(x))?
                {
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
                        let decl = func(
                            NamedNode::new(obj_string).map_err(|x| ShaclError::IriParseError(x))?,
                        );
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

    fn get_property_shape_map(&self) -> Result<HashMap<&str, Vec<PropertyShape>>, ShaclError> {
        let mut property_rels = vec![];
        if let Some(map) = self.df_map.get(SHACL_PROPERTY) {
            if let Some(tt) = map.get(&RDFNodeType::IRI) {
                let lfs = tt
                    .get_lazy_frames()
                    .map_err(|x| ShaclError::TriplestoreError(x))?;
                for lf in lfs {
                    property_rel.push(lf);
                }
            }
        }
        let property_df = concat(property_rels, true, true)
            .unwrap()
            .collect()
            .unwrap();
        let mut subj_iter = property_df.column("subject").unwrap().iter();
        let mut obj_iter = property_df.column("object").unwrap().iter();
        let mut map = HashMap::new();

        let mut paths_map = self.get_path_map()?;
        let mut constraints_map = self.get_constraints_map()?;

        for _ in property_df.height() {
            let node_str = if let Some(AnyValue::Utf8(s)) = subj_iter.next() {
                s
            } else {
                panic!("Should never happen");
            };
            let prop_str = if let Some(AnyValue::Utf8(s)) = obj_iter.next() {
                s
            } else {
                panic!("Should never happen");
            };
            let path = if let Some(p) = paths_map.remove(prop_str) {
                p
            } else {
                return Err(ShaclError::PropertyMissingPath(prop_str.to_string()));
            };
            let property = PropertyShape {
                path,
                name: None,
                description: None,
                constraints: vec![],
            };
        }

        let mut path_rels = vec![];
    }

    fn get_path_map<'a>(&'a self) -> Result<HashMap<&'a str, Path>, ShaclError> {
        let mut out_map = HashMap::new();
        let mut path_rels = vec![];
        if let Some(map) = self.df_map.get(SHACL_PATH) {
            if let Some(tt) = map.get(&RDFNodeType::IRI) {
                let lfs = tt
                    .get_lazy_frames()
                    .map_err(|x| ShaclError::TriplestoreError(x))?;
                for lf in lfs {
                    let df = lf.column.collect().unwrap();
                    path_rels.push(df);
                }
            }
        }
        let df = concat(path_rels, true, true).unwrap().collect().unwrap();

        let mut prop_iter = df.column("subject").unwrap().iter();
        let mut path_elem_iter = df.column("object").unwrap().iter();

        let (props_map, first_map, rest_map) = self.get_maps()?;

        for _ in 0..df.height() {
            let prop = prop_iter.next();
            let prop_str;
            if let Some(AnyValue::Utf8(s)) = prop {
                prop_str = s;
            } else {
                panic!("Prop is str");
            }
            let path_elem = path_elem_iter.next();
            let path_elem_str;
            if let Some(AnyValue::Utf8(s)) = path_elem {
                path_elem_str = s
            } else {
                panic!("Path elem is always string");
            }
            let pp = create_property_path(path_elem_str, &props_map, &first_map, &rest_map)?;
            out_map.insert(prop_str, pp);
        }

        Ok(out_map)
    }

    fn get_any_nonpath_object_property_df(&self) -> Result<DataFrame, ShaclError> {
        let mut lfs = vec![];
        for (verb, map) in &self.df_map {
            if verb != SHACL_PATH {
                if let Some(tt) = map.get(&RDFNodeType::BlankNode) {
                    for mut lf in tt
                        .get_lazy_frames()
                        .map_err(|x| ShaclError::TriplestoreError(x))?
                    {
                        lf = lf.with_column(lit(verb).alias("verb"));
                        lfs.push(lf)
                    }
                }
                if let Some(tt) = map.get(&RDFNodeType::IRI) {
                    for mut lf in tt
                        .get_lazy_frames()
                        .map_err(|x| ShaclError::TriplestoreError(x))?
                    {
                        lf = lf.with_column(lit(verb).alias("verb"));
                        lfs.push(lf)
                    }
                }
            }
        }
        Ok(concat(lfs, true, true).unwrap().collect().unwrap())
    }

    fn get_maps(&self) -> Result<(HashMap<&str, (&str, &str)>, HashMap<&str, &str>, HashMap<&str, &str>), ShaclError> {
        let any_object_property_df = self.get_any_nonpath_object_property_df()?;
        let mut props_map = HashMap::new();
        let mut first_map = HashMap::new();
        let mut rest_map = HashMap::new();
        let mut subj_iter = any_object_property_df.column("subject").unwrap().iter();
        let mut verb_iter = any_object_property_df.column("verb").unwrap().iter();
        let mut obj_iter = any_object_property_df.column("object").unwrap().iter();

        for _ in 0..any_object_property_df.height() {
            let subj = subj_iter.next().unwrap();
            let verb = verb_iter.next().unwrap();
            let obj = obj_iter.next().unwrap();
            let subj_str;
            let verb_str;
            let obj_str;
            if let Some(AnyValue::Utf8(s)) = subj {
                subj_str = s
            } else {
                panic!("Subject always string");
            }
            if let Some(AnyValue::Utf8(s)) = verb {
                verb_str = s
            } else {
                panic!("Verb always string");
            }
            if let Some(AnyValue::Utf8(s)) = obj {
                obj_str = s
            } else {
                panic!("Object always string");
            }
            if verb_str == SHACL_FIRST {
                first_map.insert(subj_str, obj_str);
            } else if verb_str == SHACL_REST {
                rest_map.insert(subj_str, obj_str);
            } else {
                props_map.insert(subj_str, (verb_str, obj_str));
            }
        }
        Ok((props_map, first_map, rest_map))
    }

    fn get_constraints_map(&self, props_map: &HashMap<&str, (&str, &str)>) -> HashMap<&str, Vec<Constraint>> {
        todo!()
    }
}

fn create_property_path(
    elem: &str,
    props_map: &HashMap<&str, (&str, &str)>,
    first_map: &HashMap<&str, &str>,
    rest_map: &HashMap<&str, &str>,
) -> Result<Path, ShaclError> {
    if let Some((verb, obj)) = props_map.get(&elem) {
        if verb == &SHACL_ONE_OR_MORE_PATH {
            return Ok(Path::OneOrMore(Box::new(create_property_path(
                obj, props_map, first_map, rest_map,
            )?)));
        } else if verb == &SHACL_ZERO_OR_ONE_PATH {
            return Ok(Path::ZeroOrOne(Box::new(create_property_path(
                obj, props_map, first_map, rest_map,
            )?)));
        } else if verb == &SHACL_ZERO_OR_MORE_PATH {
            return Ok(Path::ZeroOrMore(Box::new(create_property_path(
                obj, props_map, first_map, rest_map,
            )?)));
        } else if verb == &SHACL_ALTERNATIVE_PATH || verb == &SHACL_SEQUENCE_PATH {
            let elements = get_list_elems(obj, first_map, rest_map)?;
            let mut paths = vec![];
            for e in elements {
                paths.push(Box::new(create_property_path(
                    e, props_map, first_map, rest_map,
                )?));
            }
            if verb == SHACL_ALTERNATIVE_PATH {
                return Ok(Path::Alternative(paths));
            } else if verb == SHACL_SEQUENCE_PATH {
                return OK(Path::Sequence(paths));
            } else {
                panic!("Will never happen")
            }
        } else {
            panic!("Should not happen??")
        }
    } else {
        return Ok(Path::Predicate(
            NamedNode::new(elem.to_string()).map_err(|x| ShaclError::IriParseError(x))?,
        ));
    }
}

fn get_list_elems<'a>(
    mut list: &str,
    first_map: &HashMap<&str, &'a str>,
    rest_map: &HashMap<&str, &str>,
) -> Result<Vec<&'a str>, ShaclError> {
    let nil = NIL.as_str();
    let mut l = vec![];
    let mut finished = false;
    while !finished {
        if list == nil {
            finished = true;
        } else {
            if let Some(f) = first_map.get(&list) {
                l.push(*f);
            } else {
                return Err(ShaclError::ListMissingFirstElementError(list.to_string()));
            }
            if let Some(r) = rest_map.get(&list) {
                list = r;
            } else {
                return Err(ShaclError::ListMissingRestError(list.to_string()));
            }
        }
    }
    Ok(l)
}
