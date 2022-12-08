use super::Triplestore;
use crate::mapping::RDFNodeType;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::Context;
use crate::triplestore::sparql::solution_mapping::{is_string_col, SolutionMappings};
use crate::triplestore::sparql::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use log::warn;
use polars::prelude::IntoLazy;
use polars::prelude::{col, concat, lit, Expr};
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::prelude::JoinType;
use polars_core::series::Series;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

impl Triplestore {
    pub fn lazy_triple_pattern(
        &self,
        solution_mappings: Option<SolutionMappings>,
        triple_pattern: &TriplePattern,
        _context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => {
                let map_opt = self.df_map.get(n.as_str());
                if let Some(m) = map_opt {
                    if m.is_empty() {
                        panic!("Empty map should never happen");
                    } else if m.len() > 1 {
                        todo!("Multiple datatypes not supported yet")
                    } else {
                        let (dt, tt) = m.iter().next().unwrap();
                        assert!(tt.unique, "Should be deduplicated");
                        let mut lf = concat(
                            tt.get_lazy_frames()
                                .map_err(|x| SparqlError::TripleTableReadError(x))?,
                            true,
                            true,
                        )
                        .unwrap()
                        .select([col("subject"), col("object")]);
                        let mut var_cols = vec![];
                        let mut str_cols = vec![];
                        match &triple_pattern.subject {
                            TermPattern::NamedNode(nn) => {
                                lf = lf
                                    .filter(col("subject").eq(Expr::Literal(
                                        sparql_named_node_to_polars_literal_value(nn),
                                    )))
                                    .drop_columns(["subject"])
                            }
                            TermPattern::Literal(lit) => {
                                lf = lf
                                    .filter(col("subject").eq(Expr::Literal(
                                        sparql_literal_to_polars_literal_value(lit),
                                    )))
                                    .drop_columns(["subject"])
                            }
                            TermPattern::Variable(var) => {
                                lf = lf.rename(["subject"], [var.as_str()]);
                                str_cols.push(var.as_str().to_string());
                                var_cols.push(var.as_str().to_string());
                            }
                            _ => {
                                todo!("No support for {}", &triple_pattern.object)
                            }
                        }
                        match &triple_pattern.object {
                            TermPattern::NamedNode(nn) => {
                                lf = lf
                                    .filter(col("object").eq(Expr::Literal(
                                        sparql_named_node_to_polars_literal_value(nn),
                                    )))
                                    .drop_columns(["object"])
                            }
                            TermPattern::Literal(lit) => {
                                lf = lf
                                    .filter(col("object").eq(Expr::Literal(
                                        sparql_literal_to_polars_literal_value(lit),
                                    )))
                                    .drop_columns(["object"])
                            }
                            TermPattern::Variable(var) => {
                                lf = lf.rename(["object"], [var.as_str()]);
                                var_cols.push(var.as_str().to_string());
                                if is_string_col(dt) {
                                    str_cols.push(var.as_str().to_string());
                                }
                            }
                            TermPattern::BlankNode(bn) => {
                                lf = lf.rename(["object"], [bn.as_str()]);
                                var_cols.push(bn.as_str().to_string());
                                if is_string_col(dt) {
                                    str_cols.push(bn.as_str().to_string())
                                }
                            }
                        }
                        if let Some(mut mappings) = solution_mappings {
                            let join_cols: Vec<String> = var_cols
                                .clone()
                                .into_iter()
                                .filter(|x| mappings.columns.contains(x))
                                .collect();

                            for s in str_cols {
                                if join_cols.contains(&s) {
                                    lf = lf.with_column(col(&s).cast(DataType::Categorical(None)));
                                    mappings.mappings = mappings
                                        .mappings
                                        .with_column(col(&s).cast(DataType::Categorical(None)));
                                }
                            }

                            let join_on: Vec<Expr> = join_cols.iter().map(|x| col(x)).collect();

                            if join_on.is_empty() {
                                mappings.mappings = mappings.mappings.join(
                                    lf,
                                    join_on.as_slice(),
                                    join_on.as_slice(),
                                    JoinType::Cross,
                                );
                            } else {
                                let join_col_exprs: Vec<Expr> =
                                    join_cols.iter().map(|x| col(x)).collect();
                                let all_false = [false].repeat(join_cols.len());
                                lf = lf.sort_by_exprs(
                                    join_col_exprs.as_slice(),
                                    all_false.as_slice(),
                                    false,
                                );
                                mappings.mappings = mappings.mappings.sort_by_exprs(
                                    join_col_exprs.as_slice(),
                                    all_false.as_slice(),
                                    false,
                                );
                                mappings.mappings = mappings.mappings.join(
                                    lf,
                                    join_on.as_slice(),
                                    join_on.as_slice(),
                                    JoinType::Inner,
                                );
                            }
                            //Update mapping columns
                            for c in &var_cols {
                                mappings.columns.insert(c.to_string());
                            }
                            if let TermPattern::Variable(v) = &triple_pattern.subject {
                                mappings
                                    .rdf_node_types
                                    .insert(v.as_str().to_string(), RDFNodeType::IRI);
                            }
                            if let TermPattern::Variable(v) = &triple_pattern.object {
                                mappings
                                    .rdf_node_types
                                    .insert(v.as_str().to_string(), dt.clone());
                            }

                            return Ok(mappings);
                        } else {
                            let mut datatypes = HashMap::new();
                            if let TermPattern::Variable(v) = &triple_pattern.subject {
                                datatypes.insert(v.as_str().to_string(), RDFNodeType::IRI);
                            }
                            if let TermPattern::Variable(v) = &triple_pattern.object {
                                datatypes.insert(v.as_str().to_string(), dt.clone());
                            }
                            return Ok(SolutionMappings {
                                mappings: lf,
                                columns: var_cols.into_iter().map(|x| x.to_string()).collect(),
                                rdf_node_types: datatypes,
                            });
                        }
                    }
                } else {
                    warn!("Could not find triples for predicate {:?}", n);
                    let mut out_columns = HashSet::new();
                    let mut out_datatypes = HashMap::new();
                    if let TermPattern::Variable(v) = &triple_pattern.subject {
                        out_columns.insert(v.as_str().to_string());
                        out_datatypes.insert(v.as_str().to_string(), RDFNodeType::None);
                    }
                    if let TermPattern::Variable(v) = &triple_pattern.object {
                        out_columns.insert(v.as_str().to_string());
                        out_datatypes.insert(v.as_str().to_string(), RDFNodeType::None);
                    }
                    let mut variables: Vec<&String> = out_columns.iter().collect();
                    variables.sort();
                    if let Some(SolutionMappings {
                        mut mappings,
                        mut columns,
                        rdf_node_types: mut datatypes,
                    }) = solution_mappings
                    {
                        mappings = mappings.filter(lit(false));
                        let overlap: Vec<&String> = columns.intersection(&out_columns).collect();
                        if overlap.is_empty() {
                            return Ok(SolutionMappings::new(mappings, columns, datatypes));
                        }
                        let mut series = vec![];
                        for c in &variables {
                            if !columns.contains(*c) {
                                series.push(Series::new_empty(&c, &DataType::Null));
                            }
                        }
                        let join_on: Vec<Expr> = overlap.into_iter().map(|x| col(x)).collect();
                        let out_lf = DataFrame::new(series).unwrap().lazy();
                        mappings = mappings.join(
                            out_lf,
                            join_on.as_slice(),
                            join_on.as_slice(),
                            JoinType::Cross,
                        );
                        for (k, v) in out_datatypes {
                            if !datatypes.contains_key(&k) {
                                datatypes.insert(k, v);
                            }
                        }
                        columns.extend(out_columns);
                        Ok(SolutionMappings::new(mappings, columns, datatypes))
                    } else {
                        let mut series = vec![];
                        for var in variables {
                            let mut new_series = Series::new_empty(var, &DataType::Null);
                            new_series.rename(var);
                            series.push(new_series);
                        }
                        let out_lf = DataFrame::new(series).unwrap().lazy();
                        Ok(SolutionMappings::new(out_lf, out_columns, out_datatypes))
                    }
                }
            }
            NamedNodePattern::Variable(..) => {
                todo!("Not supported yet")
            }
        }
    }
}
