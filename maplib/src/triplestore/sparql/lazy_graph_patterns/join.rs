use super::Triplestore;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;
use log::debug;
use polars::prelude::{col, Expr};
use polars_core::prelude::JoinType;
use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub(crate) fn lazy_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing join graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);

        let mut left_solution_mappings =
            self.lazy_graph_pattern(left, solution_mappings.clone(), &left_context)?;
        let SolutionMappings {
            mappings: mut right_mappings,
            columns: mut right_columns,
            rdf_node_types: mut right_datatypes,
        } = self.lazy_graph_pattern(right, solution_mappings, &right_context)?;

        let mut join_on: Vec<&String> = left_solution_mappings
            .columns
            .intersection(&right_columns)
            .collect();
        join_on.sort();

        let join_on_cols: Vec<Expr> = join_on.iter().map(|x| col(x)).collect();

        if join_on.is_empty() {
            left_solution_mappings.mappings = left_solution_mappings.mappings.join(
                right_mappings,
                join_on_cols.as_slice(),
                join_on_cols.as_slice(),
                JoinType::Cross,
            )
        } else {
            let all_false = [false].repeat(join_on_cols.len());
            right_mappings = right_mappings.sort_by_exprs(join_on_cols.as_slice(), all_false.as_slice(), false);
            left_solution_mappings.mappings = left_solution_mappings.mappings.sort_by_exprs(
                join_on_cols.as_slice(),
                all_false.as_slice(),
                false,
            );
            left_solution_mappings.mappings = left_solution_mappings.mappings.join(
                right_mappings,
                join_on_cols.as_slice(),
                join_on_cols.as_slice(),
                JoinType::Inner,
            )
        }
        for c in right_columns.drain() {
            left_solution_mappings.columns.insert(c);
        }
        for (var, dt) in right_datatypes.drain() {
            if let Some(dt_left) = left_solution_mappings.rdf_node_types.get(&var) {
                if &dt != dt_left {
                    return Err(SparqlError::InconsistentDatatypes(
                        var,
                        dt_left.clone(),
                        dt,
                        context.as_str().to_string(),
                    ));
                }
            } else {
                left_solution_mappings.rdf_node_types.insert(var, dt);
            }
        }

        Ok(left_solution_mappings)
    }
}
