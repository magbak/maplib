use super::Triplestore;
use log::debug;
use polars::prelude::{col, Expr};
use spargebra::algebra::GraphPattern;
use polars_core::prelude::JoinType;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_minus(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing minus graph pattern");
        let left_context = context.extension_with(PathEntry::MinusLeftSide);
        let right_context = context.extension_with(PathEntry::MinusRightSide);
        let mut left_solution_mappings = self
            .lazy_graph_pattern(
                left,
                solution_mappings.clone(),
                &left_context,
            )
            ?;

        let right_solution_mappings = self
            .lazy_graph_pattern(
                right,
                solution_mappings,
                &right_context,
            )
            ?;

        let SolutionMappings{ mappings: mut right_mappings, columns:  right_columns, rdf_node_types: _ } = right_solution_mappings;

        let mut join_on:Vec<&String> = left_solution_mappings.columns.intersection(&right_columns).collect();
        join_on.sort();


        if join_on.is_empty() {
            Ok(left_solution_mappings)
        } else {
            let join_on_cols:Vec<Expr> = join_on.iter().map(|x|col(x)).collect();
            let all_false = [false].repeat(join_on_cols.len());
            right_mappings = right_mappings.sort_by_exprs(join_on_cols.as_slice(), all_false.as_slice(), false);
            left_solution_mappings.mappings = left_solution_mappings.mappings.sort_by_exprs(
                join_on_cols.as_slice(),
                all_false.as_slice(),
                false,
            );
            left_solution_mappings.mappings = left_solution_mappings.mappings.join(right_mappings, join_on_cols.as_slice(), join_on_cols.as_slice(), JoinType::Anti);
            Ok(left_solution_mappings)
        }
    }
}
