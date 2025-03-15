use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::deep::{rewrite_schema, try_rewrite_schema_opt};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, TableReference,
};
use datafusion_expr::expr::{Alias, ScalarFunction};
use datafusion_expr::{
    build_join_schema, Expr, Join, LogicalPlan, Projection, Subquery, SubqueryAlias,
    TableScan, Union,
};
use log::{debug, error, info, trace, warn};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct OptimizeProjectionsDeep {}

impl OptimizeProjectionsDeep {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

pub const FLAG_ENABLE: usize = 1;
pub const FLAG_ENABLE_PROJECTION_MERGING: usize = 2;
pub const FLAG_ENABLE_SUBQUERY_TRANSLATION: usize = 4;
pub const FLAG_ENABLE_PROJECTION_EXPR_DISCARD: usize = 8;

impl OptimizerRule for OptimizeProjectionsDeep {
    fn name(&self) -> &str {
        "optimize_projections_deep"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // All output fields are necessary:
        debug!(target: "deep", "optimize_projections_deep: plan={}", plan.display_indent());
        let options = config.options();
        if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE != 0 {
            let new_plan_transformed = deep_plan_transformer(plan, vec![], options)?;
            if new_plan_transformed.transformed {
                let new_plan = new_plan_transformed.data;
                let new_plan = new_plan
                    .transform_up(|p| Ok(Transformed::yes(p.recompute_schema()?)))?;
                Ok(new_plan)
            } else {
                Ok(Transformed::no(new_plan_transformed.data))
            }
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

pub type DeepColumnColumnMap = HashMap<Column, Vec<String>>;
pub type DeepColumnIndexMap = HashMap<usize, Vec<String>>;

#[derive(Clone)]
pub struct PlanWithDeepColumnMap {
    pub plan: Arc<LogicalPlan>,
    pub columns: HashMap<Column, Vec<String>>,
}

impl PlanWithDeepColumnMap {
    pub fn from_plan(input: &LogicalPlan) -> Self {
        Self {
            plan: Arc::new(input.clone()),
            columns: get_columns_referenced_in_plan(input, false),
        }
    }
}

impl Debug for PlanWithDeepColumnMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan_name = match self.plan.as_ref() {
            LogicalPlan::Projection(_) => "Projection".to_string(),
            LogicalPlan::Filter(_) => "Filter".to_string(),
            LogicalPlan::Window(_) => "Window".to_string(),
            LogicalPlan::Aggregate(_) => "Aggregate".to_string(),
            LogicalPlan::Sort(_) => "Sort".to_string(),
            LogicalPlan::Join(_) => "Join".to_string(),
            LogicalPlan::Repartition(_) => "Repartition".to_string(),
            LogicalPlan::Union(_) => "Union".to_string(),
            LogicalPlan::TableScan(_) => "TableScan".to_string(),
            LogicalPlan::EmptyRelation(_) => "EmptyRelation".to_string(),
            LogicalPlan::Subquery(_) => "Subquery".to_string(),
            LogicalPlan::SubqueryAlias(_) => "SubqueryAlias".to_string(),
            LogicalPlan::Limit(_) => "Limit".to_string(),
            LogicalPlan::Statement(_) => "Statement".to_string(),
            LogicalPlan::Values(_) => "Values".to_string(),
            LogicalPlan::Explain(_) => "Explain".to_string(),
            LogicalPlan::Analyze(_) => "Analyze".to_string(),
            LogicalPlan::Extension(_) => "Extension".to_string(),
            LogicalPlan::Distinct(_) => "Distinct".to_string(),
            LogicalPlan::Dml(_) => "Dml".to_string(),
            LogicalPlan::Ddl(_) => "Ddl".to_string(),
            LogicalPlan::Copy(_) => "Copy".to_string(),
            LogicalPlan::DescribeTable(_) => "DescribeTable".to_string(),
            LogicalPlan::Unnest(_) => "Unnest".to_string(),
            LogicalPlan::RecursiveQuery(_) => "RecursiveQuery".to_string(),
        };
        f.write_str(format!("plan={} columns={:?}", plan_name, self.columns).as_str())
    }
}

pub fn is_empty_deep_projection(p: &Vec<String>) -> bool {
    p.len() == 1 && p[0] == "*"
}

pub fn deep_plan_transformer(
    plan: LogicalPlan,
    cols_from_above: Vec<PlanWithDeepColumnMap>,
    options: &ConfigOptions,
) -> Result<Transformed<LogicalPlan>> {
    let mut cols_level: Vec<PlanWithDeepColumnMap> = cols_from_above.clone();
    trace!(
        target: "deep",
        ">>>>>>>>>>>>>>\ndeep_plan_transformer RESTART cols_from_above ={:?}",
        &cols_from_above,
    );
    plan.transform_down(|plan| {
        trace!(
            target: "deep",
            ">>>>>>>>>>>>>>\ndeep_plan_transformer working on plan={}, \n    cols_level ={:?}",
            &plan.display(), &cols_level,
        );
        let mut aliases_in_current_plan: HashMap<String, (Column, Vec<String>)> = HashMap::new();
        if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_SUBQUERY_TRANSLATION != 0 {
            // try to find aliases in current plan
            // for each alias
            //      try to look at above projections where the table is empty.
            //      if the column name is the same as the name of the alias, we need to change that pdc
            //          the key is the alias, the column is the new column
            if matches!(plan, LogicalPlan::TableScan(_)) {
                // all the null ones are for me !
                // a table scan has no aliases, we are at the end
                let LogicalPlan::TableScan(TableScan {
                                               table_name, ..
                                           }) = &plan else { unreachable!() };
                let this_table_relation = table_name.clone();
                for PlanWithDeepColumnMap { plan: _plan, columns } in cols_level.iter() {
                    for (col, projections) in columns.iter() {
                        if col.relation.is_none() {
                            // the key for this column is in the map !
                            let new_col = Column {
                                relation: Some(this_table_relation.clone()),
                                name: col.name.clone(),
                                spans: Default::default(),
                            };
                            merge_values_in_aliases(&mut aliases_in_current_plan, &new_col, projections);
                        }
                    }
                }
            } else if matches!(plan, LogicalPlan::Projection(_)) {
                for expr in plan.expressions().iter() {
                    match expr {
                        Expr::Alias(Alias { expr: alias_source, relation: relation, name }) => {
                            // info!("PRE 1 subquery translation: looking at alias_source={:?} relation={:?}, name={}", alias_source, relation, name);
                            // FIXME: what is the correct relation ? the one in the alias or the one in the expr ?
                            // We'll use the one in the expr
                            let alias_source_column_map = expr_to_deep_columns(alias_source.as_ref());
                            if alias_source_column_map.len() != 1 {
                                warn!("PRE 1 subquery translation: fix aliased cols from above - strange result for column {} map: {:?}", name, &alias_source_column_map);
                            } else {
                                let tmp_map_vec = alias_source_column_map.into_iter().collect::<Vec<_>>();
                                // we have a single entry
                                let (new_col, new_deep_projection) = tmp_map_vec[0].clone();
                                aliases_in_current_plan.insert(name.clone(), (new_col, new_deep_projection));
                            }
                        }
                        _ => {
                            trace!(target: "deep", "PRE trying to fill alias from something else EXPR: {:?}", expr);
                            let column_map = expr_to_deep_columns(expr);
                            if column_map.len() != 1 {
                                warn!("PRE 2 subquery translation: fix aliased cols from above - strange result for map: {:?}", &column_map);
                            } else {
                                let tmp_map_vec = column_map.into_iter().collect::<Vec<_>>();
                                // we have a single entry
                                let (new_col, new_deep_projection) = tmp_map_vec[0].clone();
                                aliases_in_current_plan.insert(new_col.name.clone(), (new_col, new_deep_projection));
                            }
                        }
                    }
                }
            }
            trace!(target: "deep", "PRE subquery translation: found aliases in current plan: {:?}", aliases_in_current_plan);
            trace!(target: "deep", "PRE subquery translation: from above: {:?}", &cols_from_above);
            // now, we go through all the pdcs that we got, and we see whether their relation is null and their column name is in the aliases_in_current_plan map
            // if they are, then we rewrite them so that the column is the latest one
            let mut new_cols_level: Vec<PlanWithDeepColumnMap> = vec![];
            for PlanWithDeepColumnMap { plan, columns } in cols_level.iter() {
                let mut new_columns: DeepColumnColumnMap = HashMap::new();
                for (col, projections) in columns.iter() {
                    if col.relation.is_none() && aliases_in_current_plan.contains_key(&col.name) {
                        let (actual_column, actual_projection) = aliases_in_current_plan.get(&col.name).unwrap();
                        trace!(target: "deep", "PRE subquery translation: replacing column {:?} with {:?} = old projections={:?} current projections={:?}", col, actual_column, projections, actual_projection);
                        // what is the projection ?
                        // we might have a random alias on top - SomeField, which does not exist in the table, and the current projection
                        if is_empty_deep_projection(projections) {
                            if is_empty_deep_projection(actual_projection) {
                                // both empty
                                new_columns.insert(actual_column.clone(), projections.clone());
                            } else {
                                // we now have projections
                                // SAFETY
                                new_columns.insert(actual_column.clone(), actual_projection.clone());
                            }
                        } else {
                            if is_empty_deep_projection(actual_projection) {
                                // previous projection not empty, current projection empty
                                new_columns.insert(actual_column.clone(), projections.clone());
                            } else {
                                // we now have projections
                                // MERGE ?
                                trace!(target: "deep", " >>>>>>>>>>>>>>>>>>>>>> WHAT TO DO projections={:?} current projections={:?}", projections, actual_projection);
                                new_columns.insert(actual_column.clone(), actual_projection.clone());
                            }
                        }
                        // the key for this column is in the map !
                        // merge_values_in_column_map(new_columns, actual_column.0, projections.clone())

                        // new_columns.insert(actual_column.0.clone(), actual_column.1.clone());
                    } else {
                        // copy the column
                        new_columns.insert(col.clone(), projections.clone());
                    }
                }
                new_cols_level.push(PlanWithDeepColumnMap {
                    plan: plan.clone(),
                    columns: new_columns,
                });
            }
            cols_level = new_cols_level;
            trace!(target: "deep", "PRE new cols_level after subquery translation: {:?}", &cols_level);
        }

        let current_pdc = PlanWithDeepColumnMap::from_plan(&plan);
        info!("CURR, Start Executing, Current PDC: {:?}", &current_pdc.columns);
        cols_level.push(current_pdc);
        trace!(target: "deep", "      cols_level with the current plan = {:?}", &cols_level);

        match plan {
            LogicalPlan::Projection(proj) => {
                let Projection {
                    expr, input, schema, ..
                } = proj;

                let mut new_proj_schema: Option<DFSchemaRef> = None;
                let mut new_proj_expr: Vec<Expr> = vec![];
                if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_PROJECTION_MERGING != 0 {
                    trace!(target: "deep", "  PROJMERGE projection merging: EXECUTING FLAG_ENABLE_PROJECTION_MERGING code path, cols_level = {:?}", &cols_level);
                    // trace!(target: "deep", "  PROJMERGE projection merging: EXECUTING FLAG_ENABLE_PROJECTION_MERGING expressions in current projection = {:?}", &expr);
                    let has_previous_projection = (0..cols_level.len() - 1).any(|idx| {
                        matches!(cols_level[idx].plan.deref(), &LogicalPlan::Projection(_))
                            || matches!(cols_level[idx].plan.deref(), &LogicalPlan::SubqueryAlias(_))
                            || matches!(cols_level[idx].plan.deref(), &LogicalPlan::Aggregate(_))
                            || matches!(cols_level[idx].plan.deref(), &LogicalPlan::Join(_))
                    });
                    if has_previous_projection && cols_level.len() > 0 {
                        trace!(target: "deep", "PROJMERGE can proceed, have previous projections");
                        // this projection is a candidate, it "looks" like an inserted projection
                        // for each of the columns in this projection, try to find previous references to this column
                        // get the largest of these references and set it to this projection
                        let my_pdc = cols_level.last().unwrap();
                        let pdc_with_all_references = get_columns_referenced_in_plan(&cols_level.last().unwrap().plan, true);
                        let mut my_new_modified_columns: DeepColumnColumnMap = HashMap::new();
                        'col_iterate: for (my_col, my_deep_projections) in my_pdc.columns.iter() {
                            let all_deep_projections = pdc_with_all_references.get(my_col).unwrap();
                            trace!(target: "deep", "PROJMERGE searching previous projections for column {:?}, current projections={:?}, ALL={:?}", my_col, my_deep_projections, all_deep_projections);
                            for my_deep_projection in my_deep_projections.iter() {
                                // col + projection
                                if my_deep_projection == "*" {
                                    // we only do this if it's a top-level projection
                                    let previous_deep_projections = find_all_projections_for_column(my_col, &cols_level, 0, cols_level.len() - 1);
                                    trace!(target: "deep", "PROJMERGE projection merging found previous deep projections for column {:?}: {:?}", my_col, &previous_deep_projections);
                                    if previous_deep_projections.len() == 0 {
                                        // we don't specify this column in the previous projection, so we need to leave it
                                        continue 'col_iterate;
                                    }
                                    // we have previous deep projections
                                    trace!(target: "deep", "PROJMERGE rewrite projection for col {:?}: {:?}", my_col, previous_deep_projections);
                                    // change the hashmap entry at this level
                                    my_new_modified_columns.insert(my_col.clone(), previous_deep_projections);
                                }
                            }
                        }
                        // rewrite it
                        // we have found projections above this projection, try to minimize this projection with only what is needed above
                        if my_new_modified_columns.len() > 0 {
                            // remake the pdc, copy what is not changed
                            let mut my_new_pdc = PlanWithDeepColumnMap {
                                plan: my_pdc.plan.clone(),
                                columns: HashMap::new(),
                            };
                            for (my_pdc_col, my_pdc_deep_projection) in my_pdc.columns.iter() {
                                if let Some(new_deep_projection) = my_new_modified_columns.get(my_pdc_col) {
                                    my_new_pdc.columns.insert(my_pdc_col.clone(), new_deep_projection.clone());
                                } else {
                                    my_new_pdc.columns.insert(my_pdc_col.clone(), my_pdc_deep_projection.clone());
                                }
                            }
                            trace!(target: "deep", "PROJMERGE WE HAVE NEW DEEP PROJECTIONS FROM PREVIOUS PLANS, final deep projections: {:?}", &my_new_pdc.columns);
                            let _ = cols_level.pop();
                            cols_level.push(my_new_pdc);
                        }

                        // we looked at previous projections, what about myself ?
                        // In this projection, is there another

                        // rewrite the projection expr accordingly
                        // in our new PDC, we might have eliminated some expressions for plain columns
                        // these expressions are added spuriously somewhere in the OptimizeProjection plan, but they are not actually used
                        // we try to identify and remove these

                        trace!(target:"deep", "PROJMERGE Step 2 - try to rewrite projections from this place only");
                        // In a Projection the exprs and schema have the same number of columns.
                        // If we eliminate a column, we need to eliminate the field from the schema
                        let mut new_projection_indices: Vec<usize> = vec![];
                        for (expr_index, single_expr) in expr.iter().enumerate() {
                            let mut should_discard = false;
                            // Is this expression a single column ?
                            if let Expr::Column(single_expr_column) = single_expr {
                                // Is it in the deep projection map, and does it have a projection that is deep ?
                                if let Some(single_expr_column_deep_projection) = &cols_level.last().unwrap().columns.get(single_expr_column) {
                                    if is_empty_deep_projection(single_expr_column_deep_projection) {
                                        trace!(target: "deep", "PROJMERGE Testing possible column expression elimination: {:?}", single_expr_column);
                                        // we can eliminate this expression ????????? ? ????

                                        let previous_projections = find_all_projections_for_column(
                                            single_expr_column, &cols_level, 0, cols_level.len() - 1,
                                        );
                                        trace!(target: "deep", "PROJMERGE         previous projections: {:?}", previous_projections);
                                        if previous_projections.len() == 0 && options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_PROJECTION_EXPR_DISCARD != 0 {
                                            trace!(target: "deep", "PROJMERGE     no previous references to this column, can eliminate !!!");
                                            should_discard = true;
                                        } else {
                                            // we have previous projections, can they be satisfied by the OTHER projections of this column ????
                                            trace!(target: "deep", "PROJMERGE     checking other expressions in this projection");
                                            let mut checked_previous_projections: Vec<String> = vec![];
                                            for previous_projection in previous_projections.iter() {
                                                for (other_index_in_this_projection, other_expr_in_this_projection) in expr.iter().enumerate() {
                                                    if other_index_in_this_projection == expr_index {
                                                        continue;
                                                    }
                                                    let other_column_map_in_this_projection = expr_to_deep_columns(other_expr_in_this_projection);
                                                    for (col, deep_projections_in_this_projection) in other_column_map_in_this_projection.iter() {
                                                        if col == single_expr_column {
                                                            // this expression is linked to this column
                                                            // can we extract previous_projection from this list ?
                                                            if projection_is_included(previous_projection, deep_projections_in_this_projection) {
                                                                checked_previous_projections.push(previous_projection.clone());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            trace!(target: "deep", "PROJMERGE     checking other expressions in this projection for column {:?} = checked = {:?}, previous = {:?}", single_expr_column, checked_previous_projections, previous_projections);
                                            if checked_previous_projections.len() == previous_projections.len() && (options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_PROJECTION_EXPR_DISCARD != 0) {
                                                // trace!(target: "deep", "Discarding projection expression {:?}")
                                                should_discard = true;
                                            }
                                        }
                                    }
                                }
                            }
                            if !should_discard {
                                new_proj_expr.push(single_expr.clone());
                            }
                            new_projection_indices.push(expr_index);
                        }
                        trace!(target: "deep", "PROJMERGE new projection exprs: {:?}", new_proj_expr);
                        // remake the projection
                        if (new_proj_expr.len() != expr.len()) {
                            // rewrite the projection again


                            let new_deep_column_map = get_columns_referenced_in_exprs(&new_proj_expr, false);
                            let last_my_pdc = cols_level.last().unwrap().clone();
                            let new_my_pdc = PlanWithDeepColumnMap {
                                plan: last_my_pdc.plan.clone(),
                                columns: new_deep_column_map,
                            };
                            trace!(target: "deep", "PROJMERGE REDOING my PDC: {:?}", new_my_pdc);
                            cols_level.pop();
                            // we have to remake the PDC
                            cols_level.push(new_my_pdc);
                            new_proj_expr = expr.clone();
                        }

                        // rewrite the projection schema accordingly !!!!
                        // ADR: this shouldn't be needed. But it is, there is an extra aggregation step that verifies that the physical schema is the same as the logical schema
                        // which means they need to match
                        // the option is options.execution.skip_physical_aggregate_schema_check - see the code for that
                        // Also, we could CHANGE the actual verification to check that the schema can be rewritten - deep functions, instead of checking for exact equality
                        // the code WILL work when we
                        // - remove the check by disabling the option
                        // - change the check so that it verifies that the schemas can be casted / rewritten, instead of being equal

                        // we have eliminated some exprs, we need to eliminate the same things from the current schema
                        let mut new_proj_field_qualifiers: Vec<Option<TableReference>> = vec![];
                        let mut new_proj_fields: Vec<Field> = vec![];
                        let mut all_projection_indices: Vec<usize> = vec![];
                        for (counter, index_in_current_projection) in new_projection_indices.iter().enumerate() {
                            let (table_reference, field) = schema.qualified_field(*index_in_current_projection);
                            new_proj_field_qualifiers.push(table_reference.cloned());
                            new_proj_fields.push(field.clone());
                            all_projection_indices.push(counter);
                        }
                        // this is a temporary schema WITHOUT the top-level expressions that we have eliminated
                        let work_proj_schema = Arc::new(Schema::new(new_proj_fields));

                        let corrected_deep_projections = fix_deep_projection_according_to_table_schema_for_plain_schema(&cols_level.last().unwrap().columns, &work_proj_schema);
                        trace!(target: "deep", "PROJMERGE corrected deep projections: {:?}", &corrected_deep_projections);
                        let deep_projection = transform_column_deep_projection_map_to_usize_index_type_and_clean_up_stars_for_plain_schema(
                            &corrected_deep_projections,
                            &work_proj_schema,
                        );
                        trace!(target: "deep", "PROJ REWRITE SCHEMA: projection={:?} deep_projection={:?}", &all_projection_indices, &deep_projection);
                        let new_proj_inner_schema = try_rewrite_schema_opt(work_proj_schema.clone(), Some(&all_projection_indices), Some(&deep_projection))?;

                        // trace!(target: "deep", "PROJ REWRITTEN SCHEMA: {:#?}", new_proj_inner_schema);
                        // remake a DFSchema for the Projection
                        // zip the fields
                        let new_qualified_fields = new_proj_field_qualifiers
                            .into_iter()
                            .zip(new_proj_inner_schema.fields.iter().map(|f| f.clone()))
                            .collect::<Vec<(Option<TableReference>, FieldRef)>>();
                        let new_df_schema = Arc::new(DFSchema::new_with_metadata(
                            new_qualified_fields,
                            schema.metadata().clone(),
                        )?);
                        new_proj_schema = Some(new_df_schema);
                        // }
                    }
                }

                if let Some(new_proj_schema) = new_proj_schema {
                    let new_proj = Projection::try_new_with_schema(new_proj_expr, input, new_proj_schema);
                    Ok(Transformed::new(LogicalPlan::Projection(new_proj?), false, TreeNodeRecursion::Continue))
                } else {
                    Ok(Transformed::new(LogicalPlan::Projection(Projection::try_new(expr, input)?), false, TreeNodeRecursion::Continue))
                }
            }
            LogicalPlan::Filter(_) => Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue)),
            LogicalPlan::Window(_) => Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue)),
            LogicalPlan::Aggregate(_) => Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue)),
            LogicalPlan::Limit(_) => Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue)),
            LogicalPlan::Distinct(_) => Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue)),
            LogicalPlan::Sort(_) => Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue)),
            LogicalPlan::Repartition(_) => Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue)),

            LogicalPlan::Join(join) => {
                let Join {
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema: _,
                    null_equals_null,
                } = join;
                let new_left = deep_plan_transformer(left.as_ref().clone(), cols_level.clone(), options)?;
                let new_right = deep_plan_transformer(right.as_ref().clone(), cols_level.clone(), options)?;
                let is_transformed = new_left.transformed || new_right.transformed;
                let new_join_schema = Arc::new(build_join_schema(new_left.data.schema(), new_right.data.schema(), &join_type)?);
                return Ok(
                    Transformed::new(
                        LogicalPlan::Join(Join {
                            left: Arc::new(new_left.data),
                            right: Arc::new(new_right.data),
                            on,
                            filter,
                            join_type,
                            join_constraint,
                            schema: new_join_schema, // FIXME ?
                            null_equals_null,
                        }),
                        is_transformed,
                        TreeNodeRecursion::Jump,
                    )
                );
            }
            LogicalPlan::Union(union) => {
                let mut new_children: Vec<Arc<LogicalPlan>> = vec![];
                let Union {
                    inputs, schema
                } = union;
                let mut any_transformed = false;
                for input in inputs.iter() {
                    let tmp = deep_plan_transformer(input.as_ref().clone(), vec![], options)?;
                    any_transformed = any_transformed || tmp.transformed;
                    new_children.push(Arc::new(tmp.data));
                }
                return Ok(
                    Transformed::new(
                        LogicalPlan::Union(Union {
                            inputs: new_children,
                            schema,
                        }),
                        any_transformed,
                        TreeNodeRecursion::Jump,
                    )
                );
            }
            LogicalPlan::TableScan(table_scan) => {
                let TableScan {
                    table_name,
                    source,
                    projection,
                    projection_deep: _,
                    filters,
                    fetch,
                    projected_schema,
                } = table_scan;

                trace!(target: "deep", "SCAN TABLE SCAN: input cols_level= {:?}, projection={:?}, filters: {:?}", &cols_level, &projection, &filters);
                // can we actually do the deep merging ?
                // ADR: this no longer works after passing in stuff through SubqueryAlias
                let mut projections_before_indices: Vec<usize> = vec![];
                for (idx, x) in cols_level.iter().enumerate() {
                    if matches!(x.plan.as_ref(), LogicalPlan::Projection(_)) {
                        projections_before_indices.push(idx);
                    }
                }
                if projections_before_indices.len() == 0 && cols_from_above.len() == 0 {
                    return Ok(Transformed::new(LogicalPlan::TableScan(TableScan {
                        table_name,
                        source,
                        projection,
                        projection_deep: None,
                        projected_schema,
                        filters,
                        fetch,
                    }), true, TreeNodeRecursion::Continue));
                }

                let df_schema = Arc::new(DFSchema::try_from(source.schema())?);
                let cols_level_as_simple_map = convert_plans_with_deep_column_to_columns_map(&cols_level);
                // get only the expressions for me
                let columns_for_this_scan = filter_expressions_for_table(&cols_level_as_simple_map, table_name.table().to_string());
                trace!(target: "deep", "SCAN TABLE SCAN COLUMNS FOR ME 1({}): {:?}", table_name.table(), columns_for_this_scan);
                // compact all the separate levels to a single map
                let single_columns_for_this_scan = compact_list_of_column_maps_to_single_map(&columns_for_this_scan);
                trace!(target: "deep", "SCAN TABLE SCAN COLUMNS FOR ME 2({}): {:?}", table_name.table(), single_columns_for_this_scan);
                // fix deep projections - we might have things that look like struct access, but they are map access
                let fixed_single_columns_for_this_scan = fix_deep_projection_according_to_table_schema(&single_columns_for_this_scan, &df_schema);
                trace!(target: "deep", "SCAN TABLE SCAN COLUMNS FOR ME 3({}): {:?}", table_name.table(), fixed_single_columns_for_this_scan);
                // transform to actual deep projection - use the column index instead of the column, and replace ["*"] with empty
                let projection_deep = transform_column_deep_projection_map_to_usize_index_type_and_clean_up_stars(&fixed_single_columns_for_this_scan, &df_schema);
                trace!(target: "deep", "SCAN Rewriting deep projections for table {}: {:?} {:?}", table_name.table(), projection.clone(), projection_deep);
                let reprojected_schema = reproject_for_deep_schema(
                    &table_name,
                    projection.clone(),
                    Some(projection_deep.clone()),
                    &projected_schema,
                    &source.schema(),
                )?;
                trace!(target: "deep", "SCAN reprojected schema: {:#?}", reprojected_schema.inner());
                Ok(Transformed::new(LogicalPlan::TableScan(TableScan {
                    table_name,
                    source,
                    projection,
                    projection_deep: Some(projection_deep),
                    projected_schema: reprojected_schema,
                    filters,
                    fetch,
                }), true, TreeNodeRecursion::Continue))
            }
            // we don't know what to do with an empty relation
            LogicalPlan::EmptyRelation(_) => return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump)),
            LogicalPlan::Subquery(sq) => {
                // ADR: we cannot have Subquery plans after optimization, they're all SubqueryAlias
                let Subquery {
                    subquery,
                    outer_ref_columns
                } = sq;
                let subquery_result = deep_plan_transformer(subquery.as_ref().clone(), vec![], options)?;
                let transformed = subquery_result.transformed;
                Ok(Transformed::new(LogicalPlan::Subquery(Subquery {
                    subquery: Arc::new(subquery_result.data),
                    outer_ref_columns,
                }), transformed, TreeNodeRecursion::Jump))

                // unreachable!()
            }
            LogicalPlan::SubqueryAlias(sqa) => {
                let SubqueryAlias {
                    input,
                    alias,
                    schema: _,
                    ..
                } = sqa;
                let mut translated_pdcs: Vec<PlanWithDeepColumnMap> = vec![];
                if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_SUBQUERY_TRANSLATION != 0 {
                    trace!(target: "deep", "SQA subquery translation: activating subquery translation, for alias {}", &alias);
                    trace!(target: "deep", "SQA subquery translation: activating subquery translation CHECKING {:?}", &cols_level);
                    for pdc in cols_level.iter() {
                        let mut translated_pdc = PlanWithDeepColumnMap {
                            plan: pdc.plan.clone(),
                            columns: HashMap::new(),
                        };
                        for (col, projections) in pdc.columns.iter() {
                            if let Some(col_table_reference) = &col.relation {
                                if col_table_reference == &alias {
                                    let mut new_col = col.clone();
                                    new_col.relation = None;
                                    // don't add the projection if it's empty ????? FIXME
                                    // if !is_empty_deep_projection(projections) {
                                    translated_pdc.columns.insert(new_col, projections.clone());
                                    // }
                                }
                            }
                        }
                        if translated_pdc.columns.len() > 0 {
                            translated_pdcs.push(translated_pdc);
                        }
                    }
                    trace!(target: "deep", "SQA subquery translation: translated pdcs = {:?}", translated_pdcs);
                }

                let sq_input_result = deep_plan_transformer(input.as_ref().clone(), translated_pdcs, options)?;
                let input_transformed = sq_input_result.transformed;
                let new_sqa = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(sq_input_result.data), alias)?);
                Ok(Transformed::new(new_sqa, input_transformed, TreeNodeRecursion::Jump))
            }
            // ignore DDL like statements
            LogicalPlan::Statement(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Copy(_) => return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump)),
            LogicalPlan::Unnest(_) => { // WTF
                Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue))
            }
            LogicalPlan::RecursiveQuery(rq) => {
                // FIXME: not tested
                // ADR: this makes some tests break
                // let recursive_result = deep_plan_transformer(rq.recursive_term.as_ref().clone(), vec![])?;
                // Ok(Transformed::new(recursive_result.data, recursive_result.transformed, TreeNodeRecursion::Jump))
                Ok(Transformed::new(LogicalPlan::RecursiveQuery(rq), false, TreeNodeRecursion::Continue))
            }
        }
    })
}

pub fn reproject_for_deep_schema(
    table_name: &TableReference,
    projection: Option<Vec<usize>>,
    projection_deep: Option<DeepColumnIndexMap>,
    projected_schema: &DFSchemaRef,
    source_schema: &SchemaRef,
) -> Result<DFSchemaRef> {
    if projection.is_some() && projection_deep.is_some() {
        let projection_clone = projection.unwrap().clone();
        let projection_deep_clone = projection_deep.unwrap().clone();
        let mut new_projection_deep: DeepColumnIndexMap = HashMap::new();
        projection_clone.iter().enumerate().for_each(|(ip, elp)| {
            let empty_vec: Vec<String> = vec![];
            let deep = projection_deep_clone.get(elp).or(Some(&empty_vec)).unwrap();
            new_projection_deep.insert(ip, deep.clone());
        });
        let new_projection = (0..projection_clone.len()).collect::<Vec<usize>>();
        let inner_projected_schema = projected_schema.inner().clone();
        let new_inner_projected_schema = rewrite_schema(
            inner_projected_schema,
            &new_projection,
            &new_projection_deep,
        );
        let new_fields = new_inner_projected_schema
            .fields()
            .iter()
            .map(|fi| (Some(table_name.clone()), fi.clone()))
            .collect::<Vec<(Option<TableReference>, Arc<Field>)>>();
        let mut new_projected_schema_df =
            DFSchema::new_with_metadata(new_fields, source_schema.metadata.clone())?;
        new_projected_schema_df = new_projected_schema_df.with_functional_dependencies(
            projected_schema.functional_dependencies().clone(),
        )?;
        let new_projected_schema = Arc::new(new_projected_schema_df);
        Ok(new_projected_schema)
    } else {
        Ok(projected_schema.clone())
    }
}

pub fn projection_is_included(
    prev_projection: &String,
    projections: &Vec<String>,
) -> bool {
    projections
        .iter()
        .any(|projection| prev_projection.starts_with(projection))
}

pub fn get_columns_referenced_in_plan(
    plan: &LogicalPlan,
    keep_all: bool,
) -> DeepColumnColumnMap {
    let expressions = plan.expressions();
    let mut deep_column_map: DeepColumnColumnMap =
        get_columns_referenced_in_exprs(&expressions, keep_all);
    trace!(target: "deep", "get_columns_referenced_in_plan columns : {:?}", &deep_column_map);
    deep_column_map
}

pub fn get_columns_referenced_in_exprs(
    exprs: &Vec<Expr>,
    keep_all: bool,
) -> DeepColumnColumnMap {
    let mut deep_column_map: DeepColumnColumnMap = HashMap::new();
    for expr in exprs.iter() {
        let tmp = expr_to_deep_columns(expr);
        for (k, vs) in tmp.iter() {
            if k.relation.is_none() {
                continue;
            }
            if keep_all {
                append_values_in_column_map(&mut deep_column_map, &k, vs);
            } else {
                merge_values_in_column_map(&mut deep_column_map, &k, vs);
            }
        }
    }
    // trace!(target: "deep", "get_columns_referenced_in_plan columns : {:?}", &deep_column_map);
    deep_column_map
}

pub fn convert_plans_with_deep_column_to_columns_map(
    input: &Vec<PlanWithDeepColumnMap>,
) -> Vec<DeepColumnColumnMap> {
    input.iter().map(|pdc| pdc.columns.clone()).collect()
}

pub fn filter_expressions_for_table(
    input: &Vec<DeepColumnColumnMap>,
    table_name: String,
) -> Vec<DeepColumnColumnMap> {
    let mut output: Vec<DeepColumnColumnMap> = vec![];
    for (_level, column_map) in input.iter().enumerate() {
        let mut level_map: DeepColumnColumnMap = HashMap::new();
        for (column, deep) in column_map.iter() {
            if let Some(table_reference) = &column.relation {
                match table_reference {
                    TableReference::Bare { table, .. } => {
                        if table.to_string() == table_name {
                            level_map.insert(column.clone(), deep.clone());
                        }
                    }
                    TableReference::Partial { table, .. } => {
                        if table.to_string() == table_name {
                            level_map.insert(column.clone(), deep.clone());
                        }
                    }
                    TableReference::Full { table, .. } => {
                        if table.to_string() == table_name {
                            level_map.insert(column.clone(), deep.clone());
                        }
                    }
                }
            }
        }
        if level_map.len() > 0 {
            output.push(level_map);
        }
    }
    output
}

pub fn compact_list_of_column_maps_to_single_map(
    input: &Vec<DeepColumnColumnMap>,
) -> DeepColumnColumnMap {
    let mut output: DeepColumnColumnMap = HashMap::new();
    for (_level, column_map) in input.iter().enumerate() {
        for (column, deep) in column_map.iter() {
            merge_values_in_column_map(&mut output, column, deep);
        }
    }
    output
}

pub fn transform_column_deep_projection_map_to_usize_index_type_and_clean_up_stars(
    input: &DeepColumnColumnMap,
    schema: &DFSchemaRef,
) -> DeepColumnIndexMap {
    let mut output: DeepColumnIndexMap = HashMap::new();
    for (col, values) in input.iter() {
        // This is needed because of the way DF checks qualified columns
        let col_to_check = Column::new_unqualified(col.name.clone());
        if let Some(col_idx_in_schema) = schema.maybe_index_of_column(&col_to_check) {
            if is_empty_deep_projection(values) {
                output.insert(col_idx_in_schema, vec![]);
            } else {
                output.insert(col_idx_in_schema, values.clone());
            }
        }
    }
    output
}

pub fn transform_column_deep_projection_map_to_usize_index_type_and_clean_up_stars_for_plain_schema(
    input: &DeepColumnColumnMap,
    schema: &SchemaRef,
) -> DeepColumnIndexMap {
    let mut output: DeepColumnIndexMap = HashMap::new();
    for (col, values) in input.iter() {
        // This is needed because of the way DF checks qualified columns
        let name = col.name();
        if let Ok(col_idx_in_schema) = schema.index_of(name) {
            if is_empty_deep_projection(values) {
                output.insert(col_idx_in_schema, vec![]);
            } else {
                output.insert(col_idx_in_schema, values.clone());
            }
        }
    }
    output
}

pub fn fix_deep_projection_according_to_table_schema_for_plain_schema(
    input: &DeepColumnColumnMap,
    schema: &SchemaRef,
) -> DeepColumnColumnMap {
    trace!(target: "deep", "fix_deep_projection_according_to_table_schema_for_plain_schema SCHEMA: {:#?}", schema);

    let mut output: DeepColumnColumnMap = HashMap::new();
    for (col, deep_projection_specifiers) in input.iter() {
        let name = col.name();
        trace!(target: "deep", "fix_deep_projection_according_to_table_schema_for_plain_schema checking col={:?}, deep_projection_specifiers={:?}", col, deep_projection_specifiers);
        if let Ok(col_idx_in_schema) = schema.index_of(&name) {
            trace!(target: "deep", "fix_deep_projection_according_to_table_schema_for_plain_schema COL IDX {}", col_idx_in_schema);
            // get the rest and see whether the column type
            // we iterate through the rest specifiers and we fix them
            // that is, if we see something that looks like a get field, but we know the field in the schema
            // is a map, that means that we need to replace it with *
            // map_field['val'], projection_rest = ["val"] => projection_rest=["*"]
            let mut fixed_deep_projection_specifiers: Vec<String> = vec![];
            for projection_specifier in deep_projection_specifiers {
                trace!(target: "deep", "fix_deep_projection_according_to_table_schema_for_plain_schema checking projection specifier {}", projection_specifier);
                // ADR: FIXME
                let fixed_projection_specifier_pieces = fix_possible_field_accesses(
                    &schema,
                    col_idx_in_schema,
                    projection_specifier,
                )
                .unwrap();
                let fixed_projection_specifier =
                    fixed_projection_specifier_pieces.join(".");
                fixed_deep_projection_specifiers.push(fixed_projection_specifier);
            }
            output.insert(col.clone(), fixed_deep_projection_specifiers);
        }
    }
    // trace!(target: "deep", "fix_deep_projection_according_to_table_schema_for_plain_schema RESULT={:?}", &output);

    output
}

pub fn fix_deep_projection_according_to_table_schema(
    input: &DeepColumnColumnMap,
    schema: &DFSchemaRef,
) -> DeepColumnColumnMap {
    // trace!(target: "deep", "fix_deep_projection_according_to_table_schema SCHEMA: {:?}", schema);

    let mut output: DeepColumnColumnMap = HashMap::new();
    for (col, deep_projection_specifiers) in input {
        // trace!(target: "deep", "fix_deep_projection_according_to_table_schema checking col={:?}, deep_projection_specifiers={:?}", col, deep_projection_specifiers);
        let col_to_check = Column::new_unqualified(col.name.clone());
        if let Some(col_idx_in_schema) = schema.maybe_index_of_column(&col_to_check) {
            // trace!(target: "deep", "fix_deep_projection_according_to_table_schema COL IDX {}", col_idx_in_schema);
            // get the rest and see whether the column type
            // we iterate through the rest specifiers and we fix them
            // that is, if we see something that looks like a get field, but we know the field in the schema
            // is a map, that means that we need to replace it with *
            // map_field['val'], projection_rest = ["val"] => projection_rest=["*"]
            let mut fixed_deep_projection_specifiers: Vec<String> = vec![];
            for projection_specifier in deep_projection_specifiers {
                // trace!(target: "deep", "fix_deep_projection_according_to_table_schema checking projection specifier {}", projection_specifier);
                // ADR: FIXME
                let fixed_projection_specifier_pieces = fix_possible_field_accesses(
                    &schema.inner(),
                    col_idx_in_schema,
                    projection_specifier,
                )
                .unwrap();
                let fixed_projection_specifier =
                    fixed_projection_specifier_pieces.join(".");
                fixed_deep_projection_specifiers.push(fixed_projection_specifier);
            }
            output.insert(col.clone(), fixed_deep_projection_specifiers);
        }
    }
    // trace!(target: "deep", "fix_deep_projection_according_to_table_schema RESULT={:?}", &output);

    output
}

pub fn fix_possible_field_accesses(
    schema: &SchemaRef,
    field_idx: usize,
    deep_projection_specifier: &String,
) -> Result<Vec<String>> {
    // trace!(target: "deep", "fix_possible_field_accesses {} {}", field_idx, deep_projection_specifier);
    if deep_projection_specifier == "*" {
        return Ok(vec!["*".to_string()]);
    }

    let deep_projection_specifier_pieces = deep_projection_specifier
        .clone()
        .split(".")
        .map(|x| x.to_string())
        .collect::<Vec<String>>();
    let mut field = Arc::new(schema.field(field_idx).clone());
    let mut index = 0 as usize;
    let mut out: Vec<String> = vec![];
    'outer: while index < deep_projection_specifier_pieces.len() {
        // trace!(target: "deep", "fix_possible_field_accesses  at index: {}, value: {}, field: {:?}", index, deep_projection_specifier_pieces[index], field);
        match field.clone().data_type() {
            // scalars
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Dictionary(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::RunEndEncoded(_, _) => {
                // this is a scalar field
                // add the field to the projection, break the loop
                out.push(deep_projection_specifier_pieces[index].clone());
                break;
            }
            DataType::Union(_, _) => {
                // FIXME @HStack
                // don't know what to do here
                break;
            }
            DataType::Struct(inner_struct) => {
                for f in inner_struct.iter() {
                    if f.name() == &deep_projection_specifier_pieces[index] {
                        // we found it, add it to the output, continue the iteration
                        // trace!(target: "deep", "fix_possible_field_accesses FOUND FIELD, continuing iteration");
                        out.push(deep_projection_specifier_pieces[index].clone());
                        index += 1;
                        field = f.clone();
                        continue 'outer;
                    }
                }
                // if we got here, that means we haven't found the field with the name
                return Err(DataFusionError::Internal(format!(
                    "Wrong projection, tried to get {}, but couldn't find {}",
                    deep_projection_specifier.clone(),
                    deep_projection_specifier_pieces[index]
                )));
            }
            DataType::Map(inner_map, _) => {
                match inner_map.data_type() {
                    DataType::Struct(inner_map_struct) => {
                        field = inner_map_struct[1].clone();
                        // we are at a map, just insert a * and restart the iteration from the same place
                        out.push("*".to_string());
                        // if deep_projection_specifier_pieces[index] == "*" {
                        index += 1;
                        // }
                        continue 'outer;
                    }
                    _ => {
                        return Err(DataFusionError::Internal(String::from(
                            "Invalid inner map type",
                        )));
                    }
                }
            }
            DataType::List(inner)
            | DataType::ListView(inner)
            | DataType::FixedSizeList(inner, _)
            | DataType::LargeList(inner)
            | DataType::LargeListView(inner) => {
                field = inner.clone();
                out.push("*".to_string());
                // we might get here from a direct access or an indexed access
                if deep_projection_specifier_pieces[index] == "*" {
                    index += 1;
                }
                continue 'outer;
                // match inner.data_type() {
                //     // only if it's a list that contains a struct, we can jump and check the inner struct
                //     DataType::Struct(inner_inner_struct) => {
                //         field = inner.clone();
                //         // we are at a map, just insert a * and restart the iteration from the same place
                //         out.push("*".to_string());
                //         continue;
                //     }
                //     _ => {
                //         let new_field = inner.clone();
                //         (true, true, Some(new_field))
                //     }
                // }
            }
        }
    }

    Ok(out)
}

pub fn merge_values_in_aliases(
    map: &mut HashMap<String, (Column, Vec<String>)>,
    col: &Column,
    new_values: &Vec<String>,
) {
    for new_value in new_values.iter() {
        merge_value_in_aliases(map, col, new_value);
    }
}

pub fn merge_value_in_aliases(
    map: &mut HashMap<String, (Column, Vec<String>)>,
    col: &Column,
    new_value: &String,
) {
    assert_ne!(new_value, ""); // the value should be something, we don't use empty strings anymore
    let col_name = col.name();
    if map.contains_key(col_name) {
        let (_, current_values) = map.get(col_name).unwrap();
        let new_values = merge_value_in_projections_vector(&current_values, &new_value);
        map.insert(col_name.to_string(), (col.clone(), new_values));
    } else {
        map.insert(col_name.to_string(), (col.clone(), vec![new_value.clone()]));
    }
}

pub fn append_values_in_column_map(
    map: &mut DeepColumnColumnMap,
    col: &Column,
    new_values: &Vec<String>,
) {
    for new_value in new_values {
        append_value_in_column_map(map, col, new_value);
    }
}

/// Appends a value for a column to a deep column map
/// - this can handle if we add a projection that is already included (adding a.b.c when we already have a)
pub fn append_value_in_column_map(
    map: &mut DeepColumnColumnMap,
    col: &Column,
    new_value: &String,
) {
    assert_ne!(new_value, ""); // the value should be something, we don't use empty strings anymore
    if map.contains_key(col) {
        let mut current_values = map.get(col).unwrap().clone();
        current_values.push(new_value.clone());
        map.insert(col.clone(), current_values);
    } else {
        map.insert(col.clone(), vec![new_value.clone()]);
    }
}

pub fn merge_values_in_column_map(
    map: &mut DeepColumnColumnMap,
    col: &Column,
    new_values: &Vec<String>,
) {
    for new_value in new_values {
        merge_value_in_column_map(map, col, new_value);
    }
}

/// Appends a value for a column to a deep column map
/// - this can handle if we add a projection that is already included (adding a.b.c when we already have a)
pub fn merge_value_in_column_map(
    map: &mut DeepColumnColumnMap,
    col: &Column,
    new_value: &String,
) {
    assert_ne!(new_value, ""); // the value should be something, we don't use empty strings anymore
    if map.contains_key(col) {
        let current_values = map.get(col).unwrap();
        let new_values = merge_value_in_projections_vector(&current_values, &new_value);
        map.insert(col.clone(), new_values);
    } else {
        map.insert(col.clone(), vec![new_value.clone()]);
    }
}

pub fn merge_value_in_projections_vector(
    projections: &Vec<String>,
    new_projection: &String,
) -> Vec<String> {
    if new_projection == "*" {
        // replace the existing values
        let out = vec!["*".to_string()];
        return out;
    } else if is_empty_deep_projection(projections) {
        // we already read all from above, ignore anything that comes later
        return projections.clone();
    } else {
        // we actually add something

        // do we already have it ? then return, don't modify the map
        for projection in projections.iter() {
            if projection == new_projection {
                return projections.clone();
            }
        }
        // a.b.c, but we already have a, or a.b
        for projection in projections.iter() {
            if new_projection.starts_with(projection) {
                return projections.clone();
            }
        }
        let mut values = projections.clone();
        values.push(new_projection.clone());
        return values;
    }
}

pub fn find_all_projections_for_column(
    needle: &Column,
    haystack: &Vec<PlanWithDeepColumnMap>,
    start: usize,
    end: usize,
) -> Vec<String> {
    let mut end = end;
    if start == end {
        end += 1;
    }
    let mut output: DeepColumnColumnMap = HashMap::new();
    output.insert(needle.clone(), vec![]);
    for idx in start..end {
        let pdc = haystack.get(idx).unwrap();
        if let Some(projections_for_col) = pdc.columns.get(needle) {
            merge_values_in_column_map(&mut output, needle, projections_for_col);
        }
    }
    output.get(needle).unwrap().clone()
}

/// extracts a DeepColumnMap from an expression
/// the map has for each column a vector of strings representing deep projections inside the column
pub fn expr_to_deep_columns(expr: &Expr) -> DeepColumnColumnMap {
    let mut accum: DeepColumnColumnMap = HashMap::new();
    let mut field_accum: VecDeque<String> = VecDeque::new();
    let mut in_make_struct_call: bool = false;
    let mut in_other_literal_call: bool = false;
    let _ = expr
        .apply(|expr| {
            match expr {
                Expr::Column(qc) => {
                    // @HStack FIXME: ADR: we should have a test case
                    // ignore deep columns if we have a in_make_struct_call
                    // case: struct(a, b, c)['col'] - we were getting 'col' in the accum stack
                    // FIXME Will this work for struct(get_field(a, 'substruct'))['col'] ?????
                    if in_make_struct_call || in_other_literal_call {
                        field_accum.clear()
                    }
                    // at the end, unwind the field_accum and push all to accum
                    let mut tmp: Vec<String> = vec![];
                    // if we didn't just save a "*" - which means the entire column
                    if !(field_accum.len() == 1 && field_accum.get(0).unwrap() == "*") {
                        for f in field_accum.iter().rev() {
                            tmp.push(f.to_owned());
                        }
                    }
                    field_accum.clear();
                    if tmp.len() == 0 {
                        // entire column
                        append_column::<Column>(&mut accum, qc, vec!["*".to_string()]);
                    } else {
                        append_column::<Column>(&mut accum, qc, tmp);
                    }
                }
                Expr::ScalarFunction(sf) => {
                    // TODO what about maps ? what's the operator
                    match sf.name() {
                        "get_field" => {
                            // get field, append the second argument to the stack and continue
                            match sf.args[1].clone() {
                                Expr::Literal(lit_expr) => match lit_expr {
                                    ScalarValue::Utf8(str) => {
                                        let tmp = str.unwrap();
                                        field_accum.push_back(tmp);
                                    }
                                    _ => {
                                        error!(
                                            "Can't handle expression 1 {:?}",
                                            sf.args[1]
                                        );
                                        in_other_literal_call = true
                                    }
                                },
                                _ => {
                                    error!("Can't handle expression 2 {:?}", sf.args[1]);
                                    // panic!()
                                }
                            };
                            //
                            // let literal_expr: String = match sf.args[1].clone() {
                            //     Expr::Literal(lit_expr) => match lit_expr {
                            //         ScalarValue::Utf8(str) => str.unwrap(),
                            //         _ => {
                            //             error!(
                            //                 "Can't handle expression 1 {:?}",
                            //                 sf.args[1]
                            //             );
                            //             in_other_literal_call = true
                            //             // panic!()
                            //         }
                            //     },
                            //     _ => {
                            //         error!("Can't handle expression 2 {:?}", sf.args[1]);
                            //         panic!()
                            //     }
                            // };
                            // field_accum.push_back(literal_expr);
                        }
                        "array_element" => {
                            // We don't have the schema, but when splatting the column, we need to actually push the list inner field name here
                            field_accum.push_back("*".to_owned());
                        }
                        "struct" => {
                            in_make_struct_call = true;
                        }
                        _ => {}
                    }
                }
                Expr::Unnest(_)
                | Expr::ScalarVariable(_, _)
                | Expr::Alias(_)
                | Expr::Literal(_)
                | Expr::BinaryExpr { .. }
                | Expr::Like { .. }
                | Expr::SimilarTo { .. }
                | Expr::Not(_)
                | Expr::IsNotNull(_)
                | Expr::IsNull(_)
                | Expr::IsTrue(_)
                | Expr::IsFalse(_)
                | Expr::IsUnknown(_)
                | Expr::IsNotTrue(_)
                | Expr::IsNotFalse(_)
                | Expr::IsNotUnknown(_)
                | Expr::Negative(_)
                | Expr::Between { .. }
                | Expr::Case { .. }
                | Expr::Cast { .. }
                | Expr::TryCast { .. }
                | Expr::WindowFunction { .. }
                | Expr::AggregateFunction { .. }
                | Expr::GroupingSet(_)
                | Expr::InList { .. }
                | Expr::Exists { .. }
                | Expr::InSubquery(_)
                | Expr::ScalarSubquery(_)
                | Expr::Placeholder(_)
                | Expr::OuterReferenceColumn { .. } => {}
                Expr::Wildcard { .. } => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map(|_| ());
    accum
}

pub fn append_column<T>(acc: &mut HashMap<T, Vec<String>>, column: &T, rest: Vec<String>)
where
    T: Debug + Clone + Eq + Hash,
{
    let final_name = rest.join(".");
    match acc.get_mut(column) {
        None => {
            let column_clone = column.clone();
            if rest.len() > 0 {
                acc.insert(column_clone, vec![final_name]);
            } else {
                acc.insert(column_clone, vec![]);
            }
        }
        Some(cc) => {
            if cc.len() == 0 {
                // we already had this column in full
            } else {
                if rest.len() > 0 {
                    if !cc.contains(&final_name) {
                        cc.push(final_name);
                    }
                } else {
                    // we are getting the entire column, and we already had something
                    // we should delete everything
                    cc.clear();
                }
            }
        }
    }
}

/// returns true if all expressions in the list are
/// - aliases of
/// - columns, literals
/// - or, calls to get_field function
pub fn are_exprs_plain(exprs: &Vec<Expr>) -> bool {
    for pexpr in exprs.iter() {
        match pexpr {
            Expr::Alias(Alias {
                expr: alias_inner_expr,
                ..
            }) => match alias_inner_expr.as_ref() {
                Expr::Column(_) => {}
                Expr::Literal(_) => {}
                Expr::ScalarFunction(ScalarFunction { func, args: _args }) => {
                    if func.name() != "get_field" {
                        return false;
                    }
                }
                _ => {
                    return false;
                }
            },
            Expr::Column(_) => {}
            Expr::Literal(_) => {}
            Expr::ScalarFunction(ScalarFunction { func, args: _args }) => {
                if func.name() != "get_field" {
                    return false;
                }
            }
            _ => {
                return false;
            }
        }
    }
    true
}
