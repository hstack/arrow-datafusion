use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
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
use log::{info, trace, warn};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
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
        info!("optimize_projections_deep: plan={}", plan.display_indent());
        let options = config.options();
        if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE != 0 {
            let new_plan_transformed = deep_plan_transformer(plan, vec![], options)?;
            if new_plan_transformed.transformed {
                let new_plan = new_plan_transformed.data;
                let new_plan = new_plan.transform_up(|p| {
                    // ADR: just doing recompute schema for all breaks push_down_filter::tests::multi_combined_filter_exact
                    match p {
                        LogicalPlan::Window(_) => {
                            Ok(Transformed::yes(p.recompute_schema()?))
                        }
                        LogicalPlan::Aggregate(_) => {
                            Ok(Transformed::yes(p.recompute_schema()?))
                        }
                        _ => Ok(Transformed::no(p.clone())),
                    }
                    // Ok(Transformed::yes(p.recompute_schema()?))
                })?;
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
            columns: get_columns_referenced_in_plan(input),
        }
    }
}

impl Debug for PlanWithDeepColumnMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{} {:?}", get_plan_name(&self.plan), self.columns).as_str())
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
    plan.transform_down(|plan| {
        info!(
            ">>>>>>>>>>>>>>\ndeep_plan_transformer working on plan={}, cols_from_above ={:?}",
            &plan.display(), &cols_from_above
        );
        if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_SUBQUERY_TRANSLATION != 0 {
            // try to find aliases in current plan
            // for each alias
            // try to look at above projections where the table is empty.
            // if the column name is the same as the name of the alias, we need to change that pdc
            // the key is the alias, the column is the new column
            let mut aliases_in_current_plan: HashMap<String, Column> = HashMap::new();
            if matches!(plan, LogicalPlan::TableScan(_)) {
                // all the null ones are for me !
                // a table scan has no aliases, we are at the end
                let LogicalPlan::TableScan(TableScan {
                    table_name, ..
                }) = &plan else {unreachable!()};
                let this_table_relation = table_name.clone();
                for PlanWithDeepColumnMap { plan: _plan, columns } in cols_level.iter() {
                    for (col, _projections) in columns.iter() {
                        if col.relation.is_none() {
                            // the key for this column is in the map !
                            let new_col = Column {
                                relation: Some(this_table_relation.clone()),
                                name: col.name.clone(),
                                spans: Default::default(),
                            };
                            aliases_in_current_plan.insert(col.name.clone(), new_col);
                        }
                    }
                }
            } else if matches!(plan, LogicalPlan::Projection(_)) {
                for expr in plan.expressions().iter() {
                    match expr {
                        Expr::Alias(Alias { expr: alias_source, relation: _relation, name }) => {
                            // FIXME: what is the correct relation ? the one in the alias or the one in the expr ?
                            // We'll use the one in the expr
                            let alias_source_column_map = expr_to_deep_columns(alias_source.as_ref());
                            if alias_source_column_map.len() != 1 {
                                warn!("subquery translation: fix aliased cols from above - strange result for map: {:?}", &alias_source_column_map);
                            } else {
                                let tmp_map_vec = alias_source_column_map.into_iter().collect::<Vec<_>>();
                                // we have a single entry
                                let (new_col, _new_deep_projection) = tmp_map_vec[0].clone();
                                aliases_in_current_plan.insert(name.clone(), new_col);
                            }
                        }
                        _ => {
                            let column_map = expr_to_deep_columns(expr);
                            if column_map.len() != 1 {
                                warn!("subquery translation: fix aliased cols from above - strange result for map: {:?}", &column_map);
                            } else {
                                let tmp_map_vec = column_map.into_iter().collect::<Vec<_>>();
                                // we have a single entry
                                let (new_col, _new_deep_projection) = tmp_map_vec[0].clone();
                                aliases_in_current_plan.insert(new_col.name.clone(), new_col);
                            }
                        }
                    }
                }
            }
            // info!("subquery translation: found in current plan: {:?}", aliases_in_current_plan);
            // info!("subquery translation: found in current plan: {:?}", &cols_from_above);
            // now, we go through all the pdcs that we got, and we see whether their relation is null and their column name is in the aliases_in_current_plan map
            // if they are, then we rewrite them so that the column
            let mut new_cols_level: Vec<PlanWithDeepColumnMap> = vec![];
            for PlanWithDeepColumnMap { plan, columns } in cols_level.iter() {
                let mut new_columns: DeepColumnColumnMap = HashMap::new();
                for (col, projections) in columns.iter() {
                    if col.relation.is_none() && aliases_in_current_plan.contains_key(&col.name) {
                        // the key for this column is in the map !
                        let actual_column = aliases_in_current_plan.get(&col.name).unwrap();
                        // info!("subquery translation: replacing column {:?} with {:?} = {:?}", col, actual_column, projections);
                        new_columns.insert(actual_column.clone(), projections.clone());
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
            info!("subquery translation: REPLACED: {:?}", &cols_level);
        }

        let current_pdc = PlanWithDeepColumnMap::from_plan(&plan);
        cols_level.push(current_pdc);
        // info!("BEFORE: {:?}", &cols_level);

        match plan {
            LogicalPlan::Projection(proj) => {
                let Projection {
                    expr, input, schema, ..
                } = proj;

                let mut new_proj_schema: Option<DFSchemaRef> = None;
                if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_PROJECTION_MERGING != 0 {
                    // info!("projection merging: EXECUTING FLAG_ENABLE_PROJECTION_MERGING code path, cols_level = {:?}", &cols_level);
                    // info!("projection merging: EXECUTING FLAG_ENABLE_PROJECTION_MERGING expr = {:?}", &expr);
                    if are_exprs_plain(&expr) && cols_level.len() > 0 {
                        // info!("projection merging: can proceed, projection is plain");
                        // this projection is a candidate, it "looks" like an inserted projection
                        // for each of the columns in this projection, try to find previous references to this column
                        // get the largest of these references and set it to this projection
                        let my_pdc = cols_level.last().unwrap();
                        let mut my_new_modified_columns: DeepColumnColumnMap = HashMap::new();
                        'col_iterate: for (my_col, my_deep_projections) in my_pdc.columns.iter() {
                            // info!("projection merging searching previous projections for column {:?}, current projections={:?}", my_col, my_deep_projections);
                            for my_deep_projection in my_deep_projections.iter() {
                                // col + projection
                                if my_deep_projection == "*" {
                                    // we only do this if it's a top-level projection
                                    let previous_deep_projections = find_all_projections_for_column(my_col, &cols_level, 0, cols_level.len() - 1);
                                    // info!("projection merging found previous deep projections for column {:?}: {:?}", my_col, &previous_deep_projections);
                                    if previous_deep_projections.len() == 0 {
                                        // we don't specify this column in the previous projection, so we need to leave it
                                        continue 'col_iterate;
                                    }
                                    // we have previous deep projections
                                    // info!("projection merging rewrite projection for col {:?}: {:?}", my_col, previous_deep_projections);
                                    // change the hashmap entry at this level
                                    // ADR: WTF, simplify this, don't know how to write in the friggin vec
                                    my_new_modified_columns.insert(my_col.clone(), previous_deep_projections);
                                    let new_pdc = PlanWithDeepColumnMap {
                                        plan: my_pdc.plan.clone(),
                                        columns: HashMap::new(),
                                    };
                                }
                            }
                        }
                        // rewrite it !
                        if my_new_modified_columns.len() > 0 {
                            let mut my_new_pdc = PlanWithDeepColumnMap {
                                plan: my_pdc.plan.clone(),
                                columns: HashMap::new(),
                            };
                            for (my_pdc_col, my_pdc_deep_projection) in my_pdc.columns.iter() {
                                // we just change the projections for this col
                                // it doesn't matter that it doesn't reflect what's in the actual projection, because we don't use that anymore below
                                if let Some(new_deep_projection) = my_new_modified_columns.get(my_pdc_col) {
                                    my_new_pdc.columns.insert(my_pdc_col.clone(), new_deep_projection.clone());
                                } else {
                                    my_new_pdc.columns.insert(my_pdc_col.clone(), my_pdc_deep_projection.clone());
                                }
                            }
                            let last_index = cols_level.len() - 1;
                            cols_level[last_index] = my_new_pdc;
                            // rewrite the projection schema accordingly !!!!
                            // ADR: this shouldn't be needed. But it is, there is an extra aggregation step that verifies that the physical schema is the same as the logical schema
                            // which means they need to match
                            // the option is options.execution.skip_physical_aggregate_schema_check - see the code for that
                            // Also, we could CHANGE the actual verification to check that the schema can be rewritten - deep functions, instead of checking for exact equality
                            // the code WILL work when we
                            // - remove the check by disabling the option
                            // - change the check so that it verifies that the schemas can be casted / rewritten, instead of being equal
                            {
                                let proj_inner_schema = schema.inner();
                                let proj_metadata = schema.metadata().clone();
                                let _proj_functional_dependencies = schema.functional_dependencies();
                                // compute qualified fields
                                let mut proj_qualified_fields: Vec<Option<TableReference>> = vec![];
                                let mut all_projection_indices: Vec<usize> = vec![];
                                for (idx, (c, _f)) in schema.iter().enumerate() {
                                    let newc = c.cloned();
                                    proj_qualified_fields.push(newc);
                                    all_projection_indices.push(idx);
                                }

                                let deep_projection = transform_column_deep_projection_map_to_usize_index_type_and_clean_up_stars_for_plain_schema(
                                    &cols_level.last().unwrap().columns,
                                    proj_inner_schema,
                                );
                                let new_proj_inner_schema = try_rewrite_schema_opt(proj_inner_schema.clone(), Some(&all_projection_indices), Some(&deep_projection))?;
                                info!("REWRITTEN SCHEMA: {:?}", new_proj_inner_schema);
                                // remake a DFSchema for the Projection
                                // zip the fields
                                let new_qualified_fields = proj_qualified_fields
                                    .into_iter()
                                    .zip(new_proj_inner_schema.fields.iter().map(|f|f.clone()))
                                    .collect::<Vec<(Option<TableReference>, FieldRef)>>();
                                let new_df_schema = Arc::new(DFSchema::new_with_metadata(
                                    new_qualified_fields,
                                    proj_metadata
                                )?);
                                new_proj_schema = Some(new_df_schema);
                            }
                        }
                    }
                }

                if let Some(new_proj_schema) = new_proj_schema {
                    let new_proj = Projection::try_new_with_schema(expr, input, new_proj_schema);
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
                            null_equals_null
                        }),
                        is_transformed,
                        TreeNodeRecursion::Jump,
                    )
                )
            }
            LogicalPlan::Union(union) => {
                let mut new_children: Vec<Arc<LogicalPlan>> = vec![];
                let Union {
                    inputs, schema
                } = union;
                let mut any_transformed = false;
                for input in inputs.iter() {
                    let tmp = deep_plan_transformer(input.as_ref().clone(), vec![], options)?;
                    any_transformed = any_transformed ||  tmp.transformed;
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
                )
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

                info!("TABLE SCAN: input cols_level= {:?}", &cols_level);
                // can we actually do the deep merging ?
                // ADR: this no longer works after passing in stuff through SubqueryAlias
                let mut projections_before_indices: Vec<usize> = vec![];
                for (idx, x) in cols_level.iter().enumerate() {
                    if matches!(x.plan.as_ref(), LogicalPlan::Projection(_)) {
                        projections_before_indices.push(idx);
                    }
                }
                if projections_before_indices.len() == 0 && cols_from_above.len() == 0{
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
                info!("TABLE SCAN COLUMNS FOR ME({}): {:?}", table_name.table(), columns_for_this_scan);
                // compact all the separate levels to a single map
                let single_columns_for_this_scan = compact_list_of_column_maps_to_single_map(&columns_for_this_scan);
                info!("TABLE SCAN COLUMNS FOR ME({}): {:?}", table_name.table(), single_columns_for_this_scan);
                // fix deep projections - we might have things that look like struct access, but they are map access
                let fixed_single_columns_for_this_scan = fix_deep_projection_according_to_table_schema(&single_columns_for_this_scan, &df_schema);
                info!("TABLE SCAN COLUMNS FOR ME({}): {:?}", table_name.table(), fixed_single_columns_for_this_scan);
                // transform to actual deep projection - use the column index instead of the column, and replace ["*"] with empty
                let projection_deep = transform_column_deep_projection_map_to_usize_index_type_and_clean_up_stars(&fixed_single_columns_for_this_scan, &df_schema);
                trace!(target: "deep", "Rewriting deep projections for table {}: {:?}", table_name.table(), projection_deep);
                let reprojected_schema = reproject_for_deep_schema(
                    &table_name,
                    projection.clone(),
                    Some(projection_deep.clone()),
                    &projected_schema,
                    &source.schema(),
                )?;
                // info!("reprojected schema: {:#?}", reprojected_schema.inner());
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
                let mut translated_pdcs:Vec<PlanWithDeepColumnMap> = vec![];
                if options.optimizer.deep_column_pruning_flags & FLAG_ENABLE_SUBQUERY_TRANSLATION != 0 {
                    // info!("subquery translation: activating subquery translation, for alias {}", &alias);
                    // info!("subquery translation: activating subquery translation CHECKING {:?}", &cols_level);
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
                                    // don't add the projection if it's empty
                                    if !is_empty_deep_projection(projections) {
                                        translated_pdc.columns.insert(new_col, projections.clone());
                                    }
                                }
                            }
                        }
                        if translated_pdc.columns.len() > 0 {
                            translated_pdcs.push(translated_pdc);
                        }
                    }
                    info!("subquery translation: translated pdcs = {:?}", translated_pdcs);
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

pub fn get_columns_referenced_in_plan(plan: &LogicalPlan) -> DeepColumnColumnMap {
    let expressions = plan.expressions(); //get_plan_expressions(p);
                                          // info!("  expressions = {:?}", &expressions);
    let mut deep_column_map: DeepColumnColumnMap = HashMap::new();
    for expr in expressions.iter() {
        let tmp = expr_to_deep_columns(expr);
        for (k, vs) in tmp.iter() {
            // if we have an aggregation below, we might get here a column that looks like
            // Alias(Alias { expr: Column(Column { relation: None, name: "lag(events.UserId,Int64(1)) PARTITION BY [events.DeviceId] ORDER BY [events.timestamp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" }), relation: None, name: "PreviousUserColName" }),
            // we just filter out the None relations, since we care about tables
            // we rely on a lower level field having this.
            if k.relation.is_none() {
                continue;
            }
            // info!("  column = {:?}, vs: {:?}", k, vs);
            for v in vs {
                merge_value_in_column_map(&mut deep_column_map, k, v.clone());
            }
            // if deep_column_map.contains_key(k) {
            //     let mut current_values = deep_column_map.get(k).unwrap();
            //
            //     current_values.push(v.clone());
            // } else {
            //     deep_column_map.insert(k.clone(), v.clone());
            // }
        }
    }
    // info!("columns : {:?}", &deep_column_map);
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
            for value in deep.iter() {
                merge_value_in_column_map(&mut output, column, value.clone());
            }
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

pub fn fix_deep_projection_according_to_table_schema(
    input: &DeepColumnColumnMap,
    schema: &DFSchemaRef,
) -> DeepColumnColumnMap {
    // info!("fix_deep_projection_according_to_table_schema SCHEMA: {:?}", schema);

    let mut output: DeepColumnColumnMap = HashMap::new();
    for (col, deep_projection_specifiers) in input {
        // info!("fix_deep_projection_according_to_table_schema checking {:?}", col);
        let col_to_check = Column::new_unqualified(col.name.clone());
        if let Some(col_idx_in_schema) = schema.maybe_index_of_column(&col_to_check) {
            // info!("COL IDX {}", col_idx_in_schema);
            // get the rest and see whether the column type
            // we iterate through the rest specifiers and we fix them
            // that is, if we see something that looks like a get field, but we know the field in the schema
            // is a map, that means that we need to replace it with *
            // map_field['val'], projection_rest = ["val"] => projection_rest=["*"]
            let mut fixed_deep_projection_specifiers: Vec<String> = vec![];
            for projection_specifier in deep_projection_specifiers {
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
    output
}

pub fn merge_value_in_column_map(
    map: &mut DeepColumnColumnMap,
    col: &Column,
    new_value: String,
) {
    assert_ne!(new_value, ""); // the value should be something, we don't use empty strings anymore
    if map.contains_key(col) {
        let current_values = map.get(col).unwrap();
        if new_value == "*" {
            // replace the existing values
            map.insert(col.clone(), vec!["*".to_string()]);
            return;
        } else if is_empty_deep_projection(current_values) {
            // we already read all from above, ignore anything that comes later
            return;
        } else {
            // we actually add something

            // do we already have it ? then return, don't modify the map
            for value in current_values.iter() {
                if *value == new_value {
                    return;
                }
            }
            // a.b.c, but we already have a, or a.b
            for value in current_values.iter() {
                if new_value.starts_with(value) {
                    return;
                }
            }
            let mut values = current_values.clone();
            values.push(new_value);
            map.insert(col.clone(), values);
        }
    } else {
        map.insert(col.clone(), vec![new_value]);
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
            for projection in projections_for_col {
                merge_value_in_column_map(&mut output, needle, projection.clone());
            }
        }
    }
    output.get(needle).unwrap().clone()
}

pub fn get_plan_expressions(plan: &LogicalPlan) -> Vec<Expr> {
    match plan {
        LogicalPlan::Join(join) => {
            let mut output: Vec<Expr> = vec![];
            if let Some(join_filter) = &join.filter {
                output.push(join_filter.clone())
            }
            for (e1, e2) in join.on.iter() {
                output.push(e1.clone());
                output.push(e2.clone());
            }
            output
        }
        _ => plan.expressions(),
    }
}

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

pub fn fix_possible_field_accesses(
    schema: &DFSchemaRef,
    field_idx: usize,
    deep_projection_specifier: &String,
) -> Result<Vec<String>> {
    // info!("fix_possible_field_accesses {} {}", field_idx, deep_projection_specifier);
    if deep_projection_specifier == "*" {
        return Ok(vec!["*".to_string()]);
    }
    let rest = deep_projection_specifier
        .split(".")
        .map(|x| x.to_string())
        .collect::<Vec<String>>();
    let mut field = Arc::new(schema.field(field_idx).clone());
    let mut rest_idx = 0 as usize;
    let mut out = rest.clone();
    while rest_idx < out.len() {
        let (fix_non_star_access, should_continue, new_field) = match field.data_type() {
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
            | DataType::RunEndEncoded(_, _) => (false, false, None),
            DataType::Union(_, _) => {
                // FIXME @HStack
                // don't know what to do here
                (false, false, None)
            }
            DataType::List(inner)
            | DataType::ListView(inner)
            | DataType::FixedSizeList(inner, _)
            | DataType::LargeList(inner)
            | DataType::LargeListView(inner) => {
                let new_field = inner.clone();
                (true, true, Some(new_field))
            }
            DataType::Struct(inner_struct) => {
                let mut new_field: Option<FieldRef> = None;
                for f in inner_struct.iter() {
                    if f.name() == &out[rest_idx] {
                        new_field = Some(f.clone());
                    }
                }
                (false, true, new_field)
            }
            DataType::Map(inner_map, _) => {
                let new_field: Option<FieldRef>;
                match inner_map.data_type() {
                    DataType::Struct(inner_map_struct) => {
                        new_field = Some(inner_map_struct[1].clone());
                    }
                    _ => {
                        return Err(DataFusionError::Internal(String::from(
                            "Invalid inner map type",
                        )));
                    }
                }
                (true, true, new_field)
            }
        };
        if fix_non_star_access && rest[rest_idx] != "*" {
            out[rest_idx] = "*".to_string();
        }
        if !should_continue {
            break;
        }
        field = new_field.unwrap();
        rest_idx += 1;
    }
    Ok(out)
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

// pub fn append_column(acc: &mut DeepColumnColumnMap, column: &Column, rest: Vec<String>) {
//     info!("APPEND: {:?} = {:?}", column, rest);
//     match acc.get_mut(column) {
//         None => {
//             let column_clone = column.clone();
//             if rest.len() > 0 {
//                 acc.insert(column_clone, vec![rest.join(".")]);
//             } else {
//                 acc.insert(column_clone, vec![]);
//             }
//         }
//         Some(cc) => {
//             if rest.len() > 0 {
//                 cc.push(rest.join("."));
//             }
//         }
//     }
// }

pub fn get_plan_name(plan: &LogicalPlan) -> String {
    match plan {
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
    }
}
