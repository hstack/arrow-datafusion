// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Analyzed rule to replace TableScan references
//! such as DataFrames and Views and inlines the LogicalPlan.

use crate::analyzer::AnalyzerRule;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, Result};
use datafusion_expr::{logical_plan::LogicalPlan, wildcard, Expr, LogicalPlanBuilder};

/// Analyzed rule that inlines TableScan that provide a [`LogicalPlan`]
/// (DataFrame / ViewTable)
#[derive(Default, Debug)]
pub struct InlineTableScan;

impl InlineTableScan {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for InlineTableScan {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down_with_subqueries(analyze_internal).data()
    }

    fn name(&self) -> &str {
        "inline_table_scan"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        // Match only on scans without filter / projection / fetch
        // Views and DataFrames won't have those added
        // during the early stage of planning.
        LogicalPlan::TableScan(table_scan) if table_scan.filters.is_empty() => {
            if let Some(sub_plan) = table_scan.source.get_logical_plan() {
                let sub_plan = sub_plan.into_owned();
                let projection_exprs =
                    generate_projection_expr(&table_scan.projection, &sub_plan)?;

                LogicalPlanBuilder::from(sub_plan)
                    .project(projection_exprs)?
                    // Ensures that the reference to the inlined table remains the
                    // same, meaning we don't have to change any of the parent nodes
                    // that reference this table.
                    .alias(table_scan.table_name)?
                    .build()
                    .map(Transformed::yes)
            } else {
                Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
            }
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn generate_projection_expr(
    projection: &Option<Vec<usize>>,
    sub_plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    let mut exprs = vec![];
    if let Some(projection) = projection {
        for i in projection {
            exprs.push(Expr::Column(Column::from(
                sub_plan.schema().qualified_field(*i),
            )));
        }
    } else {
        exprs.push(wildcard());
    }
    Ok(exprs)
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc, vec};

    use crate::analyzer::inline_table_scan::InlineTableScan;
    use crate::test::assert_analyzed_plan_eq;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder, TableSource};

    pub struct RawTableSource {}

    impl TableSource for RawTableSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ]))
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> datafusion_common::Result<Vec<datafusion_expr::TableProviderFilterPushDown>>
        {
            Ok((0..filters.len())
                .map(|_| datafusion_expr::TableProviderFilterPushDown::Inexact)
                .collect())
        }
    }

    pub struct CustomSource {
        plan: LogicalPlan,
    }

    impl CustomSource {
        fn new() -> Self {
            Self {
                plan: LogicalPlanBuilder::scan("y", Arc::new(RawTableSource {}), None)
                    .unwrap()
                    .build()
                    .unwrap(),
            }
        }
    }

    impl TableSource for CustomSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> datafusion_common::Result<Vec<datafusion_expr::TableProviderFilterPushDown>>
        {
            Ok((0..filters.len())
                .map(|_| datafusion_expr::TableProviderFilterPushDown::Exact)
                .collect())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
        }

        fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
            Some(Cow::Borrowed(&self.plan))
        }
    }

    #[test]
    fn inline_table_scan() -> datafusion_common::Result<()> {
        let scan = LogicalPlanBuilder::scan(
            "x".to_string(),
            Arc::new(CustomSource::new()),
            None,
        )?;
        let plan = scan.filter(col("x.a").eq(lit(1)))?.build()?;
        let expected = "Filter: x.a = Int32(1)\
        \n  SubqueryAlias: x\
        \n    Projection: *\
        \n      TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), plan, expected)
    }

    #[test]
    fn inline_table_scan_with_projection() -> datafusion_common::Result<()> {
        let scan = LogicalPlanBuilder::scan(
            "x".to_string(),
            Arc::new(CustomSource::new()),
            Some(vec![0]),
        )?;

        let plan = scan.build()?;
        let expected = "SubqueryAlias: x\
        \n  Projection: y.a\
        \n    TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), plan, expected)
    }

    #[derive(Debug)]
    // stand-in for DataFrameTableProvider which we can't access here
    struct WrappingSource {
        plan: LogicalPlan,
    }

    impl TableSource for WrappingSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            let schema: Schema = self.plan.schema().as_ref().into();
            Arc::new(schema)
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> datafusion_common::Result<Vec<datafusion_expr::TableProviderFilterPushDown>>
        {
            // A filter is added on the DataFrame when given
            Ok(vec![
                datafusion_expr::TableProviderFilterPushDown::Exact;
                filters.len()
            ])
        }

        fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
            Some(Cow::Borrowed(&self.plan))
        }
    }

    #[test]
    fn inline_table_scan_wrapped_once() -> datafusion_common::Result<()> {
        let custom_source = CustomSource::new();
        let wrapped_source = WrappingSource {
            plan: LogicalPlanBuilder::scan("wrapped", Arc::new(custom_source), None)?
                .build()?,
        };

        let scan =
            LogicalPlanBuilder::scan("x".to_string(), Arc::new(wrapped_source), None)?;
        let plan = scan.build()?;
        let expected = "SubqueryAlias: x\
        \n  Projection: *\
        \n    SubqueryAlias: wrapped\
        \n      Projection: *\
        \n        TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), plan, expected)
    }

    #[test]
    fn inline_table_scan_wrapped_twice_with_projection() -> datafusion_common::Result<()> {
        let custom_source = CustomSource::new();
        let wrapped_source_once = WrappingSource {
            plan: LogicalPlanBuilder::scan(
                "wrapped_once",
                Arc::new(custom_source),
                Some(vec![0]),
            )?
            .build()?,
        };
        let wrapped_source_twice = WrappingSource {
            plan: LogicalPlanBuilder::scan(
                "wrapped_twice",
                Arc::new(wrapped_source_once),
                None,
            )?
            .build()?,
        };

        let scan = LogicalPlanBuilder::scan(
            "x".to_string(),
            Arc::new(wrapped_source_twice),
            None,
        )?;
        let plan = scan.build()?;
        let expected = "SubqueryAlias: x\
        \n  Projection: *\
        \n    SubqueryAlias: wrapped_twice\
        \n      Projection: *\
        \n        SubqueryAlias: wrapped_once\
        \n          Projection: y.a\
        \n            TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), plan, expected)
    }
}
