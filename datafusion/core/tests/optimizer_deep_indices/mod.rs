use arrow_schema::{DataType, Field, Fields, Schema};
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::logical_expr::Operator;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::Result;
use datafusion_common::{DFSchema, JoinType};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::{col, lit, BinaryExpr, Expr, Literal, LogicalPlanBuilder};
use datafusion_functions::expr_fn::get_field;
use datafusion_optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion_optimizer::optimize_projections_deep::{
    DeepColumnIndexMap, PlanWithDeepColumnMap, FLAG_ENABLE,
    FLAG_ENABLE_PROJECTION_MERGING, FLAG_ENABLE_SUBQUERY_TRANSLATION,
};
use datafusion_optimizer::push_down_filter::PushDownFilter;
use datafusion_optimizer::push_down_limit::PushDownLimit;
use datafusion_physical_plan::work_table::WorkTableExec;
use log::info;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // enable logging so RUST_LOG works
    let _ = env_logger::try_init();
}

pub fn make_get_field(from: Expr, sub_col_name: &str) -> Expr {
    get_field(from, sub_col_name)
}

pub fn build_deep_schema() -> Schema {
    Schema::new(vec![
        Field::new("sc1", DataType::Int64, true),
        Field::new(
            "st1",
            DataType::Struct(Fields::from(vec![
                Field::new("sc1", DataType::Utf8, true),
                Field::new(
                    "st1",
                    DataType::Struct(Fields::from(vec![
                        Field::new("sc1", DataType::Int64, true),
                        Field::new("sc2", DataType::Utf8, true),
                    ])),
                    true,
                ),
            ])),
            true,
        ),
        Field::new(
            "st2",
            DataType::Struct(Fields::from(vec![Field::new(
                "st2_sc1",
                DataType::Utf8,
                true,
            )])),
            true,
        ),
    ])
}

#[test]
pub fn test_make_required_indices() {
    let _ = env_logger::try_init();
    let schema = build_deep_schema();
    let df_schema = Arc::new(DFSchema::try_from(schema.clone()).unwrap());
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    let get_st1_sc1 = make_get_field(col("st1"), "sc1");
    let get_st1_st1_sc1 = make_get_field(make_get_field(col("st1"), "st1"), "sc1");
    let get_st1_st1_sc2 = make_get_field(make_get_field(col("st1"), "st1"), "sc2");
    let st1_sc1_not_null = Expr::IsNotNull(Box::new(get_st1_sc1.clone()));
    let st1_st1_sc1_not_null = Expr::IsNotNull(Box::new(get_st1_st1_sc1.clone()));
    let test_expr = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(st1_sc1_not_null.clone()),
        Operator::And,
        Box::new(st1_st1_sc1_not_null.clone()),
    ));
}

fn build_context() -> SessionContext {
    let config = SessionConfig::new()
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
        .set_usize("datafusion.optimizer.max_passes", 2)
        // 0 - disabled
        // 1 - just main merging
        // 2 - enable projection merging
        // 1 | 2 == 3 - all
        .set_usize(
            "datafusion.optimizer.deep_column_pruning_flags",
            FLAG_ENABLE
                | FLAG_ENABLE_PROJECTION_MERGING
                | FLAG_ENABLE_SUBQUERY_TRANSLATION,
        );
    // .set_usize("datafusion.optimizer.deep_column_pruning_flags", FLAG_ENABLE | FLAG_ENABLE_PROJECTION_MERGING);
    // .set_bool("datafusion.execution.skip_physical_aggregate_schema_check", true);
    SessionContext::new_with_config(config)
}

#[tokio::test]
async fn test_deep_projections_1() -> Result<()> {
    let parquet_path = format!(
        "{}/tests/data/deep_projections/first.parquet",
        env!("CARGO_MANIFEST_DIR")
    );

    // {
    //     let file = File::open(Path::new(parquet_path.as_str()))?;
    //     let reader = SerializedFileReader::new(file).unwrap();
    //     let parquet_schema = reader.metadata().file_metadata().schema_descr();
    //     let arrow_schema = parquet_to_arrow_schema(parquet_schema, None).unwrap();
    //     let df_schema = DFSchema::try_from(arrow_schema.clone()).unwrap();
    //
    //     let filters = vec![
    //         get_field(col("cross_industry_demo_data.endUserIDs"), "aaid_id")
    //             .is_not_null(),
    //         get_field(col("cross_industry_demo_data.endUserIDs"), "aaid_id")
    //             .not_eq(lit("")),
    //         get_field(col("cross_industry_demo_data._experience"), "eVar56")
    //             .is_not_null(),
    //         get_field(col("cross_industry_demo_data._experience"), "eVar56")
    //             .not_eq(lit("")),
    //     ];
    // }

    let ctx = build_context();
    ctx.register_parquet(
        "cross_industry_demo_data",
        parquet_path,
        ParquetReadOptions::default(),
    )
    .await?;
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
            WITH events AS (
                SELECT
                    endUserIDs.aaid_id as DeviceId,
                    _experience.eVar56 as UserId,
                    timestamp
                FROM
                    cross_industry_demo_data
                WHERE _ACP_DATE='2025-01-03'
            )
            SELECT
                events.*,
                LAG(UserId, 1) OVER (PARTITION BY DeviceId ORDER BY events.timestamp) AS PreviousUserColName,
                cross_industry_demo_data._experience.eVar56
            FROM events
            INNER JOIN cross_industry_demo_data on events.DeviceId = cross_industry_demo_data.endUserIDs.aaid_id
            LIMIT 100
        "#,
        vec![
            Some(HashMap::from([(0, vec![]), (1, vec![]), (2, vec!["aaid_id".to_string()]), (3, vec!["eVar56".to_string()])])),
            Some(HashMap::from([(2, vec!["aaid_id".to_string()]), (3, vec!["eVar56".to_string()])]))
        ]
    ).await;
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
            SELECT
                count(*) as count_events
            FROM cross_industry_demo_data
            WHERE
                (_ACP_DATE BETWEEN '2023-01-01' AND '2025-02-02')
                AND _experience.eVar56 is not null
            LIMIT 100
        "#,
        vec![Some(HashMap::from([
            (0, vec![]),
            (3, vec!["eVar56".to_string()]),
        ]))],
    )
    .await;
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
            SELECT
                endUserIDs
            FROM cross_industry_demo_data
            WHERE
                (_ACP_DATE BETWEEN '2025-01-01' AND '2025-01-02')
                AND _experience.eVar56 is not null
            LIMIT 10
        "#,
        vec![Some(HashMap::from([
            (0, vec![]),
            (2, vec![]),
            (3, vec!["eVar56".to_string()]),
        ]))],
    )
    .await;
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
            SELECT
                *
            FROM cross_industry_demo_data
            WHERE
                (_ACP_DATE BETWEEN '2023-01-01' AND '2025-02-02')
                AND _experience.eVar56 is not null
            LIMIT 100
        "#,
        vec![None],
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn test_deep_projections_genstudio() -> Result<()> {
    let ctx = build_context();
    let _ = ctx.register_parquet(
        "meta_asset_summary_metrics",
        format!("{}/tests/data/deep_projections/genstudio/meta_asset_summary_metrics.parquet", env!("CARGO_MANIFEST_DIR")),
        ParquetReadOptions::default(),
    ).await?;
    let _ = ctx.register_parquet(
        "meta_asset_summary_metrics_by_age_and_gender",
        format!("{}/tests/data/deep_projections/genstudio/meta_asset_summary_metrics_by_age_and_gender.parquet", env!("CARGO_MANIFEST_DIR")),
        ParquetReadOptions::default(),
    ).await?;
    let _ = ctx.register_parquet(
        "meta_asset_featurization",
        format!("{}/tests/data/deep_projections/genstudio/meta_asset_featurization.parquet", env!("CARGO_MANIFEST_DIR")),
        ParquetReadOptions::default(),
    ).await?;

    // Stats: Asset summary metrics
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
        SELECT
            count(*) AS cnt
        FROM
            meta_asset_summary_metrics
        WHERE
            _ACP_DATE = '2024-12-01'
        "#,
        vec![Some(HashMap::from([(3, vec![])]))],
    )
    .await?;

    // Preview: Asset summary metrics
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
            SELECT
                *
            FROM
                meta_asset_summary_metrics
            LIMIT 100
        "#,
        vec![None],
    )
    .await?;

    // Agg: Count assets by age
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
        SELECT
            count (*) AS cnt,
            _aresstagevalidationco['genStudioInsights']['age'] AS age
        FROM
            meta_asset_summary_metrics_by_age_and_gender
        WHERE
            _ACP_DATE = '2024-12-01'
        GROUP BY
            age
        ORDER BY
            cnt DESC
        LIMIT
            10
        "#,
        vec![Some(HashMap::from([
            (2, vec!["genStudioInsights.age".to_string()]),
            (3, vec![]),
        ]))],
    )
    .await?;

    // Agg: clicks by url
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
        SELECT
            AVG(
                asset_metrics._aresstagevalidationco.genStudioInsights.metrics.performance.clicks.value
            ) AS clicks,
            asset_meta._aresstagevalidationco.contentAssets.assetThumbnailURL AS asset_url
        FROM
            (meta_asset_featurization AS asset_meta
            INNER JOIN meta_asset_summary_metrics AS asset_metrics ON (
                asset_meta._aresstagevalidationco['contentAssets']['assetID'] = asset_metrics._aresstagevalidationco['genStudioInsights']['assetID']
            ))
        WHERE
            _ACP_DATE = '2024-12-01'
        GROUP BY
            asset_url
        ORDER BY
            clicks DESC
        "#,
        vec![
            Some(
                HashMap::from([
                    (1, vec!["contentAssets.assetThumbnailURL".to_string(), "contentAssets.assetID".to_string()]),
                ])
            ),
            Some(
                HashMap::from([
                    (2, vec!["genStudioInsights.metrics.performance.clicks.value".to_string(), "genStudioInsights.assetID".to_string()]),
                    (3, vec![])
                ])
            ),
        ],
    )
    .await?;

    // Agg: clicks by url
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
        SELECT
            talias.tmetrics_aresstagevalidationco.genStudioInsights.metrics.performance.clicks.value  as clicks,
            talias.tfeatures_aresstagevalidationco.contentAssets.assetThumbnailURL AS asset_url
        FROM (
            SELECT
                asset_metrics._aresstagevalidationco AS tmetrics_aresstagevalidationco,
                asset_meta._aresstagevalidationco AS tfeatures_aresstagevalidationco
            FROM
                meta_asset_featurization AS asset_meta
                INNER JOIN meta_asset_summary_metrics AS asset_metrics ON (
                    asset_meta._aresstagevalidationco.contentAssets.assetID = asset_metrics._aresstagevalidationco.genStudioInsights.assetID
                )
            WHERE
                _ACP_DATE = '2024-12-01'
        ) AS talias
        ORDER BY
            clicks DESC
        "#,
        vec![
            Some(
                HashMap::from([
                    (1, vec!["contentAssets.assetThumbnailURL".to_string(), "contentAssets.assetID".to_string()]),
                ])
            ),
            Some(
                HashMap::from([
                    (2, vec!["genStudioInsights.metrics.performance.clicks.value".to_string(), "genStudioInsights.assetID".to_string()]),
                    (3, vec![])
                ])
            ),
        ],
    )
    .await?;

    // SQL Editor
    let _ = run_deep_projection_optimize_test(
        &ctx,
        r#"
        SELECT
            _ACP_DATE DAY,
            _aresstagevalidationco.genStudioInsights.campaignID campaign_id,
            SUM(
                _aresstagevalidationco.genStudioInsights.metrics.spend.value
            ) total_spend
        FROM
            meta_asset_summary_metrics
        WHERE
            _ACP_DATE BETWEEN '2024-12-01' AND '2024-12-15'
        GROUP BY
            DAY,
            campaign_id
        ORDER BY
            DAY,
            total_spend DESC,
            campaign_id
        "#,
        vec![Some(HashMap::from([
            (
                2,
                vec![
                    "genStudioInsights.campaignID".to_string(),
                    "genStudioInsights.metrics.spend.value".to_string(),
                ],
            ),
            (3, vec![]),
        ]))],
    )
    .await?;

    Ok(())
}

async fn run_deep_projection_optimize_test(
    ctx: &SessionContext,
    query: &str,
    tests: Vec<Option<DeepColumnIndexMap>>,
) -> Result<()> {
    let plan = ctx.state().create_logical_plan(query).await?;
    let optimized_plan = ctx.state().optimize(&plan)?;
    let state = ctx.state();
    let query_planner = state.query_planner().clone();
    let physical_plan = query_planner
        .create_physical_plan(&optimized_plan, &state)
        .await?;
    let mut deep_projections: Vec<Option<DeepColumnIndexMap>> = vec![];
    let _ = physical_plan.apply(|pp| {
        if let Some(pe) = pp.as_any().downcast_ref::<ParquetExec>() {
            deep_projections.push(pe.base_config().projection_deep.clone());
            // pe.base_config().projection_deep
        }
        if let Some(dse) = pp.as_any().downcast_ref::<DataSourceExec>() {
            let data_source_dyn = dse.data_source();
            if let Some(data_source_file_scan_config) =
                data_source_dyn.as_any().downcast_ref::<FileScanConfig>()
            {
                deep_projections
                    .push(data_source_file_scan_config.projection_deep.clone());
                // pe.base_config().projection_deep
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    info!(
        "Checking if plan has these deep projections: {:?}",
        &deep_projections
    );
    assert_eq!(deep_projections.len(), tests.len());
    for i in 0..deep_projections.len() {
        assert_eq!(
            deep_projections[i], tests[i],
            "Deep projections should be equal at index {}: got={:?} != expected={:?}",
            i, deep_projections[i], tests[i]
        )
    }
    Ok(())
}

#[tokio::test]
async fn test_very_complicated_plan() -> Result<()> {
    let _ = env_logger::try_init();

    let config = SessionConfig::new()
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
        .set_usize("datafusion.optimizer.max_passes", 2);

    let ctx = SessionContext::new_with_config(config);
    // ctx.register_parquet("cross_industry_demo_data", "/Users/adragomi/output.parquet", ParquetReadOptions::default()).await?;
    let _ = ctx
        .sql(
            r#"
        CREATE OR REPLACE TABLE fact_profile_overlap_of_namespace (
          merge_policy_id INT8,
          date_key DATE,
          overlap_id INT8,
          count_of_profiles INT8
        );
    "#,
        )
        .await?;

    let _ = ctx
        .sql(
            r#"
    CREATE OR REPLACE TABLE dim_overlap_namespaces (
        overlap_id INT8,
        merge_policy_id INT8,
        overlap_namespaces VARCHAR
    );
    "#,
        )
        .await?;

    let _ = ctx
        .sql(
            r#"
    CREATE OR REPLACE TABLE fact_profile_by_namespace_trendlines (
        namespace_id INT8,
        merge_policy_id INT8,
        date_key DATE,
        count_of_profiles INT8
    );
    "#,
        )
        .await?;

    let _ = ctx
        .sql(
            r#"
    CREATE OR REPLACE TABLE dim_namespaces (
        namespace_id INT8,
        namespace_description VARCHAR,
        merge_policy_id INT8
    );
    "#,
        )
        .await?;
    info!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    let query = r#"
SELECT
    sum(overlap_col1) overlap_col1,
    sum(overlap_col2) overlap_col2,
    coalesce(Sum(overlap_count), 0) overlap_count
FROM
    (
        SELECT
            0 overlap_col1,
            0 overlap_col2,
            Sum(count_of_profiles) overlap_count
        FROM
            fact_profile_overlap_of_namespace
        WHERE
            fact_profile_overlap_of_namespace.merge_policy_id = -115008144
            AND fact_profile_overlap_of_namespace.date_key = '2024-11-06'
            AND fact_profile_overlap_of_namespace.overlap_id IN (
                SELECT
                    a.overlap_id
                FROM
                    (
                        SELECT
                            dim_overlap_namespaces.overlap_id overlap_id,
                            count(*) cnt_num
                        FROM
                            dim_overlap_namespaces
                        WHERE
                            dim_overlap_namespaces.merge_policy_id = -115008144
                            AND dim_overlap_namespaces.overlap_namespaces IN (
                                'aaid',
                                'ecid'
                            )
                        GROUP BY
                            dim_overlap_namespaces.overlap_id
                    ) a
                WHERE
                    a.cnt_num > 1
            )
        UNION
        ALL
        SELECT
            count_of_profiles overlap_col1,
            0 overlap_col2,
            0 overlap_count
        FROM
            fact_profile_by_namespace_trendlines
            JOIN dim_namespaces ON fact_profile_by_namespace_trendlines.namespace_id = dim_namespaces.namespace_id
            AND fact_profile_by_namespace_trendlines.merge_policy_id = dim_namespaces.merge_policy_id
        WHERE
            fact_profile_by_namespace_trendlines.merge_policy_id = -115008144
            AND fact_profile_by_namespace_trendlines.date_key = '2024-11-06'
            AND dim_namespaces.namespace_description = 'aaid'
        UNION
        ALL
        SELECT
            0 overlap_col1,
            count_of_profiles overlap_col2,
            0 overlap_count
        FROM
            fact_profile_by_namespace_trendlines
            JOIN dim_namespaces ON fact_profile_by_namespace_trendlines.namespace_id = dim_namespaces.namespace_id
            AND fact_profile_by_namespace_trendlines.merge_policy_id = dim_namespaces.merge_policy_id
        WHERE
            fact_profile_by_namespace_trendlines.merge_policy_id = -115008144
            AND fact_profile_by_namespace_trendlines.date_key = '2024-11-06'
            AND dim_namespaces.namespace_description = 'ecid'
    ) a;
    "#;
    let plan = ctx.state().create_logical_plan(query).await?;
    info!("plan: {}", &plan);
    let optimized_plan = ctx.state().optimize(&plan)?;
    info!("optimized: {}", &optimized_plan.display_indent());
    let result = ctx.execute_logical_plan(optimized_plan).await?;
    // let result = ctx.sql(query).await?;
    result.show().await?;

    let push_down_limit = Arc::new(PushDownLimit::new());
    let push_down_filter = Arc::new(PushDownFilter::new());
    let subexpr_eliminator = Arc::new(CommonSubexprEliminate::new());
    let state = ctx.state();

    Ok(())
}

#[tokio::test]
async fn test_mid_values_window() -> Result<()> {
    let _ = env_logger::try_init();

    let config = SessionConfig::new()
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
        .set_usize("datafusion.optimizer.max_passes", 1)
        .set_usize("datafusion.optimizer.deep_column_pruning_flags", 7);

    let ctx = SessionContext::new_with_config(config);
    let _ = ctx
        .register_parquet(
            "midvalues",
            format!(
                "{}/tests/data/deep_projections/triplea/midvalues.parquet",
                env!("CARGO_MANIFEST_DIR")
            ),
            ParquetReadOptions::default(),
        )
        .await?;
    let query = r#"
        SELECT
            timestamp,
            web.webPageDetails.pageViews.value AS pageview,
            endUserIDs._experience.mcid.id AS mcid,
            endUserIDs._experience.aaid.id AS aaid,
            COALESCE(
                endUserIDs._experience.mcid.id,
                endUserIDs._experience.aaid.id
            ) AS partitionCol,
            LAG(timestamp) OVER(
                PARTITION BY COALESCE(
                    endUserIDs._experience.mcid.id,
                    endUserIDs._experience.aaid.id
                )
                ORDER BY timestamp
            ) AS last_event
        FROM
            midvalues
        WHERE
            timestamp >= TO_TIMESTAMP('2025-01-15')
            AND timestamp < TO_TIMESTAMP('2025-01-16')

    "#;

    let _ = run_deep_projection_optimize_test(
        &ctx,
        query,
        vec![Some(HashMap::from([
            (0, vec![]),
            (1, vec!["webPageDetails.pageViews.value".to_string()]),
            (
                2,
                vec![
                    "_experience.mcid.id".to_string(),
                    "_experience.aaid.id".to_string(),
                ],
            ),
        ]))],
    )
    .await;
    // let plan = ctx.state().create_logical_plan(query).await?;
    // info!("plan: {}", &plan);
    // let optimized_plan = ctx.state().optimize(&plan)?;
    // info!("optimized: {}", &optimized_plan.display_indent());
    // let result = ctx.execute_logical_plan(optimized_plan).await?;
    // // let result = ctx.sql(query).await?;
    // result.show().await?;

    Ok(())
}

#[tokio::test]
async fn test_mid_values_window_execution() -> Result<()> {
    let _ = env_logger::try_init();

    let config = SessionConfig::new()
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
        .set_usize("datafusion.optimizer.max_passes", 2)
        .set_usize("datafusion.optimizer.deep_column_pruning_flags", 7);

    let ctx = SessionContext::new_with_config(config);
    let _ = ctx
        .register_parquet(
            "midvalues",
            format!(
                "{}/tests/data/deep_projections/triplea/midvalues.parquet",
                env!("CARGO_MANIFEST_DIR")
            ),
            ParquetReadOptions::default(),
        )
        .await?;
    let query = r#"
        SELECT
            timestamp,
            web.webPageDetails.pageViews.value AS pageview,
            endUserIDs._experience.mcid.id AS mcid,
            endUserIDs._experience.aaid.id AS aaid,
            COALESCE(
                endUserIDs._experience.mcid.id,
                endUserIDs._experience.aaid.id
            ) AS partitionCol,
            LAG(timestamp) OVER(
                PARTITION BY COALESCE(
                    endUserIDs._experience.mcid.id,
                    endUserIDs._experience.aaid.id
                )
                ORDER BY timestamp
            ) AS last_event
        FROM
            midvalues
        WHERE
            timestamp >= TO_TIMESTAMP('2025-01-15')
            AND timestamp < TO_TIMESTAMP('2025-01-16')

    "#;
    let result = ctx.sql(query).await?.collect().await?;

    Ok(())
}

// #[test]
// fn test_adr() -> datafusion_common::Result<()> {
//     let tmp = datafusion_functions::expr_fn::get_field(
//         datafusion_functions::expr_fn::get_field(
//             col("aa"),
//             "bb"
//         ),
//         "cc"
//     );
//     let kk = expr_to_deep_columns(&tmp);
//     info!("kk: {:#?}", kk);
//
//     let tmp =
//         datafusion_functions::expr_fn::get_field(
//             datafusion_functions_nested::expr_fn::array_element(
//                 col("list_struct"),
//                 0_i32.lit()
//             ),
//             "cc"
//         )
//         ;
//     let kk = expr_to_deep_columns(&tmp);
//     info!("kk: {:#?}", kk);
//
//     let tmp = datafusion_functions::expr_fn::nullif(
//         datafusion_functions::expr_fn::get_field(
//             datafusion_functions_nested::expr_fn::array_element(
//                 col("list_struct"),
//                 0_i32.lit()
//             ),
//             "cc"
//         ),
//         datafusion_functions::expr_fn::get_field(
//             datafusion_functions::expr_fn::get_field(
//                 col("othercol"),
//                 "bb"
//             ),
//             "cc"
//         )
//     );
//     let kk = expr_to_deep_columns(&tmp);
//     info!("kk: {:#?}", kk);
//
//     Ok(())
// }
