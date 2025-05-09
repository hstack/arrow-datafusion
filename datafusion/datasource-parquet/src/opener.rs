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

//! [`ParquetOpener`] for opening Parquet files

use std::sync::Arc;

use crate::page_filter::PagePruningAccessPlanFilter;
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::{
    apply_file_schema_type_coercions, coerce_int96_to_resolution, row_filter,
    should_enable_page_index, ParquetAccessPlan, ParquetFileMetrics,
    ParquetFileReaderFactory,
};
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use std::cmp::min;
use std::collections::HashMap;

use arrow::datatypes::{SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use datafusion_common::{exec_err, Result};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::pruning::PruningPredicate;
use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

use futures::{StreamExt, TryStreamExt};
use log::{debug, info, trace};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::SchemaDescriptor;
// use datafusion_common::DataFusionError;
use datafusion_common::deep::{has_deep_projection, rewrite_schema, splat_columns};

/// Implements [`FileOpener`] for a parquet file
pub(super) struct ParquetOpener {
    /// Execution partition index
    pub partition_index: usize,
    /// Column indexes in `table_schema` needed by the query
    pub projection: Arc<[usize]>,
    pub projection_deep: Arc<HashMap<usize, Vec<String>>>,
    /// Target number of rows in each output RecordBatch
    pub batch_size: usize,
    /// Optional limit on the number of rows to read
    pub limit: Option<usize>,
    /// Optional predicate to apply during the scan
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Schema of the output table
    pub table_schema: SchemaRef,
    /// Optional hint for how large the initial request to read parquet metadata
    /// should be
    pub metadata_size_hint: Option<usize>,
    /// Metrics for reporting
    pub metrics: ExecutionPlanMetricsSet,
    /// Factory for instantiating parquet reader
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Should the filters be evaluated during the parquet scan using
    /// [`DataFusionArrowPredicate`](row_filter::DatafusionArrowPredicate)?
    pub pushdown_filters: bool,
    /// Should the filters be reordered to optimize the scan?
    pub reorder_filters: bool,
    /// Should the page index be read from parquet files, if present, to skip
    /// data pages
    pub enable_page_index: bool,
    /// Should the bloom filter be read from parquet, if present, to skip row
    /// groups
    pub enable_bloom_filter: bool,
    /// Schema adapter factory
    pub schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    /// Should row group pruning be applied
    pub enable_row_group_stats_pruning: bool,
    /// Coerce INT96 timestamps to specific TimeUnit
    pub coerce_int96: Option<TimeUnit>,
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();
        let extensions = file_meta.extensions.clone();
        let file_name = file_meta.location().to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        let metadata_size_hint = file_meta.metadata_size_hint.or(self.metadata_size_hint);

        let mut async_file_reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta,
                metadata_size_hint,
                &self.metrics,
            )?;

        let batch_size = self.batch_size;

        let projection = self.projection.clone();
        let projection_vec = projection
            .as_ref()
            .iter()
            .map(|i| *i)
            .collect::<Vec<usize>>();
        info!(
            "ParquetOpener::open projection={:?}, deep_projection: {:?}",
            projection, &self.projection_deep
        );
        // FIXME @HStack: ADR: why do we need to do this ? our function needs another param maybe ?
        // In the case when the projections requested are empty, we should return an empty schema
        let projected_schema = if projection_vec.len() == 0 {
            SchemaRef::from(self.table_schema.project(&projection)?)
        } else {
            rewrite_schema(
                self.table_schema.clone(),
                &projection_vec,
                self.projection_deep.as_ref(),
            )
        };

        let schema_adapter_factory = Arc::clone(&self.schema_adapter_factory);
        let schema_adapter = self
            .schema_adapter_factory
            .create(projected_schema, Arc::clone(&self.table_schema));
        let projection_deep = self.projection_deep.clone();
        let predicate = self.predicate.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let coerce_int96 = self.coerce_int96;
        let enable_bloom_filter = self.enable_bloom_filter;
        let enable_row_group_stats_pruning = self.enable_row_group_stats_pruning;
        let limit = self.limit;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .global_counter("num_predicate_creation_errors");

        let enable_page_index = self.enable_page_index;

        Ok(Box::pin(async move {
            // Don't load the page index yet. Since it is not stored inline in
            // the footer, loading the page index if it is not needed will do
            // unecessary I/O. We decide later if it is needed to evaluate the
            // pruning predicates. Thus default to not requesting if from the
            // underlying reader.
            let mut options = ArrowReaderOptions::new().with_page_index(false);
            let mut metadata_timer = file_metrics.metadata_load_time.timer();

            // Begin by loading the metadata from the underlying reader (note
            // the returned metadata may actually include page indexes as some
            // readers may return page indexes even when not requested -- for
            // example when they are cached)
            let mut reader_metadata =
                ArrowReaderMetadata::load_async(&mut async_file_reader, options.clone())
                    .await?;

            // Note about schemas: we are actually dealing with **3 different schemas** here:
            // - The table schema as defined by the TableProvider. This is what the user sees, what they get when they `SELECT * FROM table`, etc.
            // - The "virtual" file schema: this is the table schema minus any hive partition columns and projections. This is what the file schema is coerced to.
            // - The physical file schema: this is the schema as defined by the parquet file. This is what the parquet file actually contains.
            let mut physical_file_schema = Arc::clone(reader_metadata.schema());

            // The schema loaded from the file may not be the same as the
            // desired schema (for example if we want to instruct the parquet
            // reader to read strings using Utf8View instead). Update if necessary
            if let Some(merged) =
                apply_file_schema_type_coercions(&table_schema, &physical_file_schema)
            {
                physical_file_schema = Arc::new(merged);
                options = options.with_schema(Arc::clone(&physical_file_schema));
                reader_metadata = ArrowReaderMetadata::try_new(
                    Arc::clone(reader_metadata.metadata()),
                    options.clone(),
                )?;
            }

            if coerce_int96.is_some() {
                if let Some(merged) = coerce_int96_to_resolution(
                    reader_metadata.parquet_schema(),
                    &physical_file_schema,
                    &(coerce_int96.unwrap()),
                ) {
                    physical_file_schema = Arc::new(merged);
                    options = options.with_schema(Arc::clone(&physical_file_schema));
                    reader_metadata = ArrowReaderMetadata::try_new(
                        Arc::clone(reader_metadata.metadata()),
                        options.clone(),
                    )?;
                }
            }

            // Build predicates for this specific file
            let (pruning_predicate, page_pruning_predicate) = build_pruning_predicates(
                &predicate,
                &physical_file_schema,
                &predicate_creation_errors,
            );

            // The page index is not stored inline in the parquet footer so the
            // code above may not have read the page index structures yet. If we
            // need them for reading and they aren't yet loaded, we need to load them now.
            if should_enable_page_index(enable_page_index, &page_pruning_predicate) {
                reader_metadata = load_page_index(
                    reader_metadata,
                    &mut async_file_reader,
                    // Since we're manually loading the page index the option here should not matter but we pass it in for consistency
                    options.with_page_index(true),
                )
                .await?;
            }

            metadata_timer.stop();

            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                async_file_reader,
                reader_metadata,
            );

            let (schema_mapping, adapted_projections) =
                schema_adapter.map_schema(&physical_file_schema)?;

            // let mask = ProjectionMask::roots(
            //     builder.parquet_schema(),
            //     adapted_projections.iter().cloned(),
            // );
            let mask = if has_deep_projection(Some(projection_deep.clone().as_ref())) {
                let leaves = generate_leaf_paths(
                    table_schema.clone(),
                    builder.parquet_schema(),
                    &projection_vec,
                    projection_deep.clone().as_ref(),
                );
                info!(
                    "ParquetOpener::open, using deep projection parquet leaves: {:?}",
                    leaves.clone()
                );
                // let tmp = builder.parquet_schema();
                // for (i, col) in tmp.columns().iter().enumerate() {
                //     info!("  {}  {}= {:?}", i, col.path(), col);
                // }
                ProjectionMask::leaves(builder.parquet_schema(), leaves)
            } else {
                info!(
                    "ParquetOpener::open, using root projections: {:?}",
                    &adapted_projections
                );

                ProjectionMask::roots(
                    builder.parquet_schema(),
                    adapted_projections.iter().cloned(),
                )
            };

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    &physical_file_schema,
                    &table_schema,
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                    &schema_adapter_factory,
                );

                match row_filter {
                    Ok(Some(filter)) => {
                        builder = builder.with_row_filter(filter);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!(
                            "Ignoring error building row filter for '{:?}': {}",
                            predicate, e
                        );
                    }
                };
            };

            // Determine which row groups to actually read. The idea is to skip
            // as many row groups as possible based on the metadata and query
            let file_metadata = Arc::clone(builder.metadata());
            let predicate = pruning_predicate.as_ref().map(|p| p.as_ref());
            let rg_metadata = file_metadata.row_groups();
            // track which row groups to actually read
            let access_plan =
                create_initial_plan(&file_name, extensions, rg_metadata.len())?;
            let mut row_groups = RowGroupAccessPlanFilter::new(access_plan);
            // if there is a range restricting what parts of the file to read
            if let Some(range) = file_range.as_ref() {
                row_groups.prune_by_range(rg_metadata, range);
            }
            // If there is a predicate that can be evaluated against the metadata
            if let Some(predicate) = predicate.as_ref() {
                if enable_row_group_stats_pruning {
                    row_groups.prune_by_statistics(
                        &physical_file_schema,
                        builder.parquet_schema(),
                        rg_metadata,
                        predicate,
                        &file_metrics,
                    );
                }

                if enable_bloom_filter && !row_groups.is_empty() {
                    row_groups
                        .prune_by_bloom_filters(
                            &physical_file_schema,
                            &mut builder,
                            predicate,
                            &file_metrics,
                        )
                        .await;
                }
            }

            let mut access_plan = row_groups.build();

            // page index pruning: if all data on individual pages can
            // be ruled using page metadata, rows from other columns
            // with that range can be skipped as well
            if enable_page_index && !access_plan.is_empty() {
                if let Some(p) = page_pruning_predicate {
                    access_plan = p.prune_plan_with_page_index(
                        access_plan,
                        &physical_file_schema,
                        builder.parquet_schema(),
                        file_metadata.as_ref(),
                        &file_metrics,
                    );
                }
            }

            let row_group_indexes = access_plan.row_group_indexes();
            if let Some(row_selection) =
                access_plan.into_overall_row_selection(rg_metadata)?
            {
                builder = builder.with_row_selection(row_selection);
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_group_indexes)
                .build()?;

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch
                        .and_then(|b| schema_mapping.map_batch(b).map_err(Into::into))
                });

            Ok(adapted.boxed())
        }))
    }
}

/// Return the initial [`ParquetAccessPlan`]
///
/// If the user has supplied one as an extension, use that
/// otherwise return a plan that scans all row groups
///
/// Returns an error if an invalid `ParquetAccessPlan` is provided
///
/// Note: file_name is only used for error messages
fn create_initial_plan(
    file_name: &str,
    extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
    row_group_count: usize,
) -> Result<ParquetAccessPlan> {
    if let Some(extensions) = extensions {
        if let Some(access_plan) = extensions.downcast_ref::<ParquetAccessPlan>() {
            let plan_len = access_plan.len();
            if plan_len != row_group_count {
                return exec_err!(
                    "Invalid ParquetAccessPlan for {file_name}. Specified {plan_len} row groups, but file has {row_group_count}"
                );
            }

            // check row group count matches the plan
            return Ok(access_plan.clone());
        } else {
            debug!("DataSourceExec Ignoring unknown extension specified for {file_name}");
        }
    }

    // default to scanning all row groups
    Ok(ParquetAccessPlan::new_all(row_group_count))
}

// FIXME: @HStack ACTUALLY look at the arrow schema and handle map types correctly
//  Right now, we are matching "map-like" parquet leaves like "key_value.key" etc
//  But, we neeed to walk through both the arrow schema (which KNOWS about the map type)
//  and the parquet leaves to do this correctly.
fn equivalent_projection_paths_from_parquet_schema(
    _arrow_schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> Vec<(usize, (String, String))> {
    let mut output: Vec<(usize, (String, String))> = vec![];
    for (i, col) in parquet_schema.columns().iter().enumerate() {
        let original_path = col.path().string();
        let converted_path =
            convert_parquet_path_to_deep_projection_path(&original_path.as_str());
        output.push((i, (original_path.clone(), converted_path)));
    }
    output
}

fn convert_parquet_path_to_deep_projection_path(parquet_path: &str) -> String {
    if parquet_path.contains(".key_value.key")
        || parquet_path.contains(".key_value.value")
        || parquet_path.contains(".entries.keys")
        || parquet_path.contains(".entries.values")
        || parquet_path.contains(".list.element")
    {
        let tmp = parquet_path
            .replace("key_value.key", "*")
            .replace("key_value.value", "*")
            .replace("entries.keys", "*")
            .replace("entries.values", "*")
            .replace("list.element", "*");
        tmp
    } else {
        parquet_path.to_string()
    }
}

fn generate_leaf_paths(
    arrow_schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
    projection: &Vec<usize>,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> Vec<usize> {
    let actual_projection = if projection.len() == 0 {
        (0..arrow_schema.fields().len()).collect()
    } else {
        projection.clone()
    };
    let splatted =
        splat_columns(arrow_schema.clone(), &actual_projection, &projection_deep);
    trace!(target: "deep", "generate_leaf_paths: splatted: {:?}", &splatted);

    let mut out: Vec<usize> = vec![];
    for (i, (original, converted)) in
        equivalent_projection_paths_from_parquet_schema(arrow_schema, parquet_schema)
    {
        // FIXME: @HStack
        //  for map fields, the actual parquet paths look like x.y.z.key_value.key, x.y.z.key_value.value
        //  since we are ignoring these names in the paths, we need to actually collapse this access to a *
        //  so we can filter for them
        //  also, we need BOTH the key and the value for maps otherwise we run into an arrow-rs error
        //  "partial projection of MapArray is not supported"

        trace!(target: "deep", "  generate_leaf_paths looking at index {} {} =  {}", i, &original, &converted);

        let mut found = false;
        for filter in splatted.iter() {
            // check if this filter matches this leaf path
            let filter_pieces = filter.split(".").collect::<Vec<&str>>();
            // let col_pieces = col_path.parts();
            let col_pieces = converted.split(".").collect::<Vec<_>>();
            // let's check
            let mut filter_found = true;
            for i in 0..min(filter_pieces.len(), col_pieces.len()) {
                if i >= filter_pieces.len() {
                    //  we are at the end of the filter, and we matched until now, so we break, we match !
                    break;
                }
                if i >= col_pieces.len() {
                    // we have a longer filter, we matched until now, we match !
                    break;
                }
                // we can actually check
                if !(col_pieces[i] == filter_pieces[i] || filter_pieces[i] == "*") {
                    filter_found = false;
                    break;
                }
            }
            if filter_found {
                found = true;
                break;
            }
        }
        if found {
            out.push(i);
        }
    }
    out
}

/// Build a pruning predicate from an optional predicate expression.
/// If the predicate is None or the predicate cannot be converted to a pruning
/// predicate, return None.
/// If there is an error creating the pruning predicate it is recorded by incrementing
/// the `predicate_creation_errors` counter.
pub(crate) fn build_pruning_predicate(
    predicate: Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
    predicate_creation_errors: &Count,
) -> Option<Arc<PruningPredicate>> {
    match PruningPredicate::try_new(predicate, Arc::clone(file_schema)) {
        Ok(pruning_predicate) => {
            if !pruning_predicate.always_true() {
                return Some(Arc::new(pruning_predicate));
            }
        }
        Err(e) => {
            debug!("Could not create pruning predicate for: {e}");
            predicate_creation_errors.add(1);
        }
    }
    None
}

/// Build a page pruning predicate from an optional predicate expression.
/// If the predicate is None or the predicate cannot be converted to a page pruning
/// predicate, return None.
pub(crate) fn build_page_pruning_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
) -> Arc<PagePruningAccessPlanFilter> {
    Arc::new(PagePruningAccessPlanFilter::new(
        predicate,
        Arc::clone(file_schema),
    ))
}

fn build_pruning_predicates(
    predicate: &Option<Arc<dyn PhysicalExpr>>,
    file_schema: &SchemaRef,
    predicate_creation_errors: &Count,
) -> (
    Option<Arc<PruningPredicate>>,
    Option<Arc<PagePruningAccessPlanFilter>>,
) {
    let Some(predicate) = predicate.as_ref() else {
        return (None, None);
    };
    let pruning_predicate = build_pruning_predicate(
        Arc::clone(predicate),
        file_schema,
        predicate_creation_errors,
    );
    let page_pruning_predicate = build_page_pruning_predicate(predicate, file_schema);
    (pruning_predicate, Some(page_pruning_predicate))
}

/// Returns a `ArrowReaderMetadata` with the page index loaded, loading
/// it from the underlying `AsyncFileReader` if necessary.
async fn load_page_index<T: AsyncFileReader>(
    reader_metadata: ArrowReaderMetadata,
    input: &mut T,
    options: ArrowReaderOptions,
) -> Result<ArrowReaderMetadata> {
    let parquet_metadata = reader_metadata.metadata();
    let missing_column_index = parquet_metadata.column_index().is_none();
    let missing_offset_index = parquet_metadata.offset_index().is_none();
    // You may ask yourself: why are we even checking if the page index is already loaded here?
    // Didn't we explicitly *not* load it above?
    // Well it's possible that a custom implementation of `AsyncFileReader` gives you
    // the page index even if you didn't ask for it (e.g. because it's cached)
    // so it's important to check that here to avoid extra work.
    if missing_column_index || missing_offset_index {
        let m = Arc::try_unwrap(Arc::clone(parquet_metadata))
            .unwrap_or_else(|e| e.as_ref().clone());
        let mut reader =
            ParquetMetaDataReader::new_with_metadata(m).with_page_indexes(true);
        reader.load_page_index(input).await?;
        let new_parquet_metadata = reader.finish()?;
        let new_arrow_reader =
            ArrowReaderMetadata::try_new(Arc::new(new_parquet_metadata), options)?;
        Ok(new_arrow_reader)
    } else {
        // No need to load the page index again, just return the existing metadata
        Ok(reader_metadata)
    }
}
