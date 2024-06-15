use crate::datasource::schema_adapter::{SchemaAdapter, SchemaMapper};
use arrow::compute::{can_cast_types, cast};
use arrow_array::cast::{
    as_fixed_size_list_array, as_generic_list_array, as_struct_array,
};
use arrow_array::{new_null_array, Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, RecordBatch, StructArray, RecordBatchOptions};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion_common::plan_err;
use datafusion_common::DataFusionError;
use std::sync::Arc;

#[cfg(feature = "parquet")]
impl NestedSchemaAdapter {
    fn map_schema_nested(
        &self,
        fields: &Fields,
    ) -> datafusion_common::Result<(Arc<NestedSchemaMapping>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(fields.len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];

        for (file_idx, file_field) in fields.iter().enumerate() {
            if let Some((table_idx, table_field)) =
                self.table_schema.fields().find(file_field.name())
            {
                let can_cast_original =
                    can_cast_types(file_field.data_type(), table_field.data_type());
                let can_cast_with_struct =
                    match (file_field.data_type(), table_field.data_type()) {
                        (DataType::Struct(ffields), DataType::Struct(tfields)) => {
                            let ffield_names = ffields
                                .iter()
                                .map(|ffield| ffield.name())
                                .collect::<Vec<_>>();
                            let all_fields_exist = tfields
                                .iter()
                                .all(|tfield| ffield_names.contains(&tfield.name()));
                            all_fields_exist
                        }
                        _ => can_cast_original,
                    };
                match can_cast_original || can_cast_with_struct {
                    true => {
                        field_mappings[table_idx] = Some(projection.len());
                        projection.push(file_idx);
                    }
                    false => {
                        return plan_err!(
                            "Cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                            file_field.name(),
                            file_field.data_type(),
                            table_field.data_type()
                        )
                    }
                }
            }
        }

        Ok((
            Arc::new(NestedSchemaMapping {
                table_schema: self.table_schema.clone(),
                field_mappings,
            }),
            projection,
        ))
    }
}

#[cfg(feature = "parquet")]
#[derive(Clone, Debug)]
pub(crate) struct NestedSchemaAdapter {
    /// Schema for the table
    pub table_schema: SchemaRef,
}

#[cfg(feature = "parquet")]
impl SchemaAdapter for NestedSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        self.map_schema_nested(file_schema.fields())
            .map(|(s, v)| (s as Arc<dyn SchemaMapper>, v))
    }
}

#[cfg(feature = "parquet")]
#[derive(Debug)]
pub struct NestedSchemaMapping {
    table_schema: SchemaRef,
    field_mappings: Vec<Option<usize>>,
}

#[cfg(feature = "parquet")]
impl SchemaMapper for NestedSchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let record_batch = try_rewrite_record_batch(batch.schema(), batch, self.table_schema.clone(), true, false)?;
        Ok(record_batch)
    }

    fn map_partial_batch(
        &self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch> {
        try_rewrite_record_batch(batch.schema().clone(), batch, self.table_schema.clone(), false, false)
    }
}

fn try_rewrite_record_batch(
    src: SchemaRef,
    src_record_batch: RecordBatch,
    dst: SchemaRef,
    fill_missing_source_fields: bool,
    error_on_missing_source_fields: bool,
) -> datafusion_common::Result<RecordBatch> {
    fn data_type_recurs(dt: &DataType) -> bool {
        match dt {
            DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float16 | DataType::Float32 | DataType::Float64 |
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 |
            DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) | DataType::Interval(_) |
            DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary | DataType::BinaryView |
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View |
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) |
            DataType::Dictionary(_, _) | DataType::Map(_, _) | DataType::RunEndEncoded(_, _) |
            DataType::Union(_, _) =>
                false,
            DataType::List(f) => data_type_recurs(f.data_type()),
            DataType::ListView(f) => data_type_recurs(f.data_type()),
            DataType::FixedSizeList(f, _) => data_type_recurs(f.data_type()),
            DataType::LargeList(f) => data_type_recurs(f.data_type()),
            DataType::LargeListView(f) => data_type_recurs(f.data_type()),
            DataType::Struct(_) =>
                true,
        }
    }
    // called for fields - does the name resolution
    fn recurse_fields(
        dst_fields: &Fields,
        src_fields: &Fields,
        arrays: Vec<ArrayRef>,
        num_rows: usize,
        fill_missing_source_fields: bool,
        error_on_missing_source_fields: bool,
    ) -> datafusion_common::Result<(Vec<ArrayRef>, Vec<FieldRef>)> {
        let mut out_arrays: Vec<ArrayRef> = vec![];
        let mut out_fields: Vec<FieldRef> = vec![];
        for i in 0..dst_fields.len() {
            let dst_field = dst_fields[i].clone();
            let dst_name = dst_field.name();

            let src_field_opt = src_fields
                .iter()
                .enumerate()
                .find(|(idx, b)| b.name() == dst_name);

            // if the field exists in the source
            if src_field_opt.is_some() {
                let (src_idx, src_field) = src_field_opt.unwrap();
                let src_field = src_field.clone();
                let src_arr = arrays[src_idx].clone();
                let (tmp_array, tmp_field) = recurse_field(
                    dst_field,
                    src_field,
                    src_arr,
                    num_rows,
                    fill_missing_source_fields,
                    error_on_missing_source_fields,
                )?;
                out_arrays.push(tmp_array);
                out_fields.push(tmp_field);
            } else {
                if fill_missing_source_fields {
                    let tmp_array = new_null_array(dst_field.data_type(), num_rows);
                    out_arrays.push(tmp_array);
                    out_fields.push(dst_field);
                } else if error_on_missing_source_fields {
                    return Err(datafusion_common::DataFusionError::Internal(
                        format!("field {dst_name} not found in source")
                    ));
                }
            }
        }
        Ok((out_arrays, out_fields))
    }
    fn cast_df(
        array: &dyn Array,
        to_type: &DataType,
    ) -> datafusion_common::Result<ArrayRef> {
        let tmp = cast(array, to_type);
        let tmp2: datafusion_common::Result<ArrayRef> = tmp.map_err(|ae| ae.into());
        return tmp2;
    }
    fn recurse_field(
        dst_field: FieldRef,
        src_field: FieldRef,
        src_array: ArrayRef,
        num_rows: usize,
        fill_missing_source_fields: bool,
        error_on_missing_source_fields: bool,
    ) -> datafusion_common::Result<(ArrayRef, FieldRef)> {
        let arrow_cast_available =
            can_cast_types(src_field.data_type(), dst_field.data_type());
        if arrow_cast_available {
            let casted_array = cast_df(src_array.as_ref(), dst_field.data_type())?;
            return Ok((casted_array, dst_field.clone()));
        }
        match (src_field.data_type(), dst_field.data_type()) {
            (DataType::List(src_inner), DataType::List(dst_inner)) => {
                if data_type_recurs(src_field.data_type()) {
                    let src_array_clone = src_array.clone();
                    let src_inner_list_array =
                        as_generic_list_array::<i32>(src_array_clone.as_ref()).clone();
                    let src_offset_buffer = src_inner_list_array.offsets().clone();
                    let src_nulls = match src_inner_list_array.nulls() {
                        None => None,
                        Some(x) => Some(x.clone()),
                    };
                    let (values, field) = recurse_field(
                        dst_inner.clone(),
                        src_inner.clone(),
                        src_inner_list_array.values().clone(),
                        num_rows,
                        fill_missing_source_fields,
                        error_on_missing_source_fields,
                    )?;
                    let nlarr = ListArray::try_new(
                        dst_field.clone(),
                        src_offset_buffer,
                        values,
                        src_nulls,
                    );
                    Ok((Arc::new(nlarr.unwrap()), field))
                } else {
                    let casted_array = cast_df(src_array.as_ref(), dst_field.data_type())?;
                    return Ok((casted_array, dst_field.clone()));
                }
            }
            (
                DataType::FixedSizeList(src_inner, src_sz),
                DataType::FixedSizeList(dst_inner, dst_sz),
            ) => {
                if src_sz != dst_sz {
                    // Let Arrow do its thing, it's going to error
                    let casted_array = cast_df(src_array.as_ref(), dst_field.data_type())?;
                    return Ok((casted_array, dst_field.clone()));
                }
                if data_type_recurs(src_field.data_type()) {
                    let tmp = src_array.clone();
                    let src_inner_list_array =
                        as_fixed_size_list_array(tmp.as_ref()).clone();
                    let src_nulls = match src_inner_list_array.nulls() {
                        None => None,
                        Some(x) => Some(x.clone()),
                    };
                    let (values, field) = recurse_field(
                        dst_inner.clone(),
                        src_inner.clone(),
                        src_inner_list_array.values().clone(),
                        num_rows,
                        fill_missing_source_fields,
                        error_on_missing_source_fields,
                    )?;

                    let nlarr = FixedSizeListArray::try_new(
                        dst_field.clone(),
                        *dst_sz,
                        values,
                        src_nulls,
                    );
                    Ok((Arc::new(nlarr.unwrap()), field))
                } else {
                    let casted_array = cast_df(src_array.as_ref(), dst_field.data_type())?;
                    return Ok((casted_array, dst_field.clone()));
                }
            }
            (DataType::LargeList(src_inner), DataType::LargeList(dst_inner)) => {
                if data_type_recurs(src_field.data_type()) {
                    let tmp = src_array.clone();
                    let src_inner_list_array =
                        as_generic_list_array::<i64>(tmp.as_ref()).clone();
                    let src_offset_buffer = src_inner_list_array.offsets().clone();
                    let src_nulls = match src_inner_list_array.nulls() {
                        None => None,
                        Some(x) => Some(x.clone()),
                    };
                    let (values, field) = recurse_field(
                        dst_inner.clone(),
                        src_inner.clone(),
                        src_inner_list_array.values().clone(),
                        num_rows,
                        fill_missing_source_fields,
                        error_on_missing_source_fields,
                    )?;

                    let nlarr = LargeListArray::try_new(
                        dst_field.clone(),
                        src_offset_buffer,
                        values,
                        src_nulls,
                    );
                    Ok((Arc::new(nlarr.unwrap()), field))
                } else {
                    let casted_array = cast_df(src_array.as_ref(), dst_field.data_type())?;
                    return Ok((casted_array, dst_field.clone()));
                }
            }
            (DataType::Struct(src_inner), DataType::Struct(dst_inner)) => {
                let src_struct_array = as_struct_array(src_array.as_ref());
                let src_nulls = match src_struct_array.nulls() {
                    None => None,
                    Some(x) => Some(x.clone()),
                };
                let src_columns = src_struct_array
                    .columns()
                    .iter()
                    .map(|a| a.clone())
                    .collect::<Vec<_>>();
                let (dst_columns, dst_fields) = recurse_fields(
                    dst_inner,
                    src_inner,
                    src_columns,
                    num_rows,
                    fill_missing_source_fields,
                    error_on_missing_source_fields,
                )?;
                let struct_array =
                    StructArray::try_new(dst_inner.clone(), dst_columns, src_nulls)
                        .map_err(|ae| DataFusionError::from(ae))?;
                let struct_field = Field::new_struct(dst_field.name(), dst_fields, dst_field.is_nullable());
                Ok((Arc::new(struct_array), Arc::new(struct_field)))
            }
            _ => {
                panic!()
            }
        }
    }

    let num_rows = src_record_batch.num_rows();
    let (final_columns, final_fields) = recurse_fields(
        dst.fields(),
        src.fields(),
        src_record_batch.columns().into(),
        num_rows,
        fill_missing_source_fields,
        error_on_missing_source_fields,
    )?;

    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    let schema = Arc::new(Schema::new(final_fields));
    let record_batch = RecordBatch::try_new_with_options(schema, final_columns, &options)?;
    Ok(record_batch)
}
