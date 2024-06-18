use crate::datasource::schema_adapter::{SchemaAdapter, SchemaMapper};
use arrow::compute::{can_cast_types, cast};
use arrow_array::cast::{
    as_fixed_size_list_array, as_generic_list_array, as_struct_array,
};
use arrow_array::{new_null_array, Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, RecordBatch, StructArray, RecordBatchOptions, MapArray};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion_common::plan_err;
use datafusion_common::DataFusionError;
use std::sync::Arc;
use datafusion_common::cast::as_map_array;

#[cfg(feature = "parquet")]
impl NestedSchemaAdapter {
    fn map_schema_nested(
        &self,
        fields: &Fields,
    ) -> datafusion_common::Result<(Arc<NestedSchemaMapping>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(fields.len());
        // start from the destination fields
        for (table_idx, table_field) in self.table_schema.fields.iter().enumerate() {
            // if the file exists in the source, check if we can rewrite it to the destination,
            // and add it to the projections
            if let Some((file_idx, file_field)) =
                fields.find(table_field.name())
            {
                if can_rewrite_field(table_field.clone(), file_field.clone(), true) {
                    projection.push(file_idx);
                } else {
                    return plan_err!(
                        "Cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                        file_field.name(),
                        file_field.data_type(),
                        table_field.data_type()
                    );
                }
            }
        }
        Ok((
            Arc::new(NestedSchemaMapping {
                table_schema: self.table_schema.clone(),
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

fn data_type_recurs(dt: &DataType) -> bool {
    match dt {
        // scalars
        DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float16 | DataType::Float32 | DataType::Float64 |
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 |
        DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) | DataType::Interval(_) |
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary | DataType::BinaryView |
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View |
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) |
        DataType::Dictionary(_, _) =>
            false,
        // containers
        DataType::RunEndEncoded(_, val) => data_type_recurs(val.data_type()),
        DataType::Union(_, _) => true,
        DataType::List(f) => data_type_recurs(f.data_type()),
        DataType::ListView(f) => data_type_recurs(f.data_type()),
        DataType::FixedSizeList(f, _) => data_type_recurs(f.data_type()),
        DataType::LargeList(f) => data_type_recurs(f.data_type()),
        DataType::LargeListView(f) => data_type_recurs(f.data_type()),
        // list of struct
        DataType::Map(_, _) |
        DataType::Struct(_) =>
            true,
    }
}

fn try_rewrite_record_batch(
    src: SchemaRef,
    src_record_batch: RecordBatch,
    dst: SchemaRef,
    fill_missing_source_fields: bool,
    error_on_missing_source_fields: bool,
) -> datafusion_common::Result<RecordBatch> {
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

            (DataType::Map(src_inner, _), DataType::Map(dst_inner, dst_ordered))  => {
                match (src_inner.data_type(), dst_inner.data_type()) {
                    (DataType::Struct(src_inner_f), DataType::Struct(dst_inner_f)) => {
                        let src_map = as_map_array(src_array.as_ref())?;
                        let src_nulls = match src_map.nulls() {
                            None => None,
                            Some(x) => Some(x.clone()),
                        };
                        let src_offset_buffer = src_map.offsets().clone();

                        let (tmp_array, tmp_field) = recurse_field(
                            dst_inner.clone(),
                            src_inner.clone(),
                            src_map.values().clone(),
                            num_rows,
                            fill_missing_source_fields,
                            error_on_missing_source_fields
                        )?;
                        let out_arr = MapArray::try_new(
                            tmp_field.clone(),
                            src_offset_buffer,
                            as_struct_array(tmp_array.as_ref()).clone(),
                            src_nulls,
                            *dst_ordered
                        )?;

                        Ok((Arc::new(out_arr), dst_field.clone()))
                    }
                    _ => unreachable!() // unreachable
                }
            },

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

// called for fields - does the name resolution
fn can_rewrite_fields(
    dst_fields: &Fields,
    src_fields: &Fields,
    fill_missing_source_fields: bool,
) -> bool {
    let mut out = true;
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
            let can_cast = can_rewrite_field(
                dst_field,
                src_field,
                fill_missing_source_fields,
            );
            out = out && can_cast;
        } else {
            out = out && fill_missing_source_fields;
        }
    }
    out
}

fn can_rewrite_field(
    dst_field: FieldRef,
    src_field: FieldRef,
    fill_missing_source_fields: bool,
) -> bool {
    let can_cast_by_arrow =
        !data_type_recurs(dst_field.data_type()) &&
            !data_type_recurs(src_field.data_type());
    if can_cast_by_arrow {
        return can_cast_types(src_field.data_type(), dst_field.data_type());
    }
    match (src_field.data_type(), dst_field.data_type()) {
        (DataType::List(src_inner), DataType::List(dst_inner)) |
        (DataType::List(src_inner), DataType::LargeList(dst_inner)) |
        (DataType::LargeList(src_inner), DataType::LargeList(dst_inner)) => {
            if data_type_recurs(src_inner.data_type()) && data_type_recurs(dst_inner.data_type())  {
                return can_rewrite_field(
                    dst_inner.clone(),
                    src_inner.clone(),
                    fill_missing_source_fields,
                );
            } else {
                return can_cast_types(src_inner.data_type(), dst_inner.data_type());
            }
        }
        (
            DataType::FixedSizeList(src_inner, src_sz),
            DataType::FixedSizeList(dst_inner, dst_sz),
        ) => {
            if src_sz != dst_sz {
                return false
            }
            if data_type_recurs(src_inner.data_type()) && data_type_recurs(dst_inner.data_type())  {
                return can_rewrite_field(
                    dst_inner.clone(),
                    src_inner.clone(),
                    fill_missing_source_fields,
                );
            } else {
                return can_cast_types(src_inner.data_type(), dst_inner.data_type());
            }
        }
        (DataType::Map(src_inner, _), DataType::Map(dst_inner, _)) => {
            return can_rewrite_field(
                dst_inner.clone(),
                src_inner.clone(),
                fill_missing_source_fields,
            );
        }
        (DataType::Struct(src_inner), DataType::Struct(dst_inner)) => {
            return can_rewrite_fields(
                dst_inner,
                src_inner,
                fill_missing_source_fields,
            );
        }
        (src, dest) => {
            false
        }
    }
}

fn can_rewrite(
    src: SchemaRef,
    dst: SchemaRef,
    fill_missing_source_fields: bool,
) -> bool {


    can_rewrite_fields(
        dst.fields(),
        src.fields(),
        fill_missing_source_fields,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow_array::builder::{GenericStringBuilder, ListBuilder};
    use arrow_array::{BooleanArray, RecordBatch, StringArray, StructArray, UInt32Array};
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;
    use crate::datasource::schema_adapter_deep::{can_rewrite, try_rewrite_record_batch};

    #[test]
    fn test_cast() -> crate::error::Result<()> {
        // source, destination, is_fill_dependent
        let cases = vec![
            (
                Arc::new(Schema::new(vec![
                    Field::new("i1", DataType::Int32, true),
                ])),

                Arc::new(Schema::new(vec![
                    Field::new("i1", DataType::Int8, true),
                ])),
                false,
                true
            ),
            (
                Arc::new(Schema::new(vec![
                    Field::new("i1", DataType::Int32, true),
                ])),

                Arc::new(Schema::new(vec![
                    Field::new("i1", DataType::Struct(Fields::from(vec![
                        Field::new("s1", DataType::Utf8, true)
                    ])), true),
                ])),
                false,
                false
            ),
            (
                Arc::new(Schema::new(vec![
                    Field::new("l1", DataType::List(
                        Arc::new(Field::new("s1", DataType::Struct(
                            Fields::from(vec![
                                Field::new("s1extra1", DataType::Utf8, true),
                                Field::new("s1extra2", DataType::Utf8, true),
                                Field::new("s1i2", DataType::Int32, true),
                                Field::new("s1s1", DataType::Utf8, true),
                                Field::new("s1m1", DataType::Map(
                                    Arc::new(
                                        Field::new(
                                            "entries",
                                            DataType::Struct(Fields::from(vec![
                                                Field::new("key", DataType::Utf8, false),
                                                Field::new("value", DataType::Utf8, false),
                                            ])),
                                            true
                                        )
                                    ),
                                    false,
                                ), true),
                                Field::new("s1l1", DataType::List(
                                    Arc::new(Field::new("s1l1i1", DataType::Int32, true))
                                ), true),
                            ]
                        )), true))
                    ), true),
                ])),

                Arc::new(Schema::new(vec![
                    Field::new("l1", DataType::List(
                        Arc::new(Field::new("s1", DataType::Struct(
                            Fields::from(vec![
                                Field::new("s1s1", DataType::Utf8, true),
                                Field::new("s1i2", DataType::Int32, true),
                                Field::new("s1m1", DataType::Map(
                                    Arc::new(
                                        Field::new(
                                            "entries",
                                            DataType::Struct(Fields::from(vec![
                                                Field::new("key", DataType::Utf8, false),
                                                Field::new("value", DataType::Utf8, false),
                                            ])),
                                            true
                                        )
                                    ),
                                    false,
                                ), true),
                                Field::new("s1l1", DataType::List(
                                    Arc::new(Field::new("s1l1i1", DataType::Date32, true))
                                ), true),
                                // extra field
                                Field::new("s1ts1", DataType::Time32(TimeUnit::Second), true),
                            ]
                            )), true))
                    ), true),
                ])),
                true,
                true
            ),
        ];
        for (from, to, can_fill, res) in cases.iter() {
            assert_eq!(can_rewrite(from.clone(), to.clone(), *can_fill), *res, "Wrong result");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_rewrite() -> crate::error::Result<()> {
        let _ = env_logger::try_init();

        let message_type = "
        message schema {
            REQUIRED INT32 int1;
            OPTIONAL INT32 int2;
            REQUIRED BYTE_ARRAY str1 (UTF8);
            OPTIONAL GROUP stringlist1 (LIST) {
                repeated group list {
                    optional BYTE_ARRAY element (UTF8);
                }
            }
            OPTIONAL group map1 (MAP) {
                REPEATED group map {
                  REQUIRED binary str (UTF8);
                  REQUIRED int32 num;
                }
            }
            OPTIONAL GROUP array_of_arrays (LIST) {
                REPEATED GROUP list {
                    REQUIRED GROUP element (LIST) {
                        REPEATED GROUP list {
                            REQUIRED INT32 element;
                        }
                    }
                }
            }
            REQUIRED GROUP array_of_struct (LIST) {
                REPEATED GROUP struct {
                    REQUIRED BOOLEAN bools;
                    REQUIRED INT32 uint32 (INTEGER(32,false));
                    REQUIRED GROUP   int32 (LIST) {
                        REPEATED GROUP list {
                            OPTIONAL INT32 element;
                        }
                    }
                }
            }
        }
        ";
        let message_type= r#"
            message schema {
                REQUIRED GROUP struct {
                    REQUIRED BINARY name (UTF8);
                    REQUIRED BOOLEAN bools;
                    REQUIRED INT32 uint32 (INTEGER(32,false));
                    REQUIRED GROUP tags (LIST) {
                        REPEATED GROUP tags {
                            OPTIONAL BINARY tag (UTF8);
                        }
                    }
                }
            }
        "#;
        let parquet_schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let arrow_schema = Arc::new(parquet_to_arrow_schema(parquet_schema.as_ref(), None).unwrap());
        println!("schema: {:#?}", arrow_schema);
        let (idx, ffield) = arrow_schema.fields().find("struct").unwrap();
        let struct_field = ffield.clone();
        let struct_fields = match struct_field.data_type() {
            DataType::Struct(fields) => Some(fields),
            _ => None
        }.unwrap();
        println!("struct fields: {:#?}", struct_fields);

        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder)
            .with_field(Field::new("tag", DataType::Utf8, true));
        expected_builder.values().append_value("foo");
        expected_builder.values().append_value("bar");
        expected_builder.append(true);
        expected_builder.values().append_value("bar");
        expected_builder.values().append_value("foo");
        expected_builder.append(true);
        let expected = expected_builder.finish();
        let struct_column = StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "name1",
                    "name2"
                ])),
                Arc::new(BooleanArray::from(vec![
                    true,
                    false
                ])),
                Arc::new(UInt32Array::from(vec![
                    1,
                    2
                ])),
                Arc::new(expected)
            ],
            None,
        );
        let record_batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(struct_column)]).unwrap();
        // println!("rb: {:#?}", record_batch);

        let message_type= r#"
            message schema {
                REQUIRED GROUP struct {
                    REQUIRED GROUP tags (LIST) {
                        REPEATED GROUP tags {
                            OPTIONAL BINARY tag (UTF8);
                        }
                    }
                }
            }
        "#;
        let parquet_schema_2 = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let arrow_schema_2 = Arc::new(parquet_to_arrow_schema(parquet_schema_2.as_ref(), None).unwrap());
        println!("arrow_schema_2: {:#?}", arrow_schema_2);
        let new_rb = try_rewrite_record_batch(arrow_schema.clone(), record_batch, arrow_schema_2.clone(), true, false).unwrap();
        println!("new_rb: {:#?}", new_rb);


        Ok(())
    }
}

