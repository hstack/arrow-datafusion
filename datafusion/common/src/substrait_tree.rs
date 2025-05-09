use crate::tree_node::{Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion};
use crate::{DataFusionError, Result};
use substrait::proto::{
    rel::RelType, AggregateRel, ConsistentPartitionWindowRel, CrossRel, DdlRel,
    ExchangeRel, ExpandRel, ExtensionMultiRel, ExtensionSingleRel, FetchRel, FilterRel,
    HashJoinRel, JoinRel, MergeJoinRel, NestedLoopJoinRel, ProjectRel, Rel, SetRel,
    SortRel, WriteRel,
};

fn inputs(rel: &Rel) -> Vec<&Rel> {
    match &rel.rel_type {
        Some(rel_type) => match rel_type {
            RelType::Read(_) => vec![],
            RelType::Project(project_rel) => {
                project_rel.input.as_deref().into_iter().collect()
            }
            RelType::Filter(filter_rel) => {
                filter_rel.input.as_deref().into_iter().collect()
            }
            RelType::Fetch(fetch_rel) => fetch_rel.input.as_deref().into_iter().collect(),
            RelType::Aggregate(aggregate_rel) => {
                aggregate_rel.input.as_deref().into_iter().collect()
            }
            RelType::Sort(sort_rel) => sort_rel.input.as_deref().into_iter().collect(),
            // FIXME
            RelType::Join(join_rel) => {
                let mut output: Vec<&Rel> = vec![];
                if let Some(left) = join_rel.left.as_ref() {
                    output.push(left.as_ref());
                }
                if let Some(right) = join_rel.right.as_ref() {
                    output.push(right.as_ref());
                }
                output
            }
            RelType::Set(set_rel) => set_rel.inputs.iter().map(|input| input).collect(),
            RelType::ExtensionSingle(extension_single_rel) => {
                extension_single_rel.input.as_deref().into_iter().collect()
            }
            RelType::ExtensionMulti(extension_multi_rel) => extension_multi_rel
                .inputs
                .iter()
                .map(|input| input)
                .collect(),
            RelType::ExtensionLeaf(_) => vec![],
            RelType::Cross(cross_rel) => {
                let mut output: Vec<&Rel> = vec![];
                if let Some(left) = cross_rel.left.as_ref() {
                    output.push(left.as_ref());
                }
                if let Some(right) = cross_rel.right.as_ref() {
                    output.push(right.as_ref());
                }
                output
            }
            RelType::Exchange(exchange_rel) => {
                exchange_rel.input.as_deref().into_iter().collect()
            }
            // FIXME - add all the others
            RelType::Reference(ref_rel) => vec![],
            RelType::Write(write_rel) => write_rel.input.as_deref().into_iter().collect(),
            RelType::Ddl(ddl_rel) => {
                ddl_rel.view_definition.as_deref().into_iter().collect()
            }
            RelType::HashJoin(hash_join_rel) => {
                let mut output: Vec<&Rel> = vec![];
                if let Some(left) = hash_join_rel.left.as_ref() {
                    output.push(left.as_ref());
                }
                if let Some(right) = hash_join_rel.right.as_ref() {
                    output.push(right.as_ref());
                }
                output
            }
            RelType::MergeJoin(merge_join_rel) => {
                let mut output: Vec<&Rel> = vec![];
                if let Some(left) = merge_join_rel.left.as_ref() {
                    output.push(left.as_ref());
                }
                if let Some(right) = merge_join_rel.right.as_ref() {
                    output.push(right.as_ref());
                }
                output
            }
            RelType::NestedLoopJoin(nested_loop_join) => {
                let mut output: Vec<&Rel> = vec![];
                if let Some(left) = nested_loop_join.left.as_ref() {
                    output.push(left.as_ref());
                }
                if let Some(right) = nested_loop_join.right.as_ref() {
                    output.push(right.as_ref());
                }
                output
            }
            RelType::Window(window_rel) => {
                window_rel.input.as_deref().into_iter().collect()
            }
            RelType::Expand(expand_rel) => {
                expand_rel.input.as_deref().into_iter().collect()
            }
            RelType::Update(update_rel) => vec![],
        },
        None => vec![],
    }
}

fn transform_box<F: FnMut(Rel) -> Result<Transformed<Rel>>>(
    br: Box<Rel>,
    f: &mut F,
) -> Result<Transformed<Box<Rel>>> {
    Ok(f(*br)?.update_data(Box::new))
}

fn transform_option_box<F: FnMut(Rel) -> Result<Transformed<Rel>>>(
    obr: Option<Box<Rel>>,
    f: &mut F,
) -> Result<Transformed<Option<Box<Rel>>>> {
    obr.map_or(Ok(Transformed::no(None)), |be| {
        Ok(transform_box(be, f)?.update_data(Some))
    })
}

impl TreeNode for Rel {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        inputs(self).into_iter().apply_until_stop(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        if let Some(rel_type) = self.rel_type {
            let t = match rel_type {
                RelType::Read(_) => Transformed::no(rel_type),
                RelType::Project(p) => {
                    let ProjectRel {
                        common,
                        input,
                        expressions,
                        advanced_extension,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Project(Box::new(ProjectRel {
                            common,
                            input,
                            expressions,
                            advanced_extension,
                        }))
                    })
                }
                RelType::Filter(p) => {
                    let FilterRel {
                        common,
                        input,
                        condition,
                        advanced_extension,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Filter(Box::new(FilterRel {
                            common,
                            input,
                            condition,
                            advanced_extension,
                        }))
                    })
                }

                RelType::Fetch(p) => {
                    let FetchRel {
                        common,
                        input,
                        advanced_extension,
                        offset_mode,
                        count_mode,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Fetch(Box::new(FetchRel {
                            common,
                            input,
                            advanced_extension,
                            offset_mode,
                            count_mode,
                        }))
                    })
                }
                RelType::Aggregate(p) => {
                    let AggregateRel {
                        common,
                        input,
                        groupings,
                        measures,
                        grouping_expressions,
                        advanced_extension,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Aggregate(Box::new(AggregateRel {
                            common,
                            input,
                            groupings,
                            measures,
                            grouping_expressions,
                            advanced_extension,
                        }))
                    })
                }
                RelType::Sort(p) => {
                    let SortRel {
                        common,
                        input,
                        sorts,
                        advanced_extension,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Sort(Box::new(SortRel {
                            common,
                            input,
                            sorts,
                            advanced_extension,
                        }))
                    })
                }
                // FIXME
                RelType::Set(p) => {
                    let SetRel {
                        common,
                        inputs,
                        op,
                        advanced_extension,
                    } = p;
                    let mut transformed_any = false;
                    let new_inputs: Vec<_> = inputs
                        .into_iter()
                        .map(|input| {
                            let transformed =
                                transform_box(Box::new(input), &mut f).unwrap();
                            if transformed.transformed {
                                transformed_any = true;
                            }
                            *transformed.data
                        })
                        .collect();
                    if transformed_any {
                        Transformed::yes(RelType::Set(SetRel {
                            common,
                            inputs: new_inputs,
                            op,
                            advanced_extension,
                        }))
                    } else {
                        Transformed::no(RelType::Set(SetRel {
                            common,
                            inputs: new_inputs,
                            op,
                            advanced_extension,
                        }))
                    }
                }
                RelType::ExtensionSingle(p) => {
                    let ExtensionSingleRel {
                        common,
                        input,
                        detail,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::ExtensionSingle(Box::new(ExtensionSingleRel {
                            common,
                            input,
                            detail,
                        }))
                    })
                }
                RelType::ExtensionMulti(p) => {
                    let ExtensionMultiRel {
                        common,
                        inputs,
                        detail,
                    } = p;
                    let mut transformed_any = false;
                    let new_inputs: Vec<Rel> = inputs
                        .into_iter()
                        .map(|input| {
                            let transformed =
                                transform_box(Box::new(input), &mut f).unwrap();
                            if transformed.transformed {
                                transformed_any = true;
                            }
                            *transformed.data
                        })
                        .collect();
                    if transformed_any {
                        Transformed::yes(RelType::ExtensionMulti(ExtensionMultiRel {
                            common,
                            inputs: new_inputs,
                            detail,
                        }))
                    } else {
                        Transformed::no(RelType::ExtensionMulti(ExtensionMultiRel {
                            common,
                            inputs: new_inputs,
                            detail,
                        }))
                    }
                }
                RelType::Join(p) => {
                    let JoinRel {
                        common,
                        left,
                        right,
                        expression,
                        post_join_filter,
                        r#type,
                        advanced_extension,
                    } = *p;
                    let mut transformed_any = false;
                    let new_left = transform_option_box(left, &mut f)?;
                    if new_left.transformed {
                        transformed_any = true;
                    }
                    let new_right = transform_option_box(right, &mut f)?;
                    if new_right.transformed {
                        transformed_any = true;
                    }

                    if transformed_any {
                        Transformed::yes(RelType::Join(Box::new(JoinRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            expression,
                            post_join_filter,
                            r#type,
                            advanced_extension,
                        })))
                    } else {
                        Transformed::no(RelType::Join(Box::new(JoinRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            expression,
                            post_join_filter,
                            r#type,
                            advanced_extension,
                        })))
                    }
                }
                RelType::ExtensionLeaf(inner) => {
                    Transformed::no(RelType::ExtensionLeaf(inner))
                }
                RelType::Cross(p) => {
                    let CrossRel {
                        common,
                        left,
                        right,
                        advanced_extension,
                    } = *p;
                    let mut transformed_any = false;
                    let new_left = transform_option_box(left, &mut f)?;
                    if new_left.transformed {
                        transformed_any = true;
                    }
                    let new_right = transform_option_box(right, &mut f)?;
                    if new_right.transformed {
                        transformed_any = true;
                    }

                    if transformed_any {
                        Transformed::yes(RelType::Cross(Box::new(CrossRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            advanced_extension,
                        })))
                    } else {
                        Transformed::no(RelType::Cross(Box::new(CrossRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            advanced_extension,
                        })))
                    }
                }
                RelType::Reference(inner) => Transformed::no(RelType::Reference(inner)),
                RelType::Write(p) => {
                    let WriteRel {
                        table_schema,
                        op,
                        input,
                        create_mode,
                        output,
                        common,
                        advanced_extension,
                        write_type,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Write(Box::new(WriteRel {
                            table_schema,
                            op,
                            input,
                            create_mode,
                            output,
                            common,
                            advanced_extension,
                            write_type,
                        }))
                    })
                }
                RelType::Ddl(p) => {
                    let DdlRel {
                        table_schema,
                        table_defaults,
                        object,
                        op,
                        view_definition,
                        common,
                        advanced_extension,
                        write_type,
                    } = *p;
                    transform_option_box(view_definition, &mut f)?.update_data(|input| {
                        RelType::Ddl(Box::new(DdlRel {
                            table_schema,
                            table_defaults,
                            object,
                            op,
                            view_definition: input,
                            common,
                            advanced_extension,
                            write_type,
                        }))
                    })
                }
                RelType::HashJoin(p) => {
                    let HashJoinRel {
                        common,
                        left,
                        right,
                        left_keys,
                        right_keys,
                        keys,
                        post_join_filter,
                        r#type,
                        advanced_extension,
                    } = *p;
                    let mut transformed_any = false;
                    let new_left = transform_option_box(left, &mut f)?;
                    if new_left.transformed {
                        transformed_any = true;
                    }
                    let new_right = transform_option_box(right, &mut f)?;
                    if new_right.transformed {
                        transformed_any = true;
                    }

                    if transformed_any {
                        Transformed::yes(RelType::HashJoin(Box::new(HashJoinRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            left_keys,
                            right_keys,
                            keys,
                            post_join_filter,
                            r#type,
                            advanced_extension,
                        })))
                    } else {
                        Transformed::no(RelType::HashJoin(Box::new(HashJoinRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            left_keys,
                            right_keys,
                            keys,
                            post_join_filter,
                            r#type,
                            advanced_extension,
                        })))
                    }
                }
                RelType::MergeJoin(p) => {
                    let MergeJoinRel {
                        common,
                        left,
                        right,
                        left_keys,
                        right_keys,
                        keys,
                        post_join_filter,
                        r#type,
                        advanced_extension,
                    } = *p;
                    let mut transformed_any = false;
                    let new_left = transform_option_box(left, &mut f)?;
                    if new_left.transformed {
                        transformed_any = true;
                    }
                    let new_right = transform_option_box(right, &mut f)?;
                    if new_right.transformed {
                        transformed_any = true;
                    }

                    if transformed_any {
                        Transformed::yes(RelType::MergeJoin(Box::new(MergeJoinRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            left_keys,
                            right_keys,
                            keys,
                            post_join_filter,
                            r#type,
                            advanced_extension,
                        })))
                    } else {
                        Transformed::no(RelType::MergeJoin(Box::new(MergeJoinRel {
                            common,
                            left: new_left.data,
                            right: new_right.data,
                            left_keys,
                            right_keys,
                            keys,
                            post_join_filter,
                            r#type,
                            advanced_extension,
                        })))
                    }
                }
                RelType::NestedLoopJoin(p) => {
                    let NestedLoopJoinRel {
                        common,
                        left,
                        right,
                        expression,
                        r#type,
                        advanced_extension,
                    } = *p;
                    let mut transformed_any = false;
                    let new_left = transform_option_box(left, &mut f)?;
                    if new_left.transformed {
                        transformed_any = true;
                    }
                    let new_right = transform_option_box(right, &mut f)?;
                    if new_right.transformed {
                        transformed_any = true;
                    }

                    if transformed_any {
                        Transformed::yes(RelType::NestedLoopJoin(Box::new(
                            NestedLoopJoinRel {
                                common,
                                left: new_left.data,
                                right: new_right.data,
                                expression,
                                r#type,
                                advanced_extension,
                            },
                        )))
                    } else {
                        Transformed::no(RelType::NestedLoopJoin(Box::new(
                            NestedLoopJoinRel {
                                common,
                                left: new_left.data,
                                right: new_right.data,
                                expression,
                                r#type,
                                advanced_extension,
                            },
                        )))
                    }
                }
                RelType::Window(p) => {
                    let ConsistentPartitionWindowRel {
                        common,
                        input,
                        window_functions,
                        partition_expressions,
                        sorts,
                        advanced_extension,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Window(Box::new(ConsistentPartitionWindowRel {
                            common,
                            input,
                            window_functions,
                            partition_expressions,
                            sorts,
                            advanced_extension,
                        }))
                    })
                }
                RelType::Exchange(p) => {
                    let ExchangeRel {
                        common,
                        input,
                        partition_count,
                        targets,
                        advanced_extension,
                        exchange_kind,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Exchange(Box::new(ExchangeRel {
                            common,
                            input,
                            partition_count,
                            targets,
                            advanced_extension,
                            exchange_kind,
                        }))
                    })
                }
                RelType::Expand(p) => {
                    let ExpandRel {
                        common,
                        input,
                        fields,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Expand(Box::new(ExpandRel {
                            common,
                            input,
                            fields,
                        }))
                    })
                }
                RelType::Update(_) => Transformed::no(rel_type),
            };
            Ok(t.update_data(|rt| Rel { rel_type: Some(rt) }))
        } else {
            Err(DataFusionError::Plan("RelType is None".into()))
        }
    }
}
