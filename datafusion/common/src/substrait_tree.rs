use crate::tree_node::{Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion};
use crate::{DataFusionError, Result};
use substrait::proto::{
    rel::RelType, AggregateRel, ExtensionSingleRel, FetchRel, FilterRel, ProjectRel, Rel,
    SortRel,
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
            // RelType::Join(join_rel) => {
            //     vec![join_rel.left.as_ref(), join_rel.right.as_ref()]
            // }
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
            // FIXME
            // RelType::Cross(cross_rel) => {
            //     vec![cross_rel.left.as_ref(), cross_rel.right.as_ref()]
            // }
            RelType::Exchange(exchange_rel) => {
                exchange_rel.input.as_deref().into_iter().collect()
            }
            // FIXME - add all the others
            _ => vec![],
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
                        offset,
                        count,
                        advanced_extension,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Fetch(Box::new(FetchRel {
                            common,
                            input,
                            offset,
                            count,
                            advanced_extension,
                        }))
                    })
                }
                RelType::Aggregate(p) => {
                    let AggregateRel {
                        common,
                        input,
                        groupings,
                        measures,
                        advanced_extension,
                    } = *p;
                    transform_option_box(input, &mut f)?.update_data(|input| {
                        RelType::Aggregate(Box::new(AggregateRel {
                            common,
                            input,
                            groupings,
                            measures,
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
                // RelType::Set(p) => {
                //     let SetRel { common, inputs, op, advanced_extension } = *p;
                //     let mut transformed_any = false;
                //     let new_inputs: std::result::Result<Vec<_>> = inputs
                //         .into_iter()
                //         .map(|input| {
                //             let transformed = transform_box(input, &mut f)?;
                //             if transformed.transformed {
                //                 transformed_any = true;
                //             }
                //             Ok(transformed.data)
                //         })
                //         .collect();
                //     if transformed_any {
                //         Ok(Transformed::yes(RelType::Set(Box::new(SetRel {
                //             common,
                //             inputs: new_inputs?,
                //         }))))
                //     } else {
                //         Ok(Transformed::no(RelType::Set(Box::new(SetRel {
                //             common,
                //             inputs: new_inputs?,
                //         }))))
                //     }
                // }
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
                // FIXME
                // RelType::ExtensionMulti(p) => {
                //     let ExtensionMultiRel {
                //         common,
                //         inputs,
                //         extension,
                //     } = *p;
                //     let mut transformed_any = false;
                //     let new_inputs: std::result::Result<Vec<_>> = inputs
                //         .into_iter()
                //         .map(|input| {
                //             let transformed = transform_box(input, &mut f)?;
                //             if transformed.transformed {
                //                 transformed_any = true;
                //             }
                //             Ok(transformed.data)
                //         })
                //         .collect();
                //     if transformed_any {
                //         Ok(Transformed::yes(RelType::ExtensionMulti(Box::new(
                //             ExtensionMultiRel {
                //                 common,
                //                 inputs: new_inputs?,
                //                 extension,
                //             },
                //         ))))
                //     } else {
                //         Ok(Transformed::no(RelType::ExtensionMulti(Box::new(
                //             ExtensionMultiRel {
                //                 common,
                //                 inputs: new_inputs?,
                //                 extension,
                //             },
                //         ))))
                //     }
                // }

                // FIXME - add all the others
                _ => Transformed::no(rel_type),
            };
            Ok(t.update_data(|rt| Rel { rel_type: Some(rt) }))
        } else {
            Err(DataFusionError::Plan("RelType is None".into()))
        }
    }
}
