//! Tests for TreeNode Compatibility

#[cfg(test)]
mod tests {
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use datafusion::common::Result;
    use datafusion::common::substrait_tree::*;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::plan_rel::RelType;
    use substrait::proto::rel::RelType::Project;
    use substrait::proto::{Plan, ProjectRel, Rel};

    #[test]
    fn tree_visit() -> Result<()> {
        let path = "tests/testdata/contains_plan.substrait.json";
        let proto_plan = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        for r in proto_plan.relations {
            let rel = match r.rel_type.unwrap() {
                RelType::Rel(rel) => rel,
                RelType::Root(root_rel) => root_rel.input.unwrap(),
            };

            rel.apply(|r| {
                println!("REL: {:#?}", r);
                Ok(TreeNodeRecursion::Continue)
            })?;
        }

        Ok(())
    }
    #[test]
    fn tree_map() -> Result<()> {
        let path = "tests/testdata/contains_plan.substrait.json";
        let proto_plan = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        for r in proto_plan.relations {
            let rel = match r.rel_type.unwrap() {
                RelType::Rel(rel) => rel,
                RelType::Root(root_rel) => root_rel.input.unwrap(),
            };

            rel.apply(|r| {
                if let Some(Project(p)) = &r.rel_type {
                    println!("PROJECT REL: {:#?}", p);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;

            // rewrite ProjectRel node - remove common field
            let t = rel
                .transform(|r| {
                    if let Some(Project(p)) = &r.rel_type {
                        let updated = Project(Box::new(ProjectRel {
                            common: None,
                            input: p.input.clone(),
                            expressions: p.expressions.clone(),
                            advanced_extension: p.advanced_extension.clone(),
                        }));
                        Ok(Transformed::yes(Rel {
                            rel_type: Some(updated),
                        }))
                    } else {
                        Ok(Transformed::no(r))
                    }
                })?
                .data;

            println!("AFTER");
            t.apply(|r| {
                if let Some(Project(p)) = &r.rel_type {
                    println!("PROJECT REL: {:#?}", p);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }

        Ok(())
    }
}
