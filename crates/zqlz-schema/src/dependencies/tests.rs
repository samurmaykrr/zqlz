//! Tests for the Schema Dependencies Analyzer

use super::*;
use zqlz_core::ObjectType;

mod object_ref_tests {
    use super::*;

    #[test]
    fn test_object_ref_new() {
        let obj = ObjectRef::new("users", ObjectType::Table);
        assert_eq!(obj.name, "users");
        assert!(obj.schema.is_none());
        assert_eq!(obj.object_type, ObjectType::Table);
    }

    #[test]
    fn test_object_ref_with_schema() {
        let obj = ObjectRef::with_schema("public", "users", ObjectType::Table);
        assert_eq!(obj.schema, Some("public".to_string()));
        assert_eq!(obj.name, "users");
        assert_eq!(obj.object_type, ObjectType::Table);
    }

    #[test]
    fn test_object_ref_qualified_name() {
        let obj_no_schema = ObjectRef::new("users", ObjectType::Table);
        assert_eq!(obj_no_schema.qualified_name(), "users");

        let obj_with_schema = ObjectRef::with_schema("public", "users", ObjectType::Table);
        assert_eq!(obj_with_schema.qualified_name(), "public.users");
    }

    #[test]
    fn test_object_ref_equality() {
        let obj1 = ObjectRef::with_schema("public", "users", ObjectType::Table);
        let obj2 = ObjectRef::with_schema("public", "users", ObjectType::Table);
        let obj3 = ObjectRef::with_schema("other", "users", ObjectType::Table);

        assert_eq!(obj1, obj2);
        assert_ne!(obj1, obj3);
    }
}

mod dependencies_tests {
    use super::*;

    #[test]
    fn test_dependencies_new() {
        let deps = Dependencies::new();
        assert!(deps.depends_on.is_empty());
        assert!(deps.depended_by.is_empty());
    }

    #[test]
    fn test_add_dependency() {
        let mut deps = Dependencies::new();
        let obj = ObjectRef::new("users", ObjectType::Table);
        deps.add_dependency(obj.clone());

        assert!(deps.has_dependencies());
        assert_eq!(deps.depends_on.len(), 1);

        deps.add_dependency(obj.clone());
        assert_eq!(deps.depends_on.len(), 1);
    }

    #[test]
    fn test_add_dependent() {
        let mut deps = Dependencies::new();
        let obj = ObjectRef::new("users_view", ObjectType::View);
        deps.add_dependent(obj.clone());

        assert!(deps.has_dependents());
        assert_eq!(deps.depended_by.len(), 1);

        deps.add_dependent(obj.clone());
        assert_eq!(deps.depended_by.len(), 1);
    }

    #[test]
    fn test_has_dependencies() {
        let mut deps = Dependencies::new();
        assert!(!deps.has_dependencies());

        deps.add_dependency(ObjectRef::new("users", ObjectType::Table));
        assert!(deps.has_dependencies());
    }
}

mod dependency_graph_tests {
    use super::*;

    #[test]
    fn test_dependency_graph_new() {
        let graph = DependencyGraph::new();
        assert!(graph.is_empty());
        assert_eq!(graph.len(), 0);
    }

    #[test]
    fn test_add_object_with_dependencies() {
        let mut graph = DependencyGraph::new();
        let view = ObjectRef::with_schema("public", "users_view", ObjectType::View);
        let table = ObjectRef::with_schema("public", "users", ObjectType::Table);

        graph.add_object(view.clone(), vec![table.clone()]);

        assert_eq!(graph.len(), 2);

        let view_deps = graph.get("public.users_view").unwrap();
        assert!(view_deps.has_dependencies());
        assert!(!view_deps.has_dependents());

        let table_deps = graph.get("public.users").unwrap();
        assert!(!table_deps.has_dependencies());
        assert!(table_deps.has_dependents());
    }

    #[test]
    fn test_find_all_dependents() {
        let mut graph = DependencyGraph::new();

        let table = ObjectRef::new("users", ObjectType::Table);
        let view1 = ObjectRef::new("users_view", ObjectType::View);
        let view2 = ObjectRef::new("users_report", ObjectType::View);

        graph.add_object(view1.clone(), vec![table.clone()]);
        graph.add_object(view2.clone(), vec![view1.clone()]);

        let deps = graph.find_all_dependents("users");
        assert_eq!(deps.len(), 2);
        assert!(deps.contains("users_view"));
        assert!(deps.contains("users_report"));
    }
}

mod analyzer_config_tests {
    use super::*;

    #[test]
    fn test_analyzer_config_default() {
        let config = AnalyzerConfig::default();
        assert_eq!(config.default_schema, Some("public".to_string()));
        assert!(!config.include_system_schemas);
    }

    #[test]
    fn test_analyzer_config_builder() {
        let config = AnalyzerConfig::new()
            .with_default_schema("myschema")
            .with_system_schemas();

        assert_eq!(config.default_schema, Some("myschema".to_string()));
        assert!(config.include_system_schemas);
    }
}

mod dependency_analyzer_tests {
    use super::*;

    #[test]
    fn test_dependency_analyzer_new() {
        let analyzer = DependencyAnalyzer::new();
        let config = analyzer.config();
        assert_eq!(config.default_schema, Some("public".to_string()));
    }

    #[test]
    fn test_extract_from_view_sql_simple() {
        let analyzer = DependencyAnalyzer::new();
        let sql = "SELECT * FROM users";
        let deps = analyzer.extract_from_view_sql(sql);

        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].name, "users");
    }

    #[test]
    fn test_build_graph() {
        let analyzer = DependencyAnalyzer::new();
        let view = ObjectRef::new("users_view", ObjectType::View);
        let sql = "SELECT * FROM users";

        let graph = analyzer.build_graph(&[(view, sql)]);

        assert_eq!(graph.len(), 2);
    }
}

mod extract_table_references_tests {
    use super::*;

    #[test]
    fn test_extract_simple_from() {
        let sql = "SELECT * FROM users";
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
        assert!(refs[0].schema.is_none());
    }

    #[test]
    fn test_extract_qualified_table() {
        let sql = "SELECT * FROM public.users";
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
        assert_eq!(refs[0].schema, Some("public".to_string()));
    }

    #[test]
    fn test_extract_handles_quoted_identifiers() {
        // Test with simple quoted identifiers (no spaces)
        let sql = r#"SELECT * FROM "Users" JOIN "Orders" ON "Users".id = "Orders".user_id"#;
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 2);
        let names: Vec<&str> = refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"Users"));
        assert!(names.contains(&"Orders"));
    }

    #[test]
    fn test_extract_left_join() {
        let sql = "SELECT * FROM users LEFT JOIN profiles ON users.id = profiles.user_id";
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 2);
        let names: Vec<&str> = refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"profiles"));
    }

    #[test]
    fn test_extract_no_duplicates() {
        let sql = "SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.manager_id";
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
    }

    #[test]
    fn test_extract_update_statement() {
        let sql = "UPDATE users SET name = 'test'";
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
    }

    #[test]
    fn test_extract_insert_into() {
        let sql = "INSERT INTO users (name) VALUES ('test')";
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
    }

    #[test]
    fn test_extract_complex_view() {
        let sql = r#"
            SELECT u.id, u.name, o.total
            FROM public.users u
            INNER JOIN sales.orders o ON u.id = o.user_id
            LEFT JOIN inventory.products p ON o.product_id = p.id
            WHERE u.active = true
        "#;
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 3);
        let qualified_names: Vec<String> = refs.iter().map(|r| r.qualified_name()).collect();
        assert!(qualified_names.contains(&"public.users".to_string()));
        assert!(qualified_names.contains(&"sales.orders".to_string()));
        assert!(qualified_names.contains(&"inventory.products".to_string()));
    }

    #[test]
    fn test_extract_ignores_subquery_parens() {
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
        let refs = extract_table_references(sql);

        assert_eq!(refs.len(), 2);
    }
}
