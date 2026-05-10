use std::sync::Arc;
use zqlz_connection::{ConnectionManager, SavedConnection};
use zqlz_services::{ConnectionService, KeyValueService, SchemaService};

fn connection_service() -> ConnectionService {
    ConnectionService::new(
        Arc::new(ConnectionManager::new()),
        Arc::new(SchemaService::new()),
        Arc::new(KeyValueService::new(100)),
    )
}

#[tokio::test]
async fn sqlite_connection_feature_set_is_derived_from_active_connection() {
    let service = connection_service();
    let saved_connection = SavedConnection::new("SQLite Memory".to_string(), "sqlite".to_string())
        .with_param("path", ":memory:");

    let info = service
        .connect_and_initialize(&saved_connection)
        .await
        .expect("connect sqlite memory database");

    let features = service
        .connection_feature_set(info.id, None)
        .await
        .expect("derive connection feature set");

    assert_eq!(features.driver_name, "sqlite");
    assert!(features.schema_introspection.available);
    assert!(features.query.execute.available);
    assert!(features.query.explain.available);
    assert!(features.query.cancel.available);
    assert!(features.data_editing.browse_rows.available);
    assert!(features.objects.browse_objects.available);
    assert!(features.stores.key_value.reason.is_some());
    assert!(features.stores.document.reason.is_some());
}

#[tokio::test]
async fn sqlite_connection_feature_set_accepts_explicit_database_scope() {
    let service = connection_service();
    let saved_connection = SavedConnection::new("SQLite Scoped".to_string(), "sqlite".to_string())
        .with_param("path", ":memory:");

    let info = service
        .connect_and_initialize(&saved_connection)
        .await
        .expect("connect sqlite memory database");

    let features = service
        .connection_feature_set(info.id, Some("main".to_string()))
        .await
        .expect("derive scoped connection feature set");

    assert_eq!(features.driver_name, "sqlite");
    assert!(features.query.execute.available);
    assert!(features.objects.browse_objects.available);
}
