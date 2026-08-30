use lenso_data_export_retention_postgres_plugin::DataGovernanceOperator;
use lenso_postgres_kit::SetupOutcome;
use sqlx::{AssertSqlSafe, Executor};

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
#[allow(clippy::too_many_lines)]
async fn schema_and_owner_boundaries_are_explicit_and_idempotent() {
    let database_url =
        std::env::var("LENSO_POSTGRES_TEST_URL").expect("LENSO_POSTGRES_TEST_URL is required");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("data_governance_test_{}_{suffix}", std::process::id());
    assert_eq!(
        DataGovernanceOperator::setup(&database_url, &schema)
            .await
            .unwrap(),
        SetupOutcome::Created {
            version: 1,
            applied: 1,
        }
    );
    assert_eq!(
        DataGovernanceOperator::setup(&database_url, &schema)
            .await
            .unwrap(),
        SetupOutcome::AlreadyCurrent { version: 1 }
    );

    let cleanup_pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    let exports_table = format!("\"{schema}\".data_exports");
    let actions_table = format!("\"{schema}\".retention_actions");
    let results_table = format!("\"{schema}\".retention_results");

    let insert_export = format!(
        "INSERT INTO {exports_table}(export_id,requester_instance,scope_kind,scope_id,subject,source_count,item_count,total_bytes,items) VALUES($1,$2,'organization','org_acme','usr_1',1,0,0,'[]'::jsonb) ON CONFLICT DO NOTHING"
    );
    sqlx::query(AssertSqlSafe(insert_export.as_str()))
        .bind("exp_owner")
        .bind("privacy-service")
        .execute(&cleanup_pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query(AssertSqlSafe(insert_export.as_str()))
            .bind("exp_owner")
            .bind("other-privacy-service")
            .execute(&cleanup_pool)
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    let scoped_export = format!(
        "SELECT export_id FROM {exports_table} WHERE export_id=$1 AND requester_instance=$2"
    );
    assert!(
        sqlx::query(AssertSqlSafe(scoped_export.as_str()))
            .bind("exp_owner")
            .bind("privacy-service")
            .fetch_optional(&cleanup_pool)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        sqlx::query(AssertSqlSafe(scoped_export.as_str()))
            .bind("exp_owner")
            .bind("other-privacy-service")
            .fetch_optional(&cleanup_pool)
            .await
            .unwrap()
            .is_none()
    );
    let scoped_purge =
        format!("DELETE FROM {exports_table} WHERE export_id=$1 AND requester_instance=$2");
    assert_eq!(
        sqlx::query(AssertSqlSafe(scoped_purge.as_str()))
            .bind("exp_owner")
            .bind("other-privacy-service")
            .execute(&cleanup_pool)
            .await
            .unwrap()
            .rows_affected(),
        0
    );

    let first = sqlx::query(AssertSqlSafe(insert_export.as_str()))
        .bind("exp_concurrent")
        .bind("privacy-service")
        .execute(&cleanup_pool);
    let second = sqlx::query(AssertSqlSafe(insert_export.as_str()))
        .bind("exp_concurrent")
        .bind("other-privacy-service")
        .execute(&cleanup_pool);
    let (first, second) = tokio::join!(first, second);
    assert_eq!(
        first.unwrap().rows_affected() + second.unwrap().rows_affected(),
        1
    );

    let insert_action = format!(
        "INSERT INTO {actions_table}(action_id,requester_instance,scope_kind,scope_id,subject,mode,reason,participant_instances) VALUES($1,$2,'organization','org_acme','usr_1','delete','account closure',ARRAY['profile-store']) ON CONFLICT DO NOTHING"
    );
    sqlx::query(AssertSqlSafe(insert_action.as_str()))
        .bind("ret_owner")
        .bind("privacy-admin")
        .execute(&cleanup_pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query(AssertSqlSafe(insert_action.as_str()))
            .bind("ret_owner")
            .bind("other-privacy-admin")
            .execute(&cleanup_pool)
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    let scoped_action = format!(
        "SELECT action_id FROM {actions_table} WHERE action_id=$1 AND requester_instance=$2"
    );
    assert!(
        sqlx::query(AssertSqlSafe(scoped_action.as_str()))
            .bind("ret_owner")
            .bind("other-privacy-admin")
            .fetch_optional(&cleanup_pool)
            .await
            .unwrap()
            .is_none()
    );

    let store_result = format!(
        "INSERT INTO {results_table}(action_id,provider_instance,status,receipt) VALUES($1,$2,$3,$4) ON CONFLICT(action_id,provider_instance) DO UPDATE SET status=CASE WHEN retention_results.status='completed' THEN retention_results.status ELSE EXCLUDED.status END,receipt=CASE WHEN retention_results.status='completed' THEN retention_results.receipt ELSE EXCLUDED.receipt END,attempted_at=transaction_timestamp()"
    );
    sqlx::query(AssertSqlSafe(store_result.as_str()))
        .bind("ret_owner")
        .bind("profile-store")
        .bind("completed")
        .bind(Some("receipt-1"))
        .execute(&cleanup_pool)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(store_result.as_str()))
        .bind("ret_owner")
        .bind("profile-store")
        .bind("rejected")
        .bind(Option::<&str>::None)
        .execute(&cleanup_pool)
        .await
        .unwrap();
    let loaded_result = format!(
        "SELECT status,receipt FROM {results_table} WHERE action_id=$1 AND provider_instance=$2"
    );
    assert_eq!(
        sqlx::query_as::<_, (String, Option<String>)>(AssertSqlSafe(loaded_result.as_str()))
            .bind("ret_owner")
            .bind("profile-store")
            .fetch_one(&cleanup_pool)
            .await
            .unwrap(),
        ("completed".to_owned(), Some("receipt-1".to_owned()))
    );

    cleanup_pool
        .execute(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .await
        .unwrap();
    cleanup_pool.close().await;
}
