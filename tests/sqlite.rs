use std::path::Path;

use solve::db::{
    new_sqlite, AnyDatabase, Connection, Database, Executor, Row, Rows, SQLiteValue, Transaction,
    Value,
};

#[tokio::test]
async fn test_sqlite() {
    let tmpdir = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let _ = tokio::fs::remove_file(tmpdir.join("db.sqlite")).await;
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db = new_sqlite(&config).await.unwrap();
    let mut conn = db.connection(Default::default()).await.unwrap();
    let mut tx = conn.transaction(Default::default()).await.unwrap();
    let mut rows = tx.query("SELECT 1 UNION SELECT 2").await.unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().await {
        let value = row.as_ref().unwrap().values();
        count += 1;
        assert_eq!(value[0], SQLiteValue::Integer(count));
    }
    assert_eq!(count, 2);
    drop(rows);
    drop(tx);
    let mut rows = conn.query("SELECT 1").await.unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().await {
        let value = row.as_ref().unwrap().values();
        count += 1;
        assert_eq!(value[0], SQLiteValue::Integer(count));
    }
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_any_sqlite() {
    let tmpdir = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let _ = tokio::fs::remove_file(tmpdir.join("db.sqlite")).await;
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db = AnyDatabase::new(new_sqlite(&config).await.unwrap());
    db.execute(r#"CREATE TABLE test_tbl (a INTEGER PRIMARY KEY, b TEXT NOT NULL)"#)
        .await
        .unwrap();
    db.execute(r#"INSERT INTO test_tbl (b) VALUES ("test1"), ("test2")"#)
        .await
        .unwrap();
    let mut rows = db
        .query("SELECT a, b FROM test_tbl ORDER BY a")
        .await
        .unwrap();
    assert_eq!(rows.columns(), vec!["a", "b"]);
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Int64(1), Value::String("test1".to_owned())]
    );
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Int64(2), Value::String("test2".to_owned())]
    );
    assert!(rows.next().await.is_none());
    // Check commit.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Int64(3)]
    );
    // Check rollback.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Int64(3)]
    );
    // Check drop.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    drop(tx);
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Int64(3)]
    );
    // Check uncommited.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    let mut rows = tx.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Int64(4)]
    );
}
