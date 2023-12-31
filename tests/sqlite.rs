use solve::db::{
    any,
    query::Select,
    sqlite::{Database, Value},
};

mod common;

#[tokio::test]
async fn test_sqlite() {
    let tmpdir = common::temp_dir().unwrap();
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db = Database::new(&config).unwrap();
    let mut conn = db.connection().await.unwrap();
    conn.execute(
        r#"CREATE TABLE test_tbl (a INTEGER PRIMARY KEY, b TEXT NOT NULL)"#,
        &[],
    )
    .await
    .unwrap();
    conn.execute(
        r#"INSERT INTO test_tbl (b) VALUES ($1), ($2)"#,
        &["test1".to_owned().into(), "test2".to_owned().into()],
    )
    .await
    .unwrap();
    let mut rows = conn
        .query("SELECT a, b FROM test_tbl ORDER BY a", &[])
        .await
        .unwrap();
    assert_eq!(rows.columns(), vec!["a", "b"]);
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(1), Value::Text("test1".to_owned())]
    );
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(2), Value::Text("test2".to_owned())]
    );
    assert!(rows.next().await.is_none());
    drop(rows);
    // Check commit.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#, &[])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(3)]
    );
    drop(rows);
    // Check rollback.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#, &[])
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(3)]
    );
    drop(rows);
    // Check drop.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#, &[])
        .await
        .unwrap();
    drop(tx);
    let mut rows = conn
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(3)]
    );
    drop(rows);
    // Check uncommited.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(
        r#"INSERT INTO test_tbl (b) VALUES ($1)"#,
        &["test3".to_owned().into()],
    )
    .await
    .unwrap();
    let mut rows = tx
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(4)]
    );
}

#[tokio::test]
async fn test_any_sqlite() {
    let tmpdir = common::temp_dir().unwrap();
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db: any::Database = Database::new(&config).unwrap().into();
    db.execute(r#"CREATE TABLE test_tbl (a INTEGER PRIMARY KEY, b TEXT NOT NULL)"#)
        .await
        .unwrap();
    db.execute((
        r#"INSERT INTO test_tbl (b) VALUES ($1), ($2)"#,
        ["test1".to_owned().into(), "test2".to_owned().into()].as_slice(),
    ))
    .await
    .unwrap();
    let mut rows = db
        .query("SELECT a, b FROM test_tbl ORDER BY a")
        .await
        .unwrap();
    assert_eq!(rows.columns(), vec!["a", "b"]);
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get(0).unwrap(), any::Value::Int64(1));
    assert_eq!(row.get(1).unwrap(), any::Value::String("test1".into()));
    assert_eq!(row.get("a").unwrap(), any::Value::Int64(1));
    assert_eq!(row.get("b").unwrap(), any::Value::String("test1".into()));
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get(0).unwrap(), any::Value::Int64(2));
    assert_eq!(row.get(1).unwrap(), any::Value::String("test2".into()));
    assert!(rows.next().await.is_none());
    // Check commit.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get(0).unwrap(), any::Value::Int64(3));
    // Check rollback.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get(0).unwrap(), any::Value::Int64(3));
    // Check drop.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    drop(tx);
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get(0).unwrap(), any::Value::Int64(3));
    // Check uncommited.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#)
        .await
        .unwrap();
    let mut rows = tx.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get(0).unwrap(), any::Value::Int64(4));
}

#[tokio::test]
async fn test_query_builder() {
    let tmpdir = common::temp_dir().unwrap();
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db: any::Database = Database::new(&config).unwrap().into();
    let select = Select::new().table("test_tbl");
}
