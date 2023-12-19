use std::path::Path;

use solve::db::{Connection, Database, Executor, Row, Rows, SQLiteValue, Transaction};

#[tokio::test]
async fn test_sqlite() {
    let tmpdir = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db = solve::db::new_sqlite(&config).await.unwrap();
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
    tx.rollback().await.unwrap();
    let mut rows = conn.query("SELECT 1").await.unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().await {
        let value = row.as_ref().unwrap().values();
        count += 1;
        assert_eq!(value[0], SQLiteValue::Integer(count));
    }
    assert_eq!(count, 1);
}
