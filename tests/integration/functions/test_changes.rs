use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;
use turso_core::StepResult;

fn single_int(rows: &[Vec<Value>]) -> i64 {
    assert_eq!(rows.len(), 1, "expected a single row");
    assert_eq!(rows[0].len(), 1, "expected a single column");
    match rows[0][0] {
        Value::Integer(v) => v,
        ref other => panic!("expected integer value, got {other:?}"),
    }
}

#[test]
fn changes_counts_updates_and_total_changes() {
    let _ = env_logger::try_init();

    let tmp_db = TempDatabase::new_empty(false);
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(
        &tmp_db,
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);",
    );
    limbo_exec_rows(
        &tmp_db,
        &conn,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    );
    limbo_exec_rows(&tmp_db, &conn, "UPDATE t SET value = 'x' WHERE id <= 2;");

    let changes = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT changes();"));
    assert_eq!(changes, 2);

    let total = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT total_changes();"));
    assert_eq!(total, 5, "3 inserts + 2 updates should be recorded");

    // Updates that touch no rows should not affect either counter.
    limbo_exec_rows(
        &tmp_db,
        &conn,
        "UPDATE t SET value = 'unused' WHERE id = 99;",
    );
    let changes = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT changes();"));
    assert_eq!(changes, 0);
    let total_after = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT total_changes();"));
    assert_eq!(total_after, total);
}

#[test]
fn changes_counts_rowid_updates_once() {
    let _ = env_logger::try_init();

    let tmp_db = TempDatabase::new_empty(false);
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(
        &tmp_db,
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);",
    );
    limbo_exec_rows(&tmp_db, &conn, "INSERT INTO t VALUES (1, 'a');");

    limbo_exec_rows(&tmp_db, &conn, "UPDATE t SET id = id + 10 WHERE id = 1;");

    let changes = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT changes();"));
    assert_eq!(changes, 1, "rowid updates should count as a single change");
    let total = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT total_changes();"));
    assert_eq!(total, 2, "one insert plus one rowid update");

    limbo_exec_rows(&tmp_db, &conn, "UPDATE t SET id = id + 10 WHERE id = 99;");
    let total_after = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT total_changes();"));
    assert_eq!(total_after, total);
}

#[test]
fn statement_n_change_matches_changes_function() {
    let _ = env_logger::try_init();

    let tmp_db = TempDatabase::new_empty(false);
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(
        &tmp_db,
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);",
    );
    limbo_exec_rows(
        &tmp_db,
        &conn,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    );

    let mut stmt = conn
        .prepare("UPDATE t SET value = 'updated' WHERE id BETWEEN 1 AND 3;")
        .unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Done => break,
            StepResult::IO => stmt.run_once().unwrap(),
            StepResult::Row => unreachable!("UPDATE should not return rows"),
            StepResult::Busy | StepResult::Interrupt => panic!("unexpected step result"),
        }
    }

    let stmt_changes = stmt.n_change();
    assert_eq!(stmt_changes, 3);

    let function_changes = single_int(&limbo_exec_rows(&tmp_db, &conn, "SELECT changes();"));
    assert_eq!(stmt_changes, function_changes);
}
