//! Unit-level coverage of db query methods the wire tests don't exercise — currently the
//! hourly retention GC (`gc_old`), which only ever ran in production. Possible now that
//! dokan is a library crate (src/lib.rs). A column/SQL typo in these queries fails the
//! build here instead of silently at runtime, closing the documented gc_old blind spot.

use dokan::db::Db;

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://dokan:dokan@127.0.0.1:5499/dokan".into())
}

#[tokio::test]
async fn gc_old_deletes_terminal_runs_and_logs() -> anyhow::Result<()> {
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    // A script + a finished run + a log line.
    let (sid, _v) = db
        .insert_script("gc-test", "bash", "echo hi", Some("gc coverage"), None, true, None)
        .await?;
    let run_id = db.insert_run(sid, &serde_json::json!({}), None).await?;
    db.append_log(run_id, 0, "stdout", "a line").await?;
    db.finish_run(run_id, "succeeded", Some(0), None).await?;

    // days=0 → threshold is now(); the just-finished run is already older → collected.
    let (runs, logs) = db.gc_old(0.0).await?;
    assert!(runs >= 1, "gc removed the terminal run (got {runs})");
    assert!(logs >= 1, "gc removed its logs (got {logs})");
    assert_eq!(db.run_status(run_id).await?, None, "run row is gone after GC");
    Ok(())
}

#[tokio::test]
async fn gc_old_keeps_fresh_terminal_runs() -> anyhow::Result<()> {
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    let (sid, _v) = db
        .insert_script("gc-keep", "bash", "echo hi", None, None, true, None)
        .await?;
    let run_id = db.insert_run(sid, &serde_json::json!({}), None).await?;
    db.finish_run(run_id, "succeeded", Some(0), None).await?;

    // A 1-day TTL must NOT collect a run that just finished.
    db.gc_old(1.0).await?;
    assert_eq!(
        db.run_status(run_id).await?.as_deref(),
        Some("succeeded"),
        "a fresh run survives a 1-day TTL"
    );
    Ok(())
}
