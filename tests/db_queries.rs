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
        .insert_script("gc-test", "bash", "echo hi", Some("gc coverage"), None, true, None, None, false, None)
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
async fn webhook_insert_find_delete_roundtrip() -> anyhow::Result<()> {
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    let (sid, _v) = db
        .insert_script("wh-db", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;
    let token = dokan::crypto::random_token();
    let id = db.insert_webhook(&token, "script", sid, Some("agent-x")).await?;

    let found = db.find_webhook_by_token(&token).await?;
    assert_eq!(
        found,
        Some(("script".to_string(), sid, Some("agent-x".to_string()))),
        "token resolves to its target"
    );
    assert!(db.list_webhooks().await?.iter().any(|w| w["webhook_id"] == id));

    assert!(db.delete_webhook(id).await?, "delete reports removal");
    assert_eq!(db.find_webhook_by_token(&token).await?, None, "gone after delete");
    Ok(())
}

#[tokio::test]
async fn list_scripts_enumerates_catalog() -> anyhow::Result<()> {
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;
    let (a, _) = db.insert_script("cat-a", "bash", "echo a", None, None, true, None, None, false, None).await?;
    let (b, _) = db.insert_script("cat-b", "python", "print(1)", None, None, true, None, None, false, None).await?;

    let (rows, total) = db.list_scripts(500).await?;
    assert!(total >= 2, "catalog counts all scripts");
    let ids: Vec<i64> = rows.iter().map(|s| s.id).collect();
    assert!(ids.contains(&a) && ids.contains(&b), "both scripts listed");
    Ok(())
}

#[tokio::test]
async fn fail_stale_pending_retires_zombies_but_spares_fresh() -> anyhow::Result<()> {
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;
    let (sid, _) = db.insert_script("zombie", "bash", "echo z", None, None, true, None, None, false, None).await?;

    // A fresh pending run is NOT retired by a generous timeout.
    let keep = db.insert_run(sid, &serde_json::json!({}), None).await?;
    db.fail_stale_pending(3600.0).await?;
    assert_eq!(db.run_status(keep).await?.as_deref(), Some("pending"), "fresh pending survives");

    // With a 0s timeout, an already-pending run is retired as unclaimed.
    let zombie = db.insert_run(sid, &serde_json::json!({}), None).await?;
    let n = db.fail_stale_pending(0.0).await?;
    assert!(n >= 1, "retired at least the zombie");
    assert_eq!(db.run_status(zombie).await?.as_deref(), Some("failed"), "zombie failed");
    Ok(())
}

#[tokio::test]
async fn last_result_feeds_next_run() -> anyhow::Result<()> {
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    // A stateful monitor: feed_prev_result = true.
    let (sid, _v) = db
        .insert_script("monitor", "bash", "echo hi", None, None, true, None, None, true, None)
        .await?;

    // Run #1 emits a structured result {"state":"A"}.
    let run1 = db.insert_run(sid, &serde_json::json!({}), None).await?;
    db.set_run_result(run1, &serde_json::json!({ "state": "A" })).await?;

    // Run #2: the next run sees run #1's result as prev_result.
    let run2 = db.insert_run(sid, &serde_json::json!({}), None).await?;
    assert_eq!(
        db.last_result_for_script(sid, run2).await?,
        Some(serde_json::json!({ "state": "A" })),
        "next run sees the previous run's structured result"
    );

    // First run has no prior result.
    assert_eq!(
        db.last_result_for_script(sid, run1).await?,
        None,
        "the first run has no prior result"
    );
    Ok(())
}

#[tokio::test]
async fn gc_old_keeps_fresh_terminal_runs() -> anyhow::Result<()> {
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    let (sid, _v) = db
        .insert_script("gc-keep", "bash", "echo hi", None, None, true, None, None, false, None)
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

#[tokio::test]
async fn blob_roundtrip() -> anyhow::Result<()> {
    // Run artifacts (v0.2.2): content-addressed input store. put_blob is content-addressed
    // (same bytes → same sha, deduped to one row); get_blob round-trips the bytes.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    let (sha1, size1) = db.put_blob(b"hello").await?;
    let (sha2, size2) = db.put_blob(b"hello").await?; // re-upload: dedup, same handle
    assert_eq!(sha1, sha2, "identical bytes → identical content address");
    assert_eq!(size1, 5);
    assert_eq!(size2, 5);

    // Exactly one row for that sha (dedup, not a second insert).
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM blobs WHERE sha = $1")
        .bind(&sha1)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(rows, 1, "dedup: a single stored row for identical bytes");

    assert!(db.blob_exists(&sha1).await?, "blob_exists sees the stored blob");
    assert_eq!(db.get_blob(&sha1).await?.as_deref(), Some(&b"hello"[..]), "bytes round-trip");

    // Different bytes → a different content address.
    let (sha_other, _) = db.put_blob(b"world").await?;
    assert_ne!(sha1, sha_other, "different bytes → different sha");

    assert_eq!(db.get_blob("deadbeef-not-a-real-sha").await?, None, "missing handle → None");
    Ok(())
}

#[tokio::test]
async fn run_with_input_file_validates_and_persists_blobs() -> anyhow::Result<()> {
    // The wiring an executor relies on: a handle validates, then a run created with an
    // input_blobs map persists it — the source the executor reads to materialize /input.
    // Deterministic (no Docker); reads the column directly so it doesn't drain the shared
    // pending queue the way a claim_run would (which could flake parallel tests).
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    let (sha, _) = db.put_blob(b"note body").await?;
    assert!(db.blob_exists(&sha).await?, "the file handle validates before a run is created");
    let (sid, _) = db
        .insert_script("input-file-run", "bash", "cat /input/note.txt", None, None, true, None, None, false, None)
        .await?;
    let input_blobs = serde_json::json!({ "note.txt": sha });
    let run_id = db
        .insert_run_with_blobs(sid, &serde_json::json!({}), None, Some(&input_blobs))
        .await?;

    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT input_blobs FROM runs WHERE id = $1")
            .bind(run_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        stored.as_ref().and_then(|v| v.get("note.txt")).and_then(|v| v.as_str()),
        Some(sha.as_str()),
        "run carries the content-addressed input handle for the executor to materialize"
    );
    Ok(())
}
