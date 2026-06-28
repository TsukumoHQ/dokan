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
        .insert_run_with_blobs(sid, &serde_json::json!({}), None, Some(&input_blobs), false)
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

#[tokio::test]
async fn insert_run_idempotent_collapses_to_one_run() -> anyhow::Result<()> {
    // The atomic insert-or-return contract: the first call creates (created=true); a second
    // call with the SAME key recalls the first run (created=false) — exactly-once, no duplicate
    // row. This is the unit-level proof behind the webhook/run dedup wire tests.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    let (sid, _) = db
        .insert_script("idem-run", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;
    let key = format!("idem-test-{}", dokan::crypto::random_token());

    let (first_id, created1) = db
        .insert_run_idempotent(sid, &serde_json::json!({"n": 1}), Some("agent-x"), None, false, &key)
        .await?;
    assert!(created1, "first call creates the run");

    let (second_id, created2) = db
        .insert_run_idempotent(sid, &serde_json::json!({"n": 1}), Some("agent-x"), None, false, &key)
        .await?;
    assert!(!created2, "second call with the same key recalls, does not create");
    assert_eq!(first_id, second_id, "same run_id returned on the recall");

    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM runs WHERE idempotency_key = $1")
        .bind(&key)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(n, 1, "exactly one row despite two identical idempotent inserts");
    Ok(())
}

#[tokio::test]
async fn insert_flow_run_idempotent_collapses_and_builds_steps_once() -> anyhow::Result<()> {
    // Same exactly-once contract for flow_runs, plus: the flow_steps ledger is built ONLY for
    // the freshly inserted flow_run, never duplicated on the recall.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    // A real script id — flow_steps.script_id has an FK to scripts(id).
    let (script_id, _v) = db
        .insert_script("idem-flow-step", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;
    let spec = serde_json::json!({"steps": [{"id": "a", "script_id": script_id, "input": {}}]});
    let flow_id: i64 = sqlx::query_scalar("INSERT INTO flows (name, spec) VALUES ($1, $2) RETURNING id")
        .bind(format!("idem-flow-{}", dokan::crypto::random_token()))
        .bind(&spec)
        .fetch_one(&db.pool)
        .await?;
    let key = format!("idem-flow-key-{}", dokan::crypto::random_token());

    let (first_id, created1) = db
        .insert_flow_run_idempotent(flow_id, &spec, &serde_json::json!({}), &key)
        .await?;
    assert!(created1, "first call creates the flow_run");

    let (second_id, created2) = db
        .insert_flow_run_idempotent(flow_id, &spec, &serde_json::json!({}), &key)
        .await?;
    assert!(!created2, "second call with the same key recalls, does not create");
    assert_eq!(first_id, second_id, "same flow_run_id on the recall");

    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM flow_runs WHERE idempotency_key = $1")
        .bind(&key)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(runs, 1, "exactly one flow_run despite two identical inserts");
    let steps: i64 = sqlx::query_scalar("SELECT count(*) FROM flow_steps WHERE flow_run_id = $1")
        .bind(first_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(steps, 1, "steps built exactly once (only on the fresh insert)");
    Ok(())
}

#[tokio::test]
async fn gc_blobs_reclaims_only_orphaned_and_aged() -> anyhow::Result<()> {
    // Blob retention (TSU-140): gc_blobs deletes a blob ONLY when it is (a) past the TTL AND
    // (b) referenced by no run. A fresh upload (TTL guard) and a referenced blob both survive.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;
    let tag = dokan::crypto::random_token();

    // Distinct bytes per case so content addresses don't collide with other tests.
    let (orphan_old, _) = db.put_blob(format!("orphan-old-{tag}").as_bytes()).await?;
    let (orphan_fresh, _) = db.put_blob(format!("orphan-fresh-{tag}").as_bytes()).await?;
    let (referenced, _) = db.put_blob(format!("referenced-{tag}").as_bytes()).await?;

    // Reference one blob from a run's input_blobs map.
    let (sid, _) = db
        .insert_script("gc-blob-ref", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;
    let input_blobs = serde_json::json!({ "note.txt": referenced });
    db.insert_run_with_blobs(sid, &serde_json::json!({}), None, Some(&input_blobs), false).await?;

    // Age the orphan-old and the referenced blob past the TTL; leave orphan_fresh at now().
    for sha in [&orphan_old, &referenced] {
        sqlx::query("UPDATE blobs SET last_used_at = now() - interval '10 days' WHERE sha = $1")
            .bind(sha)
            .execute(&db.pool)
            .await?;
    }

    let deleted = db.gc_blobs(1.0).await?;
    assert!(deleted >= 1, "at least the aged orphan was reclaimed (got {deleted})");
    assert!(!db.blob_exists(&orphan_old).await?, "aged + unreferenced blob is reclaimed");
    assert!(db.blob_exists(&orphan_fresh).await?, "a fresh upload survives the TTL");
    assert!(db.blob_exists(&referenced).await?, "a referenced blob is never reclaimed");
    Ok(())
}

#[tokio::test]
async fn idempotency_partial_unique_indexes_enforce_dedup_at_db_level() -> anyhow::Result<()> {
    // TSU-162: dedup must hold at the DB level, not just via the app's ON CONFLICT clause.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;

    // Both partial-unique indexes are deployed.
    for idx in ["uq_runs_idempotency", "uq_flow_runs_idempotency"] {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE indexname = $1")
            .bind(idx)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(n, 1, "{idx} exists (DB-level dedup guard)");
    }

    // Behavioral proof on runs: a RAW duplicate insert (bypassing the app's ON CONFLICT) is
    // rejected by uq_runs_idempotency — so even a buggy/foreign writer can't create a dup.
    let (sid, _) = db
        .insert_script("idem-idx", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;
    let key = format!("idem-idx-{}", dokan::crypto::random_token());
    let (_id, created) = db
        .insert_run_idempotent(sid, &serde_json::json!({}), Some("a"), None, false, &key)
        .await?;
    assert!(created, "first insert creates the run");
    let raw = sqlx::query(
        "INSERT INTO runs (script_id, input, status, idempotency_key) \
         VALUES ($1, '{}'::jsonb, 'pending', $2)",
    )
    .bind(sid)
    .bind(&key)
    .execute(&db.pool)
    .await;
    assert!(raw.is_err(), "a raw duplicate idempotency_key is rejected by the partial unique index");
    Ok(())
}

#[tokio::test]
async fn flow_step_pagination_and_map_aggregation() -> anyhow::Result<()> {
    // TSU-177: SQL-level pagination of top-level steps + GROUP-BY map child aggregation.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;
    let (sid, _) = db
        .insert_script("flow-pg", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;
    let spec = serde_json::json!({"steps": [
        {"id":"a","script_id": sid, "input": {}},
        {"id":"b","script_id": sid, "input": {}},
        {"id":"m","script_id": sid, "input": {}}
    ]});
    let flow_id: i64 = sqlx::query_scalar("INSERT INTO flows (name, spec) VALUES ($1, $2) RETURNING id")
        .bind(format!("flow-pg-{}", dokan::crypto::random_token()))
        .bind(&spec)
        .fetch_one(&db.pool)
        .await?;
    let key = format!("flow-pg-{}", dokan::crypto::random_token());
    let (frid, _) = db.insert_flow_run_idempotent(flow_id, &spec, &serde_json::json!({}), &key).await?;
    // Raw-insert map children for parent "m": 2 ok + 2 failed (indices 1,2 fail).
    for (i, st) in [("0", "succeeded"), ("1", "failed"), ("2", "failed"), ("3", "succeeded")] {
        sqlx::query("INSERT INTO flow_steps (flow_run_id, step_id, script_id, status) VALUES ($1, $2, $3, $4)")
            .bind(frid)
            .bind(format!("m#{i}"))
            .bind(sid)
            .bind(st)
            .execute(&db.pool)
            .await?;
    }
    // Top-level count excludes children (a, b, m = 3).
    assert_eq!(db.flow_top_step_count(frid).await?, 3, "3 top-level steps, children excluded");
    // LIMIT/OFFSET windows the top-level page.
    assert_eq!(db.flow_top_steps(frid, 2, 0).await?.len(), 2, "first page of 2");
    assert_eq!(db.flow_top_steps(frid, 2, 2).await?.len(), 1, "second page has the remainder");
    assert!(
        db.flow_top_steps(frid, 10, 0).await?.iter().all(|s| !s.step_id.contains('#')),
        "no map children leak into the top-level page"
    );
    // GROUP-BY map counts for parent m: n=4, ok=2, failed=2.
    let (mut n, mut ok, mut failed) = (0i64, 0i64, 0i64);
    for (p, st, c) in db.flow_map_counts(frid).await? {
        if p == "m" {
            n += c;
            if st == "succeeded" { ok += c }
            if st == "failed" { failed += c }
        }
    }
    assert_eq!((n, ok, failed), (4, 2, 2), "map child counts via GROUP BY");
    // Failed child indices for m (capped) = [1, 2].
    let fi: Vec<i64> = db
        .flow_failed_child_idx(frid, 10)
        .await?
        .into_iter()
        .filter(|(p, _)| p == "m")
        .map(|(_, i)| i)
        .collect();
    assert_eq!(fi, vec![1, 2], "failed child indices, ordered");
    // Cap is honored.
    let capped = db.flow_failed_child_idx(frid, 1).await?.into_iter().filter(|(p, _)| p == "m").count();
    assert_eq!(capped, 1, "per-parent failed-index cap honored");
    Ok(())
}

#[tokio::test]
async fn gc_logs_reclaims_aged_logs_keeps_runs() -> anyhow::Result<()> {
    // TSU-177: the logs-only sweep deletes aged logs but leaves the run rows.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;
    let (sid, _) = db
        .insert_script("gc-logs", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;
    let run_id = db.insert_run(sid, &serde_json::json!({}), None).await?;
    db.finish_run(run_id, "succeeded", Some(0), None).await?;
    db.append_log(run_id, 1, "stdout", "hello").await?;
    sqlx::query("UPDATE runs SET finished_at = now() - interval '10 days' WHERE id = $1")
        .bind(run_id)
        .execute(&db.pool)
        .await?;
    let deleted = db.gc_logs(1.0).await?;
    assert!(deleted >= 1, "aged log reclaimed (got {deleted})");
    let logs: i64 = sqlx::query_scalar("SELECT count(*) FROM logs WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(logs, 0, "logs gone");
    assert_eq!(db.run_status(run_id).await?.as_deref(), Some("succeeded"), "run row kept");
    Ok(())
}

#[tokio::test]
async fn cancel_is_authoritative_over_a_racing_finish() -> anyhow::Result<()> {
    // TSU-190: a cancel must win over the killed container's racing failed-finish, and must
    // not cancel a genuinely-succeeded run.
    let db = Db::connect(&db_url()).await?;
    db.migrate().await?;
    let (sid, _) = db
        .insert_script("cancel-race", "bash", "echo hi", None, None, true, None, None, false, None)
        .await?;

    // Run A: cancel, then the killed exec tries to finish it failed → must STAY canceled.
    let a = db.insert_run(sid, &serde_json::json!({}), None).await?;
    assert!(db.cancel_run(a, "canceled by operator").await?, "pending run cancels");
    db.finish_run(a, "failed", None, Some("container killed")).await?; // the racing teardown
    assert_eq!(db.run_status(a).await?.as_deref(), Some("canceled"), "cancel not clobbered by failed");

    // Run B: cancel wins even if the failed-finish landed FIRST.
    let b = db.insert_run(sid, &serde_json::json!({}), None).await?;
    db.finish_run(b, "failed", None, Some("container killed")).await?;
    assert!(db.cancel_run(b, "canceled by operator").await?, "cancel overrides a failed run");
    assert_eq!(db.run_status(b).await?.as_deref(), Some("canceled"));

    // Run C: a genuinely-succeeded run is NOT cancelable.
    let c = db.insert_run(sid, &serde_json::json!({}), None).await?;
    db.finish_run(c, "succeeded", Some(0), None).await?;
    assert!(!db.cancel_run(c, "too late").await?, "succeeded run is not cancelable");
    assert_eq!(db.run_status(c).await?.as_deref(), Some("succeeded"), "succeeded stays succeeded");
    Ok(())
}
