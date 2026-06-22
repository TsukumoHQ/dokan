# dokan observability (Prometheus + Grafana)

dokan exposes Prometheus metrics at `GET /metrics` (PRD §8: deep analytics lives in
Grafana, not the operator UI). This stack scrapes that endpoint and ships a provisioned
Grafana dashboard so you get the full operational picture with one command.

```
observability/
├── docker-compose.yml                      # prometheus + grafana
├── prometheus/prometheus.yml               # scrape config (targets dokan on the host)
└── grafana/
    ├── provisioning/datasources/…          # Prometheus datasource (uid: dokan-prom)
    ├── provisioning/dashboards/…           # file provider
    └── dashboards/dokan.json               # the "dokan — runtime" dashboard
```

## Run

dokan must be running with the HTTP transport (the supervised `--transport http
--addr 127.0.0.1:8088` is fine). Then:

```sh
docker compose -f observability/docker-compose.yml up -d
```

- **Grafana** → http://localhost:3300 (anonymous admin, dark). The home dashboard is
  `dokan — runtime`, in the `dokan` folder.
- **Prometheus** → http://localhost:9490 (check `Status → Targets`: `dokan` should be UP).

Prometheus reaches host-side dokan via `host.docker.internal` (wired with
`extra_hosts: host-gateway`, works on Docker Desktop and Colima).

## If dokan runs with a bearer token

`/metrics` is behind the same optional gate as the operator UI. When you start dokan with
`--token` / `DOKAN_TOKEN`, uncomment the `authorization` block in
`prometheus/prometheus.yml` and set the token, then `docker compose … restart prometheus`.

## The dashboard

One screen, four rows:

- **Overview** — active runs, queue depth, success rate (1h), warm-pool hit rate (1h),
  enabled schedules.
- **Throughput & outcomes** — runs finished/s by status (stacked, status-colored), live
  queue depth by status, claims vs retries vs internal errors, run duration p50/p95/p99
  (from the histogram).
- **Warm pool** — warm vs cold acquisitions/s, idle containers per image, container
  acquire & cold-create p95.
- **Logs & reliability** — log lines/s by stream, and orphans reaped/s + timeouts (a
  sustained nonzero reap rate means workers are crashing — see the multi-worker reaper).

Status colors match the runtime: pending grey, running amber, succeeded green, failed red,
canceled slate. All latency panels read the same `_seconds` histogram buckets dokan
configures in `main.rs`.

Edits in the Grafana UI persist to its volume; to change the shipped default, edit
`grafana/dashboards/dokan.json` (it re-provisions within ~30s).
