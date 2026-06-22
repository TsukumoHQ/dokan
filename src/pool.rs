//! Warm container pool — "pool warm, run clean, discard."
//!
//! Latency cost is image-pull + container init, not namespace creation. A buffer of
//! pre-started idle containers (`sleep infinity`) lets a job start via `docker exec`
//! in <300ms instead of create+start. A container is used once then discarded (never
//! reused dirty); a background filler tops the buffer back up.
//!
//! deadpool's Manager assumes object *reuse*, which is exactly what we must not do
//! here — so the pool is hand-rolled (PRD §9: "implement the Manager yourself").

use anyhow::{anyhow, Result};
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{CreateContainerOptionsBuilder, CreateImageOptionsBuilder, StartContainerOptions};
use bollard::Docker;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

/// Hard guard against a fork bomb / runaway thread count inside a job.
const PIDS_LIMIT: i64 = 512;

pub struct WarmPool {
    docker: Docker,
    /// Warm containers to keep ready per image. Atomic so the autoscaler can retune it live
    /// from Little's Law (L = λW).
    target_idle: AtomicUsize,
    mem_bytes: i64,
    nano_cpus: i64,
    idle: Mutex<HashMap<String, Vec<String>>>, // image -> [container_id]
    known: Mutex<HashSet<String>>,             // images to keep warm
    digests: Mutex<HashMap<String, String>>,   // image -> resolved content digest
}

impl WarmPool {
    pub fn new(docker: Docker, target_idle: usize, mem_bytes: i64, nano_cpus: i64) -> Arc<Self> {
        // NB: the background filler is NOT started here. Only an *executor* process (one
        // with worker caps) arms it via `arm()`. Control-plane-only instances — e.g. a
        // per-agent stdio dokan that just enqueues/reads over the shared Postgres — share
        // the same Docker host, so they must not each spin up their own warm containers.
        Arc::new(Self {
            docker,
            target_idle: AtomicUsize::new(target_idle),
            mem_bytes,
            nano_cpus,
            idle: Mutex::new(HashMap::new()),
            known: Mutex::new(HashSet::new()),
            digests: Mutex::new(HashMap::new()),
        })
    }

    /// The resolved content digest of an image, if known (pinned into the cache key + receipt
    /// so an image update invalidates recalls). Resolved lazily when containers are created.
    pub fn digest(&self, image: &str) -> Option<String> {
        self.digests.lock().unwrap().get(image).cloned()
    }

    /// Eagerly pull + resolve digests for the given images (boot) so the cache key is stable
    /// from the first run — a lazily-resolved digest would change the key between calls.
    pub async fn resolve_all(&self, images: &[&str]) {
        for img in images {
            if self.ensure_image(img).await.is_ok() {
                self.resolve_digest(img).await;
            }
        }
    }

    async fn resolve_digest(&self, image: &str) {
        if self.digests.lock().unwrap().contains_key(image) {
            return;
        }
        if let Ok(info) = self.docker.inspect_image(image).await {
            // Prefer a repo digest; fall back to the local image id.
            let d = info
                .repo_digests
                .as_ref()
                .and_then(|v| v.first().cloned())
                .or(info.id)
                .unwrap_or_default();
            if !d.is_empty() {
                self.digests.lock().unwrap().insert(image.to_string(), d);
            }
        }
    }

    /// Cold-create a one-off, NETWORK-DISABLED container for a deterministic (network=false)
    /// job — never warmed/reused. Caller discards it after the run.
    pub async fn acquire_isolated(&self, image: &str) -> Result<String> {
        self.ensure_image(image).await?;
        self.resolve_digest(image).await;
        let body = ContainerCreateBody {
            image: Some(image.to_string()),
            cmd: Some(vec!["sleep".into(), "infinity".into()]),
            host_config: Some(HostConfig {
                memory: Some(self.mem_bytes),
                nano_cpus: Some(self.nano_cpus),
                pids_limit: Some(PIDS_LIMIT),
                network_mode: Some("none".to_string()),
                ..Default::default()
            }),
            labels: Some(HashMap::from([("dokan.role".to_string(), "warm".to_string())])),
            ..Default::default()
        };
        let opts = CreateContainerOptionsBuilder::default().build();
        let created = self.docker.create_container(Some(opts), body).await?;
        self.docker
            .start_container(&created.id, None::<StartContainerOptions>)
            .await?;
        metrics::counter!("dokan_pool_acquire_total", "result" => "isolated").increment(1);
        Ok(created.id)
    }

    /// Retune the warm depth per image (autoscaler). Returns the value set.
    pub fn set_target_idle(&self, n: usize) -> usize {
        self.target_idle.store(n, Ordering::Relaxed);
        n
    }

    /// Start the background filler. Call once, only on the executor process.
    pub fn arm(self: &Arc<Self>) {
        self.clone().spawn_filler();
    }

    /// Per-job (mem_bytes, nano_cpus) caps applied to every container.
    pub fn limits(&self) -> (i64, i64) {
        (self.mem_bytes, self.nano_cpus)
    }

    /// Mark images as wanted so the filler pulls + warms them now (instead of lazily on the
    /// first acquire). Call once on the executor after arm().
    pub fn prewarm(&self, images: &[&str]) {
        let mut known = self.known.lock().unwrap();
        for img in images {
            known.insert(img.to_string());
        }
    }

    /// Remove warm containers left behind by a previously-crashed dokan (labeled
    /// `dokan.role=warm`). Run at executor startup: in the single-executor model these are
    /// always orphans, so reclaiming them stops the slow Docker-host saturation that caused
    /// the teardown 404s. Returns the count removed.
    pub async fn sweep_orphans(&self) -> usize {
        use bollard::query_parameters::ListContainersOptionsBuilder;
        let mut filters = HashMap::new();
        filters.insert("label".to_string(), vec!["dokan.role=warm".to_string()]);
        let opts = ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();
        let list = match self.docker.list_containers(Some(opts)).await {
            Ok(l) => l,
            Err(_) => return 0,
        };
        let mut n = 0;
        for c in list {
            if let Some(id) = c.id {
                self.discard(&id).await;
                n += 1;
            }
        }
        n
    }

    /// Check out a ready container for `image`. Pops a warm one if available, else
    /// creates on demand. Caller owns the container and must discard it after use.
    pub async fn acquire(&self, image: &str) -> Result<String> {
        self.known.lock().unwrap().insert(image.to_string());
        let t0 = std::time::Instant::now();
        let (popped, remaining) = {
            let mut idle = self.idle.lock().unwrap();
            let v = idle.get_mut(image);
            let remaining = v.as_ref().map(|v| v.len()).unwrap_or(0);
            match v.and_then(|v| v.pop()) {
                Some(id) => (Some(id), remaining.saturating_sub(1)),
                None => (None, 0),
            }
        };
        let result = match popped {
            // Warm hit: a pre-started container was ready (the fast path the pool exists for).
            Some(id) => {
                metrics::counter!("dokan_pool_acquire_total", "result" => "warm").increment(1);
                metrics::gauge!("dokan_pool_idle_containers", "image" => image.to_string())
                    .set(remaining as f64);
                Ok(id)
            }
            // Cold miss: buffer was empty, pay create (+ maybe pull) on the hot path.
            None => {
                metrics::counter!("dokan_pool_acquire_total", "result" => "cold").increment(1);
                self.create_idle(image).await
            }
        };
        metrics::histogram!("dokan_pool_acquire_seconds").record(t0.elapsed().as_secs_f64());
        result
    }

    /// Discard a container (after a run, or a stale idle one). Best-effort.
    pub async fn discard(&self, container_id: &str) {
        use bollard::query_parameters::RemoveContainerOptionsBuilder;
        let _ = self
            .docker
            .remove_container(
                container_id,
                Some(RemoveContainerOptionsBuilder::default().force(true).build()),
            )
            .await;
        metrics::counter!("dokan_pool_containers_discarded_total").increment(1);
    }

    async fn ensure_image(&self, image: &str) -> Result<()> {
        if self.docker.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        metrics::counter!("dokan_pool_image_pulls_total").increment(1);
        let opts = CreateImageOptionsBuilder::default().from_image(image).build();
        let mut stream = self.docker.create_image(Some(opts), None, None);
        while let Some(item) = stream.next().await {
            item.map_err(|e| anyhow!("pull {image}: {e}"))?;
        }
        Ok(())
    }

    async fn create_idle(&self, image: &str) -> Result<String> {
        let t0 = std::time::Instant::now();
        self.ensure_image(image).await?;
        self.resolve_digest(image).await;
        let body = ContainerCreateBody {
            image: Some(image.to_string()),
            // Idle until we exec the job into it; resource caps applied here.
            cmd: Some(vec!["sleep".into(), "infinity".into()]),
            host_config: Some(HostConfig {
                memory: Some(self.mem_bytes),
                nano_cpus: Some(self.nano_cpus),
                pids_limit: Some(PIDS_LIMIT),
                ..Default::default()
            }),
            // Tag so a fresh executor can sweep containers orphaned by a crashed one.
            labels: Some(HashMap::from([("dokan.role".to_string(), "warm".to_string())])),
            ..Default::default()
        };
        let opts = CreateContainerOptionsBuilder::default().build();
        let created = self.docker.create_container(Some(opts), body).await?;
        self.docker
            .start_container(&created.id, None::<StartContainerOptions>)
            .await?;
        metrics::counter!("dokan_pool_containers_created_total").increment(1);
        metrics::histogram!("dokan_pool_create_seconds").record(t0.elapsed().as_secs_f64());
        Ok(created.id)
    }

    fn spawn_filler(self: Arc<Self>) {
        // Per tick, create up to this many containers per image — fast enough to refill
        // after a burst (autoscaler raises target_idle), bounded to avoid a create storm.
        const BURST: usize = 4;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(750)).await;
                let target = self.target_idle.load(Ordering::Relaxed);
                let images: Vec<String> = self.known.lock().unwrap().iter().cloned().collect();
                for image in images {
                    let have = {
                        let idle = self.idle.lock().unwrap();
                        idle.get(&image).map(|v| v.len()).unwrap_or(0)
                    };
                    if have > target {
                        // Scale down: the autoscaler lowered the target (λ fell), so discard
                        // the excess idle containers instead of holding host resources.
                        let excess = (have - target).min(BURST);
                        let mut drop_ids = Vec::new();
                        {
                            let mut idle = self.idle.lock().unwrap();
                            if let Some(v) = idle.get_mut(&image) {
                                for _ in 0..excess {
                                    if let Some(id) = v.pop() {
                                        drop_ids.push(id);
                                    }
                                }
                            }
                        }
                        for id in &drop_ids {
                            self.discard(id).await;
                        }
                        let n = self.idle.lock().unwrap().get(&image).map(|v| v.len()).unwrap_or(0);
                        metrics::gauge!("dokan_pool_idle_containers", "image" => image.clone())
                            .set(n as f64);
                        continue;
                    }
                    let deficit = target.saturating_sub(have).min(BURST);
                    for _ in 0..deficit {
                        if let Ok(id) = self.create_idle(&image).await {
                            let n = {
                                let mut idle = self.idle.lock().unwrap();
                                let v = idle.entry(image.clone()).or_default();
                                v.push(id);
                                v.len()
                            };
                            metrics::gauge!("dokan_pool_idle_containers", "image" => image.clone())
                                .set(n as f64);
                        }
                    }
                }
            }
        });
    }
}
