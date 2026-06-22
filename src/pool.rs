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
use std::sync::Mutex;
use std::sync::Arc;
use std::time::Duration;

const MEM_LIMIT_BYTES: i64 = 512 * 1024 * 1024; // 512 MiB
const NANO_CPUS: i64 = 1_000_000_000; // 1.0 CPU

pub struct WarmPool {
    docker: Docker,
    target_idle: usize,
    idle: Mutex<HashMap<String, Vec<String>>>, // image -> [container_id]
    known: Mutex<HashSet<String>>,             // images to keep warm
}

impl WarmPool {
    pub fn new(docker: Docker, target_idle: usize) -> Arc<Self> {
        let pool = Arc::new(Self {
            docker,
            target_idle,
            idle: Mutex::new(HashMap::new()),
            known: Mutex::new(HashSet::new()),
        });
        pool.clone().spawn_filler();
        pool
    }

    /// Check out a ready container for `image`. Pops a warm one if available, else
    /// creates on demand. Caller owns the container and must discard it after use.
    pub async fn acquire(&self, image: &str) -> Result<String> {
        self.known.lock().unwrap().insert(image.to_string());
        let popped = {
            let mut idle = self.idle.lock().unwrap();
            idle.get_mut(image).and_then(|v| v.pop())
        };
        match popped {
            Some(id) => Ok(id),
            None => self.create_idle(image).await,
        }
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
    }

    async fn ensure_image(&self, image: &str) -> Result<()> {
        if self.docker.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        let opts = CreateImageOptionsBuilder::default().from_image(image).build();
        let mut stream = self.docker.create_image(Some(opts), None, None);
        while let Some(item) = stream.next().await {
            item.map_err(|e| anyhow!("pull {image}: {e}"))?;
        }
        Ok(())
    }

    async fn create_idle(&self, image: &str) -> Result<String> {
        self.ensure_image(image).await?;
        let body = ContainerCreateBody {
            image: Some(image.to_string()),
            // Idle until we exec the job into it; resource caps applied here.
            cmd: Some(vec!["sleep".into(), "infinity".into()]),
            host_config: Some(HostConfig {
                memory: Some(MEM_LIMIT_BYTES),
                nano_cpus: Some(NANO_CPUS),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = CreateContainerOptionsBuilder::default().build();
        let created = self.docker.create_container(Some(opts), body).await?;
        self.docker
            .start_container(&created.id, None::<StartContainerOptions>)
            .await?;
        Ok(created.id)
    }

    fn spawn_filler(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let images: Vec<String> = self.known.lock().unwrap().iter().cloned().collect();
                for image in images {
                    let have = {
                        let idle = self.idle.lock().unwrap();
                        idle.get(&image).map(|v| v.len()).unwrap_or(0)
                    };
                    // Top up one per tick per image — gentle, avoids create storms.
                    if have < self.target_idle {
                        if let Ok(id) = self.create_idle(&image).await {
                            self.idle.lock().unwrap().entry(image).or_default().push(id);
                        }
                    }
                }
            }
        });
    }
}
