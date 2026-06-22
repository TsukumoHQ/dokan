//! Local text embeddings (fastembed → ONNX) for semantic `search_script`. Zero API
//! cost (PRD §9). Model: BGE-small-en-v1.5, 384 dims. The model is heavy to load and
//! its `embed` is blocking, so it lives behind a blocking-pool call and is optional —
//! if init fails (air-gapped / missing weights), search falls back to substring match.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

/// Embedding dimensionality (BGE-small) — must match `scripts.embedding vector(384)`.
#[allow(dead_code)]
pub const DIM: usize = 384;

#[derive(Clone)]
pub struct Embedder {
    // fastembed's `embed` takes &mut self; serialize access behind a Mutex.
    model: Arc<Mutex<TextEmbedding>>,
}

impl Embedder {
    /// Try to load the model, caching weights under `cache_dir` (default `.fastembed_cache`).
    pub fn try_load(cache_dir: &str) -> Result<Self> {
        let opts = InitOptions::new(EmbeddingModel::BGESmallENV15)
            .with_cache_dir(cache_dir.into())
            .with_show_download_progress(false);
        let model = TextEmbedding::try_new(opts)?;
        Ok(Self {
            model: Arc::new(Mutex::new(model)),
        })
    }

    /// Embed a single text on the blocking pool (fastembed is synchronous).
    pub async fn embed(&self, text: String) -> Result<Vec<f32>> {
        let model = self.model.clone();
        let v = tokio::task::spawn_blocking(move || {
            let mut m = model.lock().unwrap();
            m.embed(vec![text], None)
        })
        .await??;
        Ok(v.into_iter().next().unwrap_or_default())
    }
}
