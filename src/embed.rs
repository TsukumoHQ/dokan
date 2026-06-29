//! Local text embeddings (fastembed → ONNX) for semantic `search_script`. Zero API cost (PRD §9).
//! Model: BGE-small-en-v1.5, 384 dims. Behind the optional `embed` cargo feature — release binaries
//! ship WITHOUT it (ort/onnxruntime is heavy + has no x86_64-apple-darwin prebuilt). When off, the
//! `Embedder` type still exists but can never be constructed, so `search_script` always uses the
//! substring/pg_trgm fallback. TSU-224.

/// Embedding dimensionality (BGE-small) — must match `scripts.embedding vector(384)`.
#[allow(dead_code)]
pub const DIM: usize = 384;

#[cfg(feature = "embed")]
mod imp {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

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
}

#[cfg(not(feature = "embed"))]
mod imp {
    //! Stub used when built without the `embed` feature: the type exists so the `Option<Embedder>`
    //! plumbing compiles, but it holds an uninhabited value so it can never be constructed — every
    //! `search_script` therefore takes the substring/pg_trgm branch.
    #[derive(Clone)]
    pub struct Embedder(std::convert::Infallible);

    impl Embedder {
        pub async fn embed(&self, _text: String) -> anyhow::Result<Vec<f32>> {
            match self.0 {}
        }
    }
}

pub use imp::Embedder;
