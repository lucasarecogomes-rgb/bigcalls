use anyhow::Result;
use serde_json::Value;
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom},
};
use tracing::warn;
pub(crate) struct Tail {
    pub(crate) path: PathBuf,
    offset: u64,
}

impl Tail {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path, offset: 0 }
    }

    pub(crate) fn from_offset(path: PathBuf, offset: u64) -> Self {
        Self { path, offset }
    }

    pub(crate) async fn read_new(&mut self) -> Result<Vec<(u64, Value)>> {
        let file = match File::open(&self.path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(
            file.metadata().await?.len() >= self.offset,
            "correlation history was truncated"
        );
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset)).await?;
        let mut records = Vec::new();
        loop {
            let mut line = Vec::new();
            let size = reader.read_until(b'\n', &mut line).await?;
            if size == 0 || line.last() != Some(&b'\n') {
                break;
            }
            let offset = self.offset;
            self.offset += size as u64;
            match serde_json::from_slice(&line) {
                Ok(value) => records.push((offset, value)),
                Err(_) => warn!(offset, "invalid correlation history line skipped"),
            }
        }
        Ok(records)
    }
}
