use std::io;
use std::path::{Path, PathBuf};

use rand::distributions::{Alphanumeric, DistString};
use solve::core::Error;

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn join<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.0.join(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

pub fn temp_dir() -> Result<TempDir, Error> {
    let tmpdir = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let path = loop {
        let path = tmpdir.join(Alphanumeric.sample_string(&mut rand::thread_rng(), 32));
        match std::fs::metadata(&path) {
            Ok(_) => continue,
            Err(v) if v.kind() == io::ErrorKind::NotFound => break path,
            Err(v) => return Err(v.into()),
        }
    };
    std::fs::create_dir_all(&path)?;
    Ok(TempDir(path))
}
