//! Manages the data store for the application.

use std::path::PathBuf;

pub struct DataStore {
    pub root: PathBuf,
}

impl DataStore {
    /// Create a new instance of the data store
    pub fn new() -> Self {
        let mut root = dirs_next::data_dir().unwrap();
        root = root.join("CEDA");
        Self { root }
    }

    /// Path to where the capability data is stored
    pub fn capability_dir(&self) -> PathBuf {
        let dir_path = self.root.join("raw/capability");
        if !dir_path.exists() {
            std::fs::create_dir_all(&dir_path).unwrap();
        }

        dir_path
    }

    /// Path to where the data files are stored
    pub fn rawdata_dir(&self) -> PathBuf {
        let dir_path = self.root.join("raw/data");
        if !dir_path.exists() {
            std::fs::create_dir_all(&dir_path).unwrap();
        }

        dir_path
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let _store = DataStore::new();
        // assert!(store.root.exists());
    }
}