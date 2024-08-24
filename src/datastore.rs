//! Manages the data store for the application.

use std::path::PathBuf;

/// Represents a datastore in the file system to assist in managing data files
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

    /// Get a list of the data file properties
    pub fn get_file_properties(&self) -> Vec<FileProperties> {
        let mut datafiles = Vec::new();
        let dir_path = self.rawdata_dir();

        for file_path in std::fs::read_dir(dir_path).unwrap() {
            let file_path = file_path.unwrap();
            datafiles.push(FileProperties::new(file_path.path()));
        }

        datafiles
    }
}

/// Represents the properties of a data file, obtqined from the filename
#[derive(Debug)]
pub struct FileProperties {
    pub path: PathBuf,
    pub description: String,
    pub updated: String,
    pub region_name: String,
    pub region_id: u32,
    pub station_name: String,
    pub qcv: String,
    pub year: u32,
}

impl FileProperties {
    /// Create a new instance of the data file
    pub fn new(path: PathBuf) -> Self {
        let filename = path.file_name().unwrap().to_str().unwrap();
        let parts: Vec<&str> = filename.split('_').collect();
        let description = parts[1].to_string();
        let updated = parts[2].to_string();
        let region_name = parts[3].to_string();
        let region_id: u32 = parts[4].parse().unwrap();
        let station_name = parts[5].to_string();
        let qcv = parts[6].to_string();
        let year: u32 = parts[7].split('.').next().unwrap().parse().unwrap();

        Self { path, description, updated, region_name, region_id, station_name, qcv, year }
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

    #[test]
    fn test_new_datafile() {
        let file_path = "/Users/richardlyon/Library/Application Support/CEDA/raw/data/midas-open_uk-hourly-weather-obs_dv-202407_aberdeenshire_00144_corgarff-castle-lodge_qcv-1_1997.csv";
        let data_file = FileProperties::new(PathBuf::from(file_path));

        assert_eq!(data_file.path.to_string_lossy(), file_path);
        assert_eq!(data_file.description, "uk-hourly-weather-obs");
        assert_eq!(data_file.updated, "dv-202407");
        assert_eq!(data_file.region_name, "aberdeenshire");
        assert_eq!(data_file.region_id, 144);
        assert_eq!(data_file.station_name, "corgarff-castle-lodge");
        assert_eq!(data_file.qcv, "qcv-1");
        assert_eq!(data_file.year, 1997);
    }
}