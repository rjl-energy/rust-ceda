//! Manages the data store for the application.

use std::env;
use std::path::PathBuf;

/// Represents a datastore in the file system to assist in managing data files
pub struct DataStore {
    pub root: PathBuf,
}

impl DataStore {
    /// Create a new instance of the data store
    pub fn new() -> Self {
        let root = DataStore::get_data_dir();
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

    /// Path to where the database is stored
    pub fn db_dir(&self) -> PathBuf {
        let dir_path = self.root.join("db");
        if !dir_path.exists() {
            std::fs::create_dir_all(&dir_path).unwrap();
        }

        dir_path
    }

    /// Get a list of the data file properties
    pub fn list_data_files(&self) -> Vec<FileProperties> {
        let mut datafiles = Vec::new();
        let dir_path = self.rawdata_dir();

        for file_path in std::fs::read_dir(dir_path).unwrap() {
            let file_path = file_path.unwrap();
            datafiles.push(FileProperties::new(file_path.path()));
        }

        datafiles
    }

    pub fn get_data_dir() -> PathBuf {
        dotenv::dotenv().ok();
        env::var("DATA_DIR").expect("DATA_DIR must be set").into()
    }
}

/// Represents the properties of a data file, obtqined from the filename
#[derive(Debug)]
pub struct FileProperties {
    pub path: PathBuf,
    pub collection_name: String,
    pub title: String,
    pub updated: String,
    pub county_name: String,
    pub station_id: u32,
    pub station_name: String,
    pub qcv: String,
    pub year: u32,
}

impl FileProperties {
    /// Create a new instance of the data file
    pub fn new(path: PathBuf) -> Self {
        let filename = path.file_name().unwrap().to_str().unwrap();
        let parts: Vec<&str> = filename.split('_').collect();
        let collection_name = parts[0].to_string();
        let title = parts[1].to_string();
        let updated = parts[2].to_string();
        let county_name = parts[3].to_string();
        let station_id: u32 = parts[4].parse().unwrap();
        let station_name = parts[5].to_string();
        let qcv = parts[6].to_string();
        let year: u32 = parts[7].split('.').next().unwrap().parse().unwrap();

        Self { path, collection_name, title, updated, county_name, station_id, station_name, qcv, year }
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
        assert_eq!(data_file.collection_name, "midas-open");
        assert_eq!(data_file.title, "uk-hourly-weather-obs");
        assert_eq!(data_file.updated, "dv-202407");
        assert_eq!(data_file.county_name, "aberdeenshire");
        assert_eq!(data_file.station_id, 144);
        assert_eq!(data_file.station_name, "corgarff-castle-lodge");
        assert_eq!(data_file.qcv, "qcv-1");
        assert_eq!(data_file.year, 1997);
    }
}