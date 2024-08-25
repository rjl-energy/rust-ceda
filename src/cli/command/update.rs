//! Update datafiles command
//!
//! Downloads the latest datafiles from the CEDA API.

use crate::ceda::CedaClient;
use crate::datastore;
use crate::error::AppError as Error;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub async fn update() -> Result<(), Error> {
    let datastore = datastore::DataStore::new();
    let client = CedaClient::new("202407").map_err(|_| Error::GenericError)?;
    let mut all_station_links: Vec<String> = Vec::new();

    let region_links = client.get_region_links().await.map_err(|_| Error::GenericError)?;


    // Get the stations for each region
    for link in region_links.iter().take(1) { // FIXME: remove .take(1) to get all regions
        let station_links = client.get_station_links(link).await.map_err(|_| Error::GenericError)?;
        all_station_links.extend(station_links);
    }

    println!("Downloading to {:?}", datastore.root);

    let total_stations = all_station_links.len();

    let pb = create_progress_bar(total_stations as u64, "Fetching station data".to_string());

    // Download the data for each station
    for station_link in all_station_links.into_iter().take(5) {
        let data_links = client.get_data_links(&station_link).await.map_err(|_| Error::GenericError)?;
        client.download_csv(&data_links.capability, &datastore.capability_dir()).await.map_err(|_| Error::GenericError)?;
        for link in data_links.data.iter() {
            client.download_csv(link, &datastore.rawdata_dir()).await.map_err(|_| Error::GenericError)?;
        }

        pb.inc(1);
    }

    pb.finish_with_message("Finished fetching station data");

    Ok(())
}


pub fn create_spinner(message: String) -> ProgressBar {
    let bar = ProgressBar::new_spinner().with_message(message);
    bar.enable_steady_tick(Duration::from_millis(100));

    bar
}

pub fn create_progress_bar(size: u64, message: String) -> ProgressBar {
    ProgressBar::new(size).with_message(message).with_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {msg}")
            .unwrap()
            .progress_chars("##-"),
    )
}
