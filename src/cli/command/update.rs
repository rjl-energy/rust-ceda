//! Update datafiles command
//!
//! Downloads the latest datafiles from the CEDA API.

use crate::ceda::{CedaClient, DataLinks};
use crate::datastore;
use crate::error::AppError as Error;
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub async fn update() -> Result<(), Error> {
    let datastore = datastore::DataStore::new();
    let client = CedaClient::new("202407").map_err(|_| Error::GenericError)?;
    let mut all_station_links: Vec<String> = Vec::new();
    let mut all_data_links: Vec<DataLinks> = Vec::new();

    // Get the county links
    let sp = create_spinner("Fetching county links...".to_string());
    let county_links = client.get_county_links().await.map_err(|_| Error::GenericError)?;
    sp.finish_with_message(format!("Fetched {} county links", county_links.len()));

    // Get the stations for each county
    let pb = create_progress_bar(county_links.len() as u64, "Fetching station links...".to_string());
    for county_link in county_links.into_iter().take(2) { // FIXME: remove .take(1) to get all counties
        let station_links = client.get_station_links(&county_link).await.map_err(|_| Error::GenericError)?;
        all_station_links.extend(station_links);
        pb.inc(1);
    }
    pb.finish_with_message(format!("Fetched {} station links", all_station_links.len()));

    // Get the data links for each station
    let pb = create_progress_bar(all_station_links.len() as u64, "Fetching data links...".to_string());
    for station_link in all_station_links.into_iter().take(5) {
        let data_links = client.get_data_links(&station_link).await.map_err(|_| Error::GenericError)?;
        all_data_links.push(data_links);
        pb.inc(1);
    }

    let mut datalinks_count = 0;
    for data_links in all_data_links.iter() {
        datalinks_count += data_links.data.len();
    }

    pb.finish_with_message(format!("Fetched {} data links", datalinks_count));

    // Download the data files
    let pb = create_progress_bar(datalinks_count as u64, "Downloading data files...".to_string());
    let mut tasks = Vec::new();
    
    for data_links in all_data_links.iter() {
        let client = client.clone();
        let capability_dir = datastore.capability_dir();
        let rawdata_dir = datastore.rawdata_dir();
        let pb = pb.clone();
        let data_links = data_links.clone();

        tasks.push(tokio::spawn(async move {
            client.download_csv(&data_links.capability, &capability_dir).await.map_err(|_| Error::GenericError)?;
            for link in data_links.data.iter() {
                client.download_csv(link, &rawdata_dir).await.map_err(|_| Error::GenericError)?;
                pb.inc(1);
            }
            Ok::<(), Error>(())
        }));
    }

    join_all(tasks).await.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

    pb.finish_with_message("Downloaded data files");

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
