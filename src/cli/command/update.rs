//! Update datafiles command
//!
//! Downloads the latest datafiles from the CEDA API.

use crate::ceda::{CedaClient, DataLinks};
use crate::datastore::DataStore;
use crate::error::{AppError as Error, AppError};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub async fn update() -> Result<(), Error> {
    let client = CedaClient::new("202407").map_err(|_| Error::GenericError)?;

    let county_links = get_county_links(&client).await?;
    let station_links = get_station_links(&client, county_links).await?;
    let (all_data_links, datalinks_count) = get_data_links(&client, station_links).await?;

    download_data(client, all_data_links, datalinks_count).await?;

    Ok(())
}

async fn download_data(client: CedaClient, all_data_links: Vec<DataLinks>, datalinks_count: usize) -> Result<(), AppError> {
    let datastore = DataStore::new();

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

async fn get_data_links(client: &CedaClient, station_links: Vec<String>) -> Result<(Vec<DataLinks>, usize), AppError> {
    let pb = create_progress_bar(station_links.len() as u64, "Fetching data links...".to_string());
    let mut all_data_links: Vec<DataLinks> = Vec::new();
    for station_link in station_links {
        let data_links = client.get_data_links(&station_link).await.map_err(|_| Error::GenericError)?;
        all_data_links.push(data_links);
        pb.inc(1);
    }

    let datalinks_count = count_data_links(&mut all_data_links);

    pb.finish_with_message(format!("Fetched {} data links", datalinks_count));
    Ok((all_data_links, datalinks_count))
}

fn count_data_links(all_data_links: &mut Vec<DataLinks>) -> usize {
    let mut datalinks_count = 0;
    for data_links in all_data_links.iter() {
        datalinks_count += data_links.data.len();
    }
    datalinks_count
}

async fn get_station_links(client: &CedaClient, county_links: Vec<String>) -> Result<Vec<String>, AppError> {
    let pb = create_progress_bar(county_links.len() as u64, "Fetching station links...".to_string());
    let mut all_station_links: Vec<String> = Vec::new();
    for county_link in county_links {
        let station_links = client.get_station_links(&county_link).await.map_err(|_| Error::GenericError)?;
        all_station_links.extend(station_links);
        pb.inc(1);
    }
    pb.finish_with_message(format!("Fetched {} station links", all_station_links.len()));
    Ok(all_station_links)
}

async fn get_county_links(client: &CedaClient) -> Result<Vec<String>, AppError> {
    let sp = create_spinner("Fetching county links...".to_string());
    let client_clone = client.clone();

    let county_links_task = tokio::spawn(async move {
        client_clone.get_county_links().await.map_err(|_| Error::GenericError)
    });

    let county_links = county_links_task.await.map_err(|_| Error::GenericError)??;
    sp.finish_with_message(format!("Fetched {} county links", county_links.len()));

    Ok(county_links)
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
