//! Update datafiles command
//!
//! Downloads the latest datafiles from the CEDA API.

use crate::ceda_client::CedaClient;
use crate::datastore::DataStore;
use crate::error::{AppError as Error, AppError};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub async fn update() -> Result<(), Error> {
    let client = CedaClient::new("202407").map_err(|_| Error::GenericError)?;

    let county_links = get_county_links(&client).await?;
    let station_links = get_station_links(&client, county_links).await?;
    let data_folder_links = get_data_folder_links(&client, station_links).await?;
    let (all_data_file_links, datalinks_count) = get_data_file_links(&client, data_folder_links).await?;
    download_data(client, all_data_file_links, datalinks_count).await?;

    Ok(())
}

async fn get_county_links(client: &CedaClient) -> Result<Vec<String>, AppError> {
    let sp = create_spinner("Fetching county links...".to_string());
    let client_clone = client.clone();

    let county_links_task = tokio::spawn(async move {
        client_clone
            .get_county_links()
            .await
            .map_err(|_| Error::GenericError)
    });

    let county_links = county_links_task.await.map_err(|_| Error::GenericError)??;
    sp.finish_with_message(format!("Fetched {} county links", county_links.len()));

    Ok(county_links)
}

async fn get_station_links(
    client: &CedaClient,
    county_links: Vec<String>,
) -> Result<Vec<String>, AppError> {
    let pb = create_progress_bar(
        county_links.len() as u64,
        "Fetching station links...".to_string(),
    );
    let mut tasks = Vec::new();

    for county_link in county_links {
        let client = client.clone();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            let station_links = client
                .get_station_links(&county_link)
                .await
                .map_err(|_| Error::GenericError)?;
            pb.inc(1);
            Ok::<Vec<String>, Error>(station_links)
        }));
    }

    let results = join_all(tasks).await;
    let mut all_station_links: Vec<String> = Vec::new();
    for result in results {
        match result {
            Ok(Ok(station_links)) => all_station_links.extend(station_links),
            _ => return Err(Error::GenericError),
        }
    }

    pb.finish_with_message(format!("Fetched {} station links", all_station_links.len()));

    Ok(all_station_links)
}


async fn get_data_folder_links(
    client: &CedaClient,
    station_links: Vec<String>,
) -> Result<Vec<String>, AppError> {
    let pb = create_progress_bar(
        station_links.len() as u64,
        "Fetching data folder links...".to_string(),
    );
    let mut tasks = Vec::new();

    for station_link in station_links {
        let client = client.clone();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            let data_folder_link = client
                .get_data_folder_link(&station_link)
                .await?;
            pb.inc(1);
            Ok::<String, Error>(data_folder_link)
        }));
    }

    let results = join_all(tasks).await;

    let mut all_data_folder_links: Vec<String> = Vec::new();
    for result in results.into_iter().filter_map(Result::ok).filter_map(Result::ok) {
        all_data_folder_links.push(result);
    }

    pb.finish_with_message(format!(
        "Fetched {} data folder links",
        all_data_folder_links.len()
    ));

    Ok(all_data_folder_links)
}

async fn get_data_file_links(client: &CedaClient, data_folder_links: Vec<String>) -> Result<(Vec<String>, u32), Error> {
    let pb = create_progress_bar(
        data_folder_links.len() as u64,
        "Fetching data file links...".to_string(),
    );
    let mut tasks = Vec::new();

    for data_folder_link in data_folder_links {
        let client = client.clone();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            let data_file_links = client
                .get_data_file_links(&data_folder_link)
                .await
                .map_err(|_| Error::GenericError)?;
            pb.inc(1);
            Ok::<Vec<String>, Error>(data_file_links)
        }));
    }

    let results = join_all(tasks).await;
    let mut all_data_file_links: Vec<String> = Vec::new();
    for data_file_links in results.into_iter().filter_map(|r| r.ok()).filter_map(|r| r.ok()) {
        all_data_file_links.extend(data_file_links);
    }
    let data_file_links_count = all_data_file_links.len() as u32;
    pb.finish_with_message(format!("Fetched {} data file links", data_file_links_count));

    Ok((all_data_file_links, data_file_links_count))
}


async fn download_data(
    client: CedaClient,
    all_data_links: Vec<String>,
    datalinks_count: u32,
) -> Result<(), AppError> {
    let datastore = DataStore::new();

    let pb = create_progress_bar(
        datalinks_count as u64,
        "Downloading data files...".to_string(),
    );
    let mut tasks = Vec::new();

    for data_link in all_data_links.iter() {
        let client = client.clone();
        let rawdata_dir = datastore.rawdata_dir();
        let pb = pb.clone();
        let data_link = data_link.clone();

        tasks.push(tokio::spawn(async move {
            client
                .download_csv(&data_link, &rawdata_dir)
                .await
                .map_err(|_| Error::GenericError)?;
            pb.inc(1);

            Ok::<(), Error>(())
        }));
    }

    join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    pb.finish_with_message("Downloaded data files");
    Ok(())
}


fn create_spinner(message: String) -> ProgressBar {
    let bar = ProgressBar::new_spinner().with_message(message);
    bar.enable_steady_tick(Duration::from_millis(100));

    bar
}

fn create_progress_bar(size: u64, message: String) -> ProgressBar {
    ProgressBar::new(size).with_message(message).with_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {msg}")
            .unwrap()
            .progress_chars("##-"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_updates() {
        let _ = update().await;
    }
}
