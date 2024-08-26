//! Represents the CEDA website and provides methods to interact with it.

use crate::error::AppError as Error;
use futures::stream::StreamExt;
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use scraper::{Html, Selector};
use std::env;
use std::path::Path;
use tokio::fs::File;
use tokio::io::copy;
use tokio_util::io::StreamReader;

/// Represents the CEDA client
#[derive(Debug, Clone)]
pub struct CedaClient {
    dataset_version: String,
    client: reqwest::Client,
    root: String,
}

impl CedaClient {
    /// Create a new instance of the CEDA client
    ///
    /// dataset_version: The version of the dataset to use e.g. "202407"
    pub fn new(dataset_version: &str) -> Result<Self, Error> {
        let dataset_version = dataset_version.to_string();
        let access_token = CedaClient::get_access_token();

        let mut headers = HeaderMap::new();
        let auth_value = format!("Bearer {}", access_token);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value).map_err(|_| Error::GenericError)?,
        );

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|_| Error::GenericError)?;

        let root = "https://data.ceda.ac.uk".to_string();

        Ok(Self {
            dataset_version,
            client,
            root,
        })
    }

    /// Get the document from a URL
    async fn get_document(&self, url: &str) -> Result<Html, Error> {
        let res = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| Error::GenericError)?;
        if !res.status().is_success() {
            return Err(Error::GenericError);
        }

        let body = res.text().await.map_err(|_| Error::GenericError)?;
        let document = Html::parse_document(&body);

        Ok(document)
    }

    /// Get all links to regions from the root page
    pub async fn get_county_links(&self) -> Result<Vec<String>, Error> {
        let url = format!(
            "{}{}{}/",
            self.root,
            "/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-",
            self.dataset_version
        );
        let document = self.get_document(&url).await.unwrap();
        let selector = Selector::parse("#results a").unwrap();

        let re_start = Regex::new(r"^/badc").unwrap();
        let re_end = Regex::new(r"change_log_station_files$").unwrap();

        let links: Vec<String> = document
            .select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .map(|href| href.to_string())
            .filter(|link| re_start.is_match(link) && !re_end.is_match(link))
            .collect();

        Ok(links)
    }

    /// Get all station links from a region page
    pub async fn get_station_links(&self, region_link: &str) -> Result<Vec<String>, Error> {
        let url = format!("{}{}", self.root, region_link);
        let document = self.get_document(&url).await.unwrap();
        let selector = Selector::parse("#content-main > div.row > div > table a").unwrap();

        let links: Vec<String> = document
            .select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .map(|href| href.to_string())
            .collect();

        Ok(links)
    }

    /// Get the data folder link for a station
    pub async fn get_data_folder_link(&self, station_link: &str) -> Result<String, Error> {
        let url = format!("{}{}", self.root, station_link);
        let document = self.get_document(&url).await.map_err(|e| Error::DocumentFetchError(e.to_string()))?;

        let link = extract_qc_version_1_link(&document.html()).ok_or(Error::QCV1NotFound)?;

        Ok(link)
    }

    /// Get the data file links for a data folder
    pub async fn get_data_file_links(&self, data_folder_link: &str) -> Result<Vec<String>, Error> {
        let url = format!("{}{}", self.root, data_folder_link);
        let document = self.get_document(&url).await.unwrap();
        let selector = Selector::parse("#results a").unwrap();

        // Get the links to the data files
        let data_file_links: Vec<String> = document
            .select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .map(|href| href.to_string())
            .collect();

        Ok(data_file_links)
    }


    /// Download a CSV file to the specified directory
    pub async fn download_csv(&self, url: &str, dir: &Path) -> Result<(), Error> {
        let res = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| Error::GenericError)?;
        if !res.status().is_success() {
            return Err(Error::GenericError);
        }

        let filename = url.split('/').last().unwrap();

        // remove all after '.csv'
        let filename = match filename.find(".csv") {
            Some(pos) => &filename[..pos + 4],
            None => filename,
        };

        // skip if file already exists
        if dir.join(filename).exists() {
            return Ok(());
        }

        let file_path = dir.join(filename);
        let mut file = File::create(&file_path)
            .await
            .map_err(|_| Error::GenericError)?;
        let stream = res
            .bytes_stream()
            .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
        let mut stream_reader = StreamReader::new(stream);

        copy(&mut stream_reader, &mut file)
            .await
            .map_err(|_| Error::GenericError)?;

        Ok(())
    }

    fn get_access_token() -> String {
        dotenv::dotenv().ok();
        env::var("CEDA_ACCESS_TOKEN").expect("CEDA_ACCESS_TOKEN must be set")
    }
}

fn extract_qc_version_1_link(html: &str) -> Option<String> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("#results a").unwrap();

    for element in document.select(&selector) {
        if element.text().any(|text| text == "qc-version-1") {
            return element.value().attr("href").map(|href| href.to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_new() {
        let _client = CedaClient::new("202407");
    }

    #[tokio::test]
    #[ignore]
    async fn it_gets_region_links() {
        let client = CedaClient::new("202407").unwrap();

        let links = client.get_county_links().await.unwrap();

        assert!(!links.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn it_gets_station_links() {
        let client = CedaClient::new("202407").unwrap();
        let region_links = client.get_county_links().await.unwrap();
        let station_link = region_links.iter().take(1).next().unwrap();

        let station_links = client.get_station_links(station_link).await.unwrap();

        assert!(!station_links.is_empty());
    }


    #[tokio::test]
    #[ignore]
    async fn it_gets_datalinks() {
        let client = CedaClient::new("202407").unwrap();
        let region_links = client.get_county_links().await.unwrap();
        let station_link = region_links.iter().take(1).next().unwrap();
        let station_links = client.get_station_links(station_link).await.unwrap();
        let data_link = station_links.iter().take(1).next().unwrap();

        let data_links = client.get_data_file_links(data_link).await.unwrap();

        assert!(!data_links.is_empty());
    }
}
