//! Represents the CEDA website and provides methods to interact with it.

use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use scraper::{Html, Selector};
use std::env;
use std::error::Error;
use std::path::Path;

/// Represents the links to the data files
#[derive(Debug, Clone)]
pub struct DataLinks {
    pub capability: String,
    pub data: Vec<String>,
}


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
    pub fn new(dataset_version: &str) -> Result<Self, Box<dyn Error>> {
        let dataset_version = dataset_version.to_string();
        let access_token = CedaClient::get_access_token();

        let mut headers = HeaderMap::new();
        let auth_value = format!("Bearer {}", access_token);
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_value)?);

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        let root = "https://data.ceda.ac.uk".to_string();

        Ok(Self { dataset_version, client, root })
    }

    /// Get the document from a URL
    async fn get_document(&self, url: &str) -> Result<Html, Box<dyn Error>> {
        let res = self.client.get(url).send().await?;
        if !res.status().is_success() {
            return Err(format!("Failed to load page: {}", res.status()).into());
        }

        let body = res.text().await?;
        let document = Html::parse_document(&body);

        Ok(document)
    }

    /// Get all links to regions from the root page
    pub async fn get_county_links(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let url = format!("{}{}{}/", self.root, "/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-", self.dataset_version);
        let document = self.get_document(&url).await.unwrap();
        let selector = Selector::parse("#results a").unwrap();

        let links: Vec<String> = document.select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .map(|href| href.to_string())
            .collect();

        // remove all links that don't start with /badc
        let re = Regex::new(r"^/badc").unwrap();
        let links: Vec<String> = links.into_iter().filter(|link| re.is_match(link)).collect();

        Ok(links)
    }

    /// Get all station links from a region page
    pub async fn get_station_links(&self, region_link: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let url = format!("{}{}", self.root, region_link);
        let document = self.get_document(&url).await.unwrap();
        let selector = Selector::parse("#content-main > div.row > div > table a").unwrap();

        let links: Vec<String> = document.select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .map(|href| href.to_string())
            .collect();

        Ok(links)
    }

    /// Get the data file links for a station
    pub async fn get_data_links(&self, station_link: &str) -> Result<DataLinks, Box<dyn Error>> {
        let url = format!("{}{}", self.root, station_link);
        let document = self.get_document(&url).await.unwrap();
        let selector = Selector::parse("#results a").unwrap();

        // Get the links to the data files
        let links: Vec<String> = document.select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .map(|href| href.to_string())
            .collect();

        // Parse the capability file link
        let capability = links.iter().find(|link| link.contains("capability.csv")).unwrap().to_string();

        // Parse the data file links
        let data = self.parse_data_links(&links).await?;

        Ok(DataLinks { capability, data })
    }

    // Parse a station page data links to get the data files
    async fn parse_data_links(&self, links: &[String]) -> Result<Vec<String>, Box<dyn Error>> {
        let mut data_link = String::new();

        if let Some(link) = links.iter().find(|link| link.contains("qc-version-1")) {
            data_link = link.clone();
        } else if let Some(link) = links.iter().find(|link| link.contains("qc-version-0")) {
            data_link = link.clone();
        }

        if data_link.is_empty() {
            return Err("No URL matches qc-version-1 or qc-version-0".into());
        }

        // Get the data links
        // FIXME: Implement this

        let data: Vec<String> = self.parse_datafile_links(&data_link).await?;

        Ok(data)
    }

    /// Parse the data file links for a given QC version link
    async fn parse_datafile_links(&self, qc_version_link: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let url = format!("{}{}", self.root, qc_version_link);
        let document = self.get_document(&url).await.unwrap();
        let selector = Selector::parse("#results a").unwrap();

        // Get the links to the data files
        let links: Vec<String> = document.select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .map(|href| href.to_string())
            .collect();

        Ok(links)
    }

    /// Download a CSV file to the specified directory
    pub async fn download_csv(&self, url: &str, dir: &Path) -> Result<(), Box<dyn Error>> {
        let res = self.client.get(url).send().await?;
        if !res.status().is_success() {
            return Err(format!("Failed to download CSV: {}", res.status()).into());
        }

        let body = res.bytes().await?;

        let filename = url.split('/').last().unwrap();

        // skip if file already exists
        if dir.join(filename).exists() {
            return Ok(());
        }

        // remove all after '.csv'
        let filename = match filename.find(".csv") {
            Some(pos) => &filename[..pos + 4],
            None => filename,
        };

        tokio::fs::write(dir.join(filename), &body).await?;

        Ok(())
    }

    fn get_access_token() -> String {
        dotenv::dotenv().ok();
        env::var("CEDA_ACCESS_TOKEN").expect("CEDA_ACCESS_TOKEN must be set")
    }
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
    async fn it_gets_capability() {
        let client = CedaClient::new("202407").unwrap();
        let region_links = client.get_county_links().await.unwrap();
        let station_link = region_links.iter().take(1).next().unwrap();
        let station_links = client.get_station_links(station_link).await.unwrap();
        let data_link = station_links.iter().take(1).next().unwrap();

        let data_links = client.get_data_links(data_link).await.unwrap();

        let re = Regex::new(r"capability.csv").unwrap();
        assert!(re.is_match(&data_links.capability));
    }

    #[tokio::test]
    #[ignore]
    async fn it_gets_datalinks() {
        let client = CedaClient::new("202407").unwrap();
        let region_links = client.get_county_links().await.unwrap();
        let station_link = region_links.iter().take(1).next().unwrap();
        let station_links = client.get_station_links(station_link).await.unwrap();
        let data_link = station_links.iter().take(1).next().unwrap();

        let data_links = client.get_data_links(data_link).await.unwrap();

        assert!(!data_links.data.is_empty());
    }
}
