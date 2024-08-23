mod datastore;
mod ceda;

use crate::ceda::CedaClient;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let datastore = datastore::DataStore::new();
    let client = CedaClient::new("202407")?;
    let mut all_station_links: Vec<String> = Vec::new();

    let region_links = client.get_region_links().await?;

    // Get the stations for each region
    for link in region_links.iter().take(1) { // FIXME: remove .take(1) to get all regions
        let station_links = client.get_station_links(link).await?;
        all_station_links.extend(station_links);
    }

    println!("Downloading to {:?}", datastore.root);

    // Download the data for each station
    for station_link in all_station_links.into_iter().take(1) {
        let data_links = client.get_data_links(&station_link).await.unwrap();
        client.download_csv(&data_links.capability, &datastore.capability_dir()).await.unwrap();
        for link in data_links.data.iter() {
            client.download_csv(&link, &datastore.rawdata_dir()).await.unwrap();
        }
    }


    Ok(())
}

// async fn download(url: &str, dir: &PathBuf) -> Result<(), Box<dyn Error>> {
//     let res = reqwest::get(url).await.unwrap();
//     let body = res.bytes().await?;
//     let filename = url.split('/').last().unwrap();
//     std::fs::write(dir.join(filename), &body)?;
//
//     Ok(())
// }


// Download capabilities file
// async fn download_capabilites(client: &Client, links: &Vec<String>) -> Result<(), Box<dyn Error>> {
//     let capabilities_url = links.iter().find(|link| link.contains("capability")).unwrap();
//
//
//     let res = client.get(capabilities_url).send().await.unwrap();
//     if !res.status().is_success() {
//         return Err(format!("Failed to download CSV: {}", res.status()).into());
//     }
//
//     let body = res.bytes().await?;
//     let filename = capabilities_url.split('/').last().unwrap();
//     std::fs::write(filename, &body)?;
//
//
//     Ok(())
// }

// Download data file
// async fn download_data(client: &Client, links: &Vec<String>) -> Result<(), Box<dyn Error>> {
//     for link in links.iter().filter(|link| link.contains("data")) {
//         let res = client.get(link).send().await.unwrap();
//         let body = res.text().await?;
//         let filename = link.split("/").last().unwrap();
//         std::fs::write(filename, body)?;
//     }
//
//     Ok(())
// }

