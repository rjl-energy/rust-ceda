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
            client.download_csv(link, &datastore.rawdata_dir()).await.unwrap();
        }
    }

    Ok(())
}

