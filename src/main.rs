use regex::Regex;
use reqwest;
use reqwest::Client;
use scraper::{Html, Selector};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let root = "https://data.ceda.ac.uk";
    let url = format!("{}{}", root, "/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-202407/");

    let region_links = get_region_links(&client, &url).await.unwrap();

    for link in region_links {
        let url = format!("{}{}", root, link);
    }

    // print links
    // for link in region_links {
    //     println!("{}", link);
    //     #content-main > div.row > div > table
    // }

    Ok(())
}

// get all links to regions from the root page
async fn get_region_links(client: &Client, root: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let res = client.get(root).send().await.unwrap();
    if !res.status().is_success() {
        return Err(format!("Failed to load page: {}", res.status()).into());
    }

    let body = res.text().await?;
    let document = Html::parse_document(&body);

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


