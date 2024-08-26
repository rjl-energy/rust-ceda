//! Process datafiles command
//!
//! Loads the CSV data in the datastore to a SQLITE database.

use crate::ceda_csv_reader::CedaCsvReader;
use crate::datastore;
use crate::db::Database;
use crate::error::AppError as Error;

pub async fn process(init: bool) -> Result<(), Error> {
    let datastore = datastore::DataStore::new();
    let db = Database::new().await.unwrap();

    if init {
        db.init().await?;
    }

    let data_files = datastore.list_data_files();

    for data_file in data_files.into_iter().take(5) {
        let record = CedaCsvReader::new(data_file.path)?;

        db.insert_station(
            record.midas_station_id,
            &record.historic_county_name,
            &record.observation_station,
            record.location.lat,
            record.location.lon,
            record.height,
        )
        .await?;

        for observation in record.observations {
            db.insert_observation(
                record.midas_station_id,
                observation.date_time,
                observation.wind.speed,
                observation.wind.direction,
                observation.wind.unit_id,
                observation.wind.opr_type,
            )
            .await?;
        }
    }

    Ok(())
}
