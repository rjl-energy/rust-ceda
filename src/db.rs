//! Database handler

use crate::datastore::DataStore;
use crate::error::AppError as Error;
use chrono::NaiveDateTime;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite};

#[derive(Debug)]
pub struct Database {
    pool: Pool<Sqlite>,
}

impl Database {
    pub async fn new() -> Result<Self, Error> {
        let datastore = DataStore::new();
        let db_path = datastore.db_dir().join("weather.sqlite");

        // FIXME: Figure out why it won't create the database
        // Create the connection pool
        let database_url = format!("sqlite:{}", db_path.to_str().ok_or(Error::GenericError)?);
        let pool: Pool<Sqlite> = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(database_url.as_str())
            .await?;


        Ok(Self { pool })
    }

    pub async fn init(&self) -> Result<(), Error> {
        // Drop tables if they exist
        sqlx::query(
            r#"
        PRAGMA foreign_keys = OFF;
        DROP TABLE IF EXISTS stations;
        DROP TABLE IF EXISTS observations;
        PRAGMA foreign_keys = ON;
        "#
        )
            .execute(&self.pool)
            .await?;

        // Create tables if they do not exist
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            midas_station_id INTEGER NOT NULL UNIQUE,
            historic_county_name TEXT NOT NULL,
            observation_station TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            height INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            midas_station_id INTEGER NOT NULL,
            date_time TEXT NOT NULL,
            wind_speed REAL,
            wind_direction REAL,
            wind_unit_id INTEGER,
            wind_opr_type INTEGER,
            FOREIGN KEY (midas_station_id) REFERENCES stations (midas_station_id)
        );
        "#
        )
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn insert_station(&self, midas_station_id: u32, historic_county_name: &str, observation_station: &str, lat: f32, lon: f32, height: u32) -> Result<i64, Error> {
        let result = sqlx::query(
            r#"
        INSERT INTO stations (midas_station_id, historic_county_name, observation_station, lat, lon, height)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(midas_station_id) DO NOTHING;
        "#
        )
            .bind(midas_station_id)
            .bind(historic_county_name)
            .bind(observation_station)
            .bind(lat)
            .bind(lon)
            .bind(height)
            .execute(&self.pool)
            .await?;

        Ok(result.last_insert_rowid())
    }

    pub async fn insert_observation(&self, midas_station_id: u32, date_time: NaiveDateTime, wind_speed: Option<f32>, wind_direction: Option<f32>, wind_unit_id: Option<u32>, wind_opr_type: Option<u32>) -> Result<i64, sqlx::Error> {
        let date_time_str = date_time.format("%Y-%m-%d %H:%M:%S").to_string();


        let result = sqlx::query(
            r#"
        INSERT INTO observations (midas_station_id, date_time, wind_speed, wind_direction, wind_unit_id, wind_opr_type)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING;
        "#
        )
            .bind(midas_station_id)
            .bind(date_time_str)
            .bind(wind_speed)
            .bind(wind_direction)
            .bind(wind_unit_id)
            .bind(wind_opr_type)
            .execute(&self.pool)
            .await?;

        Ok(result.last_insert_rowid())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new() {
        let db = Database::new().await;

        assert!(db.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_init() {
        let db = Database::new().await.unwrap();
        let result = db.init().await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_station() {
        let db = Database::new().await.unwrap();
        // let _ = db.init().await;
        let result = db.insert_station(1, "Dublin", "DUB", 10.0, 180.0, 1).await;

        println!("{:?}", result);

        // assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_observation() {
        let db = Database::new().await.unwrap();
        let datetime = NaiveDateTime::parse_from_str("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let _ = db.init().await;
        let _ = db.insert_station(1, "Dublin", "DUB", 10.0, 180.0, 1).await;
        let result = db.insert_observation(1, datetime, Some(10.0), Some(180.0), Some(1), Some(1)).await;

        println!("{:?}", result);

        assert!(result.is_ok());
    }
}