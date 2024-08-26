//! A struct for reading CEDA weather data CSV files.

use crate::error;
use chrono::{DateTime, NaiveDateTime, Utc};
use csv::{Reader, StringRecord, Writer};
use error::AppError as Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

/// Represents a reader for processing CEDA weather data CSV files.
pub struct CedaCsvReader {
    pub location: Location,
    pub date_valid: DateValid,
    pub observations: Vec<Observation>,
}

/// The location of a weather station.
#[derive(Debug, PartialEq)]
pub struct Location {
    pub lat: f32,
    pub lon: f32,
}

/// The valid date range for the weather data.
#[derive(Debug)]
pub struct DateValid {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// A weather observation.
#[derive(Debug, Default)]
pub struct Observation {
    pub date_time: NaiveDateTime,
    pub id: u32,
    pub wind: WindObservation,

}

/// A wind observation.
#[derive(Debug, Default, PartialEq)]
pub struct WindObservation {
    pub wind_speed: Option<f32>,
    pub wind_direction: Option<f32>,
    pub unit_id: Option<u32>,
    pub opr_type: Option<u32>,
}

impl CedaCsvReader {
    /// Create a parsed weather data object from a CSV file.
    pub fn new(path: PathBuf) -> Result<Self, Error> {
        let file = File::open(&path).map_err(|_| Error::FileNotFound)?;
        let reader = BufReader::new(file);
        let lines = reader.lines().collect::<Result<Vec<String>, _>>().map_err(|_| Error::FileReadError)?;

        let location = CedaCsvReader::parse_location(&lines)?;
        let date_valid = CedaCsvReader::parse_date_valid(&lines)?;
        let observations = CedaCsvReader::parse_observations(&lines)?;

        Ok(Self {
            location,
            date_valid,
            observations,
        })
    }

    fn parse_location(lines: &[String]) -> Result<Location, Error> {
        let parts: Vec<String> = lines[14].split(',').map(|s| s.to_string()).collect();

        if parts[0] != "location" {
            return Err(Error::CsvLocationMissingError);
        }

        let lat = parts[2].parse::<f32>()?;
        let lon = parts[3].parse::<f32>()?;

        Ok(Location { lat, lon })
    }

    fn parse_date_valid(lines: &[String]) -> Result<DateValid, Error> {
        let parts: Vec<String> = lines[16].split(',').map(|s| s.to_string()).collect();

        if parts[0] != "date_valid" {
            return Err(Error::CsvDateValidMissingError);
        }

        let date_from_naivedate = NaiveDateTime::parse_from_str(&parts[2], "%Y-%m-%d %H:%M:%S")?;
        let date_to_naivedate = NaiveDateTime::parse_from_str(&parts[3], "%Y-%m-%d %H:%M:%S")?;

        Ok(DateValid {
            from: DateTime::<Utc>::from_naive_utc_and_offset(date_from_naivedate, Utc),
            to: DateTime::<Utc>::from_naive_utc_and_offset(date_to_naivedate, Utc),
        })
    }

    // Parse the observations from the CSV data
    fn parse_observations(lines: &[String]) -> Result<Vec<Observation>, Error> {

        // Read the CSV data to a string
        let csv_data = CedaCsvReader::vec_to_csv(lines)?;

        // Process the CSV data
        let mut rdr = Reader::from_reader(csv_data.as_bytes());
        let headers = rdr.headers().unwrap().clone();

        let date_time_index = CedaCsvReader::get_column_index(&headers, "ob_time")?;
        let id_index = CedaCsvReader::get_column_index(&headers, "id")?;
        let wind_speed_index = CedaCsvReader::get_column_index(&headers, "wind_speed")?;
        let wind_direction_index = CedaCsvReader::get_column_index(&headers, "wind_direction")?;
        let wind_speed_unit_id_index = CedaCsvReader::get_column_index(&headers, "wind_speed_unit_id")?;
        let src_opr_type_index = CedaCsvReader::get_column_index(&headers, "src_opr_type")?;

        let mut observations = Vec::new();
        for result in rdr.records() {
            let record = result.unwrap();
            let date_time = NaiveDateTime::parse_from_str(&record[date_time_index], "%Y-%m-%d %H:%M:%S")?;
            let id = record[id_index].parse::<u32>().unwrap();
            let wind = Self::parse_wind(wind_speed_index, wind_direction_index, wind_speed_unit_id_index, src_opr_type_index, record);

            let observation = Observation {
                date_time,
                id,
                wind,
            };
            observations.push(observation);
        }

        Ok(observations)
    }

    fn get_column_index(headers: &StringRecord, column_name: &str) -> Result<usize, Error> {
        headers.iter().position(|h| h == column_name).ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))
    }

    fn parse_wind(wind_speed_index: usize, wind_direction_index: usize, wind_speed_unit_id_index: usize, src_opr_type_index: usize, record: StringRecord) -> WindObservation {
        let wind_speed = record[wind_speed_index].parse::<f32>().ok();
        let wind_direction = record[wind_direction_index].parse::<f32>().ok();
        let wind_speed_unit_id = record[wind_speed_unit_id_index].parse::<u32>().ok();
        let src_opr_type = record[src_opr_type_index].parse::<u32>().ok();

        let wind = WindObservation {
            wind_speed,
            wind_direction,
            unit_id: wind_speed_unit_id,
            opr_type: src_opr_type,
        };
        wind
    }

    // Convert a vector of strings to a CSV string
    fn vec_to_csv(lines: &[String]) -> Result<String, Error> {
        let mut wtr = Writer::from_writer(vec![]);

        let mut iter = lines.iter();

        // Skip lines until the header row containing "ob_time" is found
        while let Some(line) = iter.next() {
            let parts = line.split(',').map(|s| s.to_string()).collect::<Vec<String>>();

            if parts[0] == "ob_time" {
                wtr.write_record(line.split(',').collect::<Vec<&str>>()).unwrap();
                break;
            }
        }

        // Write the remaining lines to the CSV writer
        for line in iter {
            let parts: Vec<&str> = line.split(',').collect();
            if parts[0] != "end data" {
                wtr.write_record(&parts).unwrap();
            }
        }

        let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();

        Ok(data)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_creates_new() {
        let file_path = get_test_file_path();
        let _ = CedaCsvReader::new(file_path);
    }

    #[test]
    fn it_gets_date_valid() {
        let file_path = get_test_file_path();
        let reader = CedaCsvReader::new(file_path).unwrap();
        let expected_from_date = DateTime::parse_from_rfc3339("1994-01-01T00:00:00Z").unwrap();
        let expected_to_date = DateTime::parse_from_rfc3339("1994-12-31T23:59:59Z").unwrap();

        assert_eq!(reader.date_valid.from, expected_from_date);
        assert_eq!(reader.date_valid.to, expected_to_date);
    }

    #[test]
    fn it_gets_location() {
        let file_path = get_test_file_path();
        let reader = CedaCsvReader::new(file_path).unwrap();
        let expected_location = Location { lat: 54.865, lon: -6.458 };

        assert_eq!(reader.location, expected_location);
    }

    #[test]
    fn it_gets_observation_date() {
        let file_path = get_test_file_path();
        let reader = CedaCsvReader::new(file_path).unwrap();
        let observation = &reader.observations[0];

        let date_time_expected = NaiveDateTime::parse_from_str("1994-10-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        // assert_eq!(reader.observations.len(), 315);
        assert_eq!(observation.date_time, date_time_expected);
    }

    #[test]
    fn it_gets_observation_id() {
        let file_path = get_test_file_path();
        let reader = CedaCsvReader::new(file_path).unwrap();
        let observation = &reader.observations[0];

        assert_eq!(observation.id, 3915);
    }

    #[test]
    fn it_gets_observation_wind() {
        let file_path = get_test_file_path();
        let reader = CedaCsvReader::new(file_path).unwrap();
        let observation = &reader.observations[2];

        let expected_wind = WindObservation {
            wind_speed: Some(4.0),
            wind_direction: Some(170.0),
            unit_id: None,
            opr_type: None,
        };

        assert_eq!(observation.wind, expected_wind);
    }

    fn get_test_file_path() -> PathBuf {
        PathBuf::from("/Users/richardlyon/Documents/CEDA/raw/data/midas-open_uk-hourly-weather-obs_dv-202407_antrim_01448_portglenone_qcv-1_1994.csv")
    }
}
