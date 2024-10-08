//! Application errors

use std::num::ParseFloatError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("A generic error occurred")]
    GenericError,

    // CEDA API errors
    #[error("Document Fetch error: {0}")]
    DocumentFetchError(String),

    // File errors
    #[error("File not found")]
    FileNotFound,
    #[error("File read error")]
    FileReadError,

    // CSV Parse Errors
    #[error("CSV Observation Station parsing error")]
    CsvObservationStationParsingError,
    #[error("CSV Historic County Name parsing error")]
    CsvHistoricCountyNameParsingError,
    #[error("CSV Midas Station ID parsing error")]
    CsvMidasStationIdParsingError,
    #[error("CSV Height parsing error")]
    CsvHeightParsingError,
    #[error("CSV Location field error")]
    CsvLocationMissingError,
    #[error("CSV Location parse error")]
    CsvLocationParsingError(#[from] ParseFloatError),
    #[error("CSV Date Valid field error")]
    CsvDateValidMissingError,
    #[error("CSV Date Parse error: {0}")]
    CsvDateParseError(#[from] chrono::ParseError),
    #[error("CSV Reading Column not found: {0}")]
    ColumnNotFound(String),
    #[error("CSV Reading QCV1 Folder Not Found error")]
    QCV1NotFound,

    // Database errors
    #[error("Database connection error")]
    DatabaseConnectionError(#[from] sqlx::Error),

}
