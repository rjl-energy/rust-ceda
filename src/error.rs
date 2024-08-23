//! Application errors

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("A generic error occurred")]
    GenericError,
}