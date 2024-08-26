mod datastore;
mod ceda;
mod cli;
mod error;
mod ceda_csv_reader;
mod db;

use crate::cli::{command, Cli, Commands};
use clap::Parser;
use error::AppError as Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Update {} => command::update().await,
        Commands::Process { init } => command::process(*init).await,
    }
}

