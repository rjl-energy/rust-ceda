mod ceda_client;
mod ceda_csv_reader;
mod cli;
mod datastore;
mod db;
mod error;

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
