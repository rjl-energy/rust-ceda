pub mod command;

use clap::{command, Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// Contains the commands.
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
/// Available commands.
pub enum Commands {
    /// Update datafiles
    Update {},
    /// Process datafiles
    Process {
        #[arg(short, long, default_value_t = false)]
        /// Initialise the database WARNING: This will delete all data and cannot be undone
        init: bool,
    },
}
