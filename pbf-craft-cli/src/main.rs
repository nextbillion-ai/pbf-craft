mod commands;
mod db;

use env_logger;
use std::time::Instant;

use clap::Parser;

#[macro_use]
extern crate colour;

#[macro_use]
extern crate anyhow;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: commands::Commands,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let start = Instant::now();

    let cli = Cli::parse();
    cli.command.run();

    let end = Instant::now();
    green!("Finished ");
    println!(" in {:?}", end.duration_since(start));

    Ok(())
}
