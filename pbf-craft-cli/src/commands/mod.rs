mod boundary;
mod diff;
mod export;
mod search;
mod with_deps;

use clap::Subcommand;

#[derive(Subcommand)]
pub enum Commands {
    /// Get a element with dependencies
    Get(with_deps::GetCommand),
    /// search for elements
    Search(search::SearchCommand),
    /// export database to a PBF file
    Export(export::ExportCommand),
    /// an experimental feature
    Diff(diff::DiffCommand),
    /// get the boundary of a PBF file
    Boundary(boundary::BoundaryCommand),
}

impl Commands {
    pub fn run(self) {
        match self {
            Commands::Get(command) => {
                command.run();
            }
            Commands::Search(command) => {
                command.run();
            }
            Commands::Export(command) => {
                command.run();
            }
            Commands::Diff(command) => {
                command.run();
            }
            Commands::Boundary(command) => command.run(),
        }
    }
}
