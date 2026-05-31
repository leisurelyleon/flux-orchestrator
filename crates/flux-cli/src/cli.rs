//! Command-line argument definitions.

use clap::{Parser, Subcommand};

/// A fault-tolerant distributed job orchestrator.
#[derive(Debug, Parser)]
#[command(name = "flux", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run an end-to-end demonstration on the in-memory bus (no broker needed).
    Demo,
}
