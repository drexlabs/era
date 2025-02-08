use std::{cell::RefCell, sync::Arc, time::Duration};

use era::{model::ConfigRoot, pipeline};
use gasket::runtime::{spawn_stage, Tether};

pub fn run(args: &Args) -> Result<Tether, era::Error> {
    let config = ConfigRoot::new(&args.config)
        .map_err(|err| era::Error::ConfigError(format!("{:?}", err)))?;

    Ok(spawn_stage(
        pipeline::Pipeline::bootstrap_gasket_stage(
            RefCell::new(config),
            Arc::new(args.console.clone().unwrap_or_default()),
        ),
        config.gasket_policy(),
    ))
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, value_parser)]
    //#[clap(description = "config file to load by the daemon")]
    config: Option<std::path::PathBuf>,

    #[clap(long, value_parser)]
    //#[clap(description = "type of progress to display")],
    console: Option<pipeline::console::Mode>,
}
