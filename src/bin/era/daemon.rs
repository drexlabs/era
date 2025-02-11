use std::{cell::RefCell, sync::Arc};

use era::{
    model::ConfigRoot,
    pipeline::{self, console},
};
use gasket::{
    messaging::{tokio::connect_ports, InputPort, OutputPort},
    runtime::{spawn_stage, Tether},
};

pub fn run(args: &Args) -> Result<Tether, era::Error> {
    let mut config = ConfigRoot::new(&args.config)
        .map_err(|err| era::Error::ConfigError(format!("{:?}", err)))?;

    if let Some(display_console_argument) = &args.console {
        config.display.replace(console::Config {
            mode: display_console_argument.clone(),
        });
    }

    let pipeline_config = config.pipeline.take().unwrap_or_default();

    let gasket_policy = config.gasket_policy();

    Ok(pipeline_config
        .bootstrapper(RefCell::new(config))
        .spawn_stage(gasket_policy))
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
