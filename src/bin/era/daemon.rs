use std::{cell::RefCell, sync::Arc, time::Duration};

use era::{model::ConfigRoot, pipeline};
use gasket::runtime::{spawn_stage, Tether};
use serde::Deserialize;

pub fn run(args: &Args) -> Result<Tether, era::Error> {
    let config = ConfigRoot::new(&args.config)
        .map_err(|err| era::Error::ConfigError(format!("{:?}", err)))?;

    // todo make configurable again. There is a retry policy and a runtime policy
    let default_policy = gasket::retries::Policy {
        max_retries: 20,
        backoff_unit: Duration::from_secs(1),
        backoff_factor: 2,
        max_backoff: Duration::from_secs(60),
        dismissible: false,
    };

    Ok(spawn_stage(
        pipeline::Pipeline::bootstrap_gasket_stage(
            RefCell::new(config),
            Arc::new(args.console.clone().unwrap_or_default()),
        ),
        gasket::runtime::Policy {
            tick_timeout: None, // dumb to make this None and unconfigurable. see comment above default_policy (the retry policy)
            bootstrap_retry: default_policy.clone(),
            work_retry: default_policy.clone(),
            teardown_retry: default_policy.clone(),
        },
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
