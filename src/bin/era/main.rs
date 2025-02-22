use clap::Parser;
use era::{
    model::ConfigRoot,
    pipeline::{self, console, logs},
};
use gasket::{
    messaging::tokio::funnel_ports,
    runtime::{spawn_stage, Tether, TetherState},
};
use gasket_log::debug;
use std::{borrow::BorrowMut, cell::RefCell, process, sync::Arc};
use tokio::{
    self,
    signal::unix::{signal, SignalKind},
};

#[derive(Parser)]
#[clap(name = "Era")]
#[clap(bin_name = "era")]
#[clap(author, version, about, long_about = None)]
enum Era {
    Daemon(Args),
}

async fn is_dropped(tether: &Tether) -> bool {
    tether.check_state() == TetherState::Dropped
}

#[tokio::main]
async fn main() {
    let args = match Era::parse() {
        Era::Daemon(args) => args,
    };

    debug!("entrypoint");

    let mut config = ConfigRoot::new(&args.config)
        .map_err(|err| era::Error::ConfigError(format!("{:?}", err)))
        .unwrap();

    if let Some(display_console_argument) = &args.console {
        config.display.replace(console::Config {
            mode: display_console_argument.clone(),
        });
    }

    let mut logs_transformer = logs::Stage::new();

    let logs_bootstrapper = config
        .logging
        .take()
        .unwrap_or_default()
        .bootstrapper(vec![logs_transformer.input.borrow_mut()]);

    let ctx = config.take_some_to_make_context();

    let policy = config.gasket_policy();

    let mut pipeline_bootstrapper = config
        .pipeline
        .take()
        .unwrap_or_default()
        .bootstrapper(RefCell::new(config.clone()), ctx);

    let mut output_bootstrapper = config
        .display
        .take()
        .unwrap_or_default()
        .bootstrapper(Arc::clone(&pipeline_bootstrapper.stage.ctx));

    funnel_ports(
        vec![
            logs_transformer.output.borrow_mut(),
            pipeline_bootstrapper.stage.output.borrow_mut(),
        ],
        output_bootstrapper.borrow_input_port(),
        100,
    );

    let mut tethers: Vec<Tether> = vec![];

    tethers.push(output_bootstrapper.spawn_stage(policy.clone()));
    tethers.push(spawn_stage(logs_transformer, policy.clone()));
    tethers.push(logs_bootstrapper.spawn(policy.clone()));
    tethers.push(pipeline_bootstrapper.spawn_stage(policy));

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
        _ = sigterm.recv() => {
            println!("Received SIGTERM, shutting down...");
        }
    }

    // todo: keyboard interrupt
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
