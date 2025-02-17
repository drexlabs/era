use clap::Parser;
use std::process;

mod daemon;

#[derive(Parser)]
#[clap(name = "Era")]
#[clap(bin_name = "era")]
#[clap(author, version, about, long_about = None)]
enum Era {
    Daemon(daemon::Args),
}

#[tokio::main]
async fn main() {
    let args = Era::parse();

    match args {
        Era::Daemon(x) => match daemon::run(&x) {
            Ok(pipeline_tether) => {
                pipeline_tether.join_stage();
            }

            Err(_) => {
                process::exit(1);
            }
        },
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(100000)).await; // todo: remove, obv

    // todo: keyboard interrupt
}
