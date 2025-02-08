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
            Ok(tether) => {
                tether.join_stage();
            }

            Err(err) => {
                eprintln!("ERROR: {:#?}", err);
                process::exit(1);
            }
        },
    };

    // todo: keyboard interrupt
}
