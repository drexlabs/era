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

    let result = match args {
        Era::Daemon(x) => daemon::run(&x),
    };

    if let Err(err) = &result {
        eprintln!("ERROR: {:#?}", err);
        process::exit(1);
    } else {
        // tood make loop cancelable. prob move to daemon, tbh
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        process::exit(0);
    }
}
