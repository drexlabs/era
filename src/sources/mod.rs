use std::sync::Arc;

use crate::{model, pipeline, storage::Cursor};
use gasket::{messaging::tokio::OutputPort, runtime::Tether};
use serde::Deserialize;
use tokio::sync::Mutex;

pub mod utils;

pub mod n2n;
pub mod utxorpc;

#[cfg(target_family = "unix")]
pub mod n2c;

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    N2C(n2c::Config),
    N2N(n2n::Config),
    UTXORPC(utxorpc::Config),
}

impl Config {
    pub fn bootstrapper(
        self,
        ctx: Arc<Mutex<pipeline::Context>>,
        cursor: Cursor,
    ) -> Option<Bootstrapper> {
        match self {
            Config::N2N(c) => Some(Bootstrapper::N2N(c.bootstrapper(ctx, cursor))),
            Config::UTXORPC(c) => Some(Bootstrapper::UTXORPC(c.bootstrapper(ctx, cursor))),
            Config::N2C(c) => Some(Bootstrapper::N2C(c.bootstrapper(ctx, cursor))),
        }
    }
}

pub enum Bootstrapper {
    N2C(n2c::chainsync::Stage),
    N2N(n2n::chainsync::Stage),
    UTXORPC(utxorpc::Stage),
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::RawBlockPayload> {
        match self {
            Bootstrapper::N2C(s) => &mut s.output,
            Bootstrapper::N2N(s) => &mut s.output,
            Bootstrapper::UTXORPC(s) => &mut s.output,
        }
    }

    pub fn spawn_stage(self, pipeline: &pipeline::Pipeline) -> Tether {
        match self {
            Bootstrapper::N2C(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::N2N(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::UTXORPC(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
        }
    }
}
