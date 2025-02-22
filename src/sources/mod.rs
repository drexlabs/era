use std::sync::Arc;

use crate::{model, pipeline::Context, storage::Cursor};
use gasket::{
    messaging::OutputPort,
    runtime::{Policy, Tether},
};
use serde::Deserialize;

pub mod utils;

pub mod n2n;

#[cfg(target_family = "unix")]
pub mod n2c;

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    N2C(n2c::Config),
    N2N(n2n::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<Context>, cursor: Cursor) -> Bootstrapper {
        match self {
            Config::N2N(c) => Bootstrapper::N2N(c.bootstrapper(ctx, cursor)),
            Config::N2C(c) => Bootstrapper::N2C(c.bootstrapper(ctx, cursor)),
        }
    }
}

pub enum Bootstrapper {
    N2C(n2c::chainsync::Stage),
    N2N(n2n::chainsync::Stage),
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::RawBlockPayload> {
        match self {
            Bootstrapper::N2C(s) => &mut s.output,
            Bootstrapper::N2N(s) => &mut s.output,
        }
    }

    pub fn spawn_stage(self, policy: Policy) -> Tether {
        match self {
            Bootstrapper::N2C(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::N2N(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}
