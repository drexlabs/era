pub mod skip;
pub mod sled;

use std::sync::Arc;

use serde::Deserialize;

use gasket::{
    messaging::{InputPort, OutputPort},
    runtime::{Policy, Tether},
};

use crate::{model, pipeline};

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    Skip(skip::Config),
    Sled(sled::Config),
}

impl Default for Config {
    fn default() -> Self {
        Self::Skip(skip::Config {})
    }
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<pipeline::Context>) -> Bootstrapper {
        match self {
            Config::Skip(w) => Bootstrapper::Skip(w.bootstrapper()),
            Config::Sled(w) => Bootstrapper::Sled(w.bootstrapper(ctx)),
        }
    }
}

pub enum Bootstrapper {
    Skip(skip::Stage),
    Sled(sled::Stage),
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::EnrichedBlockPayload> {
        match self {
            Bootstrapper::Skip(s) => &mut s.output,
            Bootstrapper::Sled(s) => &mut s.output,
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<model::RawBlockPayload> {
        match self {
            Bootstrapper::Skip(s) => &mut s.input,
            Bootstrapper::Sled(s) => &mut s.input,
        }
    }

    pub fn spawn_stage(self, policy: Policy) -> Tether {
        match self {
            Bootstrapper::Skip(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::Sled(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}
