pub mod redis;
pub mod skip;

use std::sync::Arc;

use gasket::{
    messaging::InputPort,
    runtime::{Policy, Tether},
};
use serde::Deserialize;

use crate::{crosscut::PointArg, model, pipeline::Context};

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    Skip(skip::Config),
    Redis(redis::Config),
}

pub enum Stage {
    Skip(skip::Stage),
    Redis(redis::Stage),
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<Context>) -> Bootstrapper {
        match self {
            Config::Skip(c) => Bootstrapper::Skip(c.bootstrapper()),
            Config::Redis(c) => Bootstrapper::Redis(c.bootstrapper(ctx)),
        }
    }
}

#[derive(Clone)]
pub enum Cursor {
    Skip,
    Redis(redis::Cursor),
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<PointArg>, crate::Error> {
        match self {
            Cursor::Skip => Ok(None),
            Cursor::Redis(x) => x.last_point(),
        }
    }
}

pub enum Bootstrapper {
    Skip(skip::Stage),
    Redis(redis::Stage),
}

impl Bootstrapper {
    pub fn cursor(&mut self) -> Cursor {
        match self {
            Bootstrapper::Skip(_) => Cursor::Skip,
            Bootstrapper::Redis(x) => Cursor::Redis(x.cursor.clone()),
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<model::CRDTCommand> {
        match self {
            Bootstrapper::Skip(s) => &mut s.input,
            Bootstrapper::Redis(s) => &mut s.input,
        }
    }

    pub fn spawn_stage(self, policy: Policy) -> Tether {
        match self {
            Bootstrapper::Skip(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::Redis(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}
