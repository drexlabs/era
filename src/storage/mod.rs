pub mod redis;
pub mod skip;

#[cfg(feature = "elastic")]
pub mod elastic;

use std::sync::Arc;

use gasket::{messaging::tokio::InputPort, runtime::Tether};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    crosscut::PointArg,
    model,
    pipeline::{self, Context},
};

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    Skip(skip::Config),
    Redis(redis::Config),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Config),
}

pub enum Stage {
    Skip(skip::Stage),
    Redis(redis::Stage),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Stage),
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<Mutex<Context>>) -> Option<Bootstrapper> {
        match self {
            Config::Skip(c) => Some(Bootstrapper::Skip(c.bootstrapper(ctx))),
            Config::Redis(c) => Some(Bootstrapper::Redis(c.bootstrapper(ctx))),

            #[cfg(feature = "elastic")]
            Config::Elastic(c) => Some(Bootstrapper::Elastic(c.bootstrapper(ctx))),
        }
    }
}

#[derive(Clone)]
pub enum Cursor {
    Skip,
    Redis(redis::Cursor),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Cursor),
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<PointArg>, crate::Error> {
        match self {
            Cursor::Skip => Ok(None),
            Cursor::Redis(x) => x.last_point(),

            #[cfg(feature = "elastic")]
            Cursor::Elastic(x) => x.last_point(),
        }
    }
}

pub enum Bootstrapper {
    Skip(skip::Stage),
    Redis(redis::Stage),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Stage),
}

impl Bootstrapper {
    pub fn build_cursor(&mut self) -> Cursor {
        match self {
            Bootstrapper::Skip(_) => Cursor::Skip,
            Bootstrapper::Redis(x) => Cursor::Redis(x.cursor.clone()),

            #[cfg(feature = "elastic")]
            Bootstrapper::Elastic(x) => Cursor::Elastic(x.cursor.clone()),
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<model::CRDTCommand> {
        match self {
            Bootstrapper::Skip(s) => &mut s.input,
            Bootstrapper::Redis(s) => &mut s.input,
            Bootstrapper::Elastic(s) => &mut s.input,
        }
    }

    pub fn spawn_stage(self, pipeline: &pipeline::Pipeline) -> Tether {
        match self {
            Bootstrapper::Skip(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::Redis(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::Elastic(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
        }
    }
}
