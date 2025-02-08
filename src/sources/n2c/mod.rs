pub mod chainsync;

use std::sync::Arc;

use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{pipeline, storage::Cursor};

#[derive(Clone, Debug)]
pub enum ChainSyncInternalPayload {
    RollForward(Point),
    RollBack(Point),
}

impl ChainSyncInternalPayload {
    pub fn roll_forward(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(point),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub path: String,
    pub min_depth: Option<usize>,
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<pipeline::Context>, cursor: Cursor) -> chainsync::Stage {
        chainsync::Stage {
            config: self,
            output: Default::default(),
            chain_tip: Default::default(),
            ctx,
            cursor,
        }
    }
}
