use crate::{
    model::{CRDTCommand, DecodedBlockAction},
    pipeline::Context,
};
use gasket::messaging::OutputPort;
use serde::Deserialize;
use std::sync::Arc;

pub mod assets_balances;
pub mod assets_last_moved;
pub mod handle;
pub mod macros;
pub mod metadata;
pub mod parameters;
pub mod stake_to_pool;
pub mod utils;
pub mod utxo;
pub mod worker;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    Utxo(utxo::Config),
    Parameters(parameters::Config),
    Metadata(metadata::Config),
    AssetsLastMoved(assets_last_moved::Config),
    AssetsBalances(assets_balances::Config),
    Handle(handle::Config),
    StakeToPool(stake_to_pool::Config),
}

impl Config {
    fn bootstrapper(self, ctx: Arc<Context>) -> Reducer {
        match self {
            Config::Utxo(c) => c.plugin(),
            Config::Parameters(c) => c.plugin(ctx),
            Config::Metadata(c) => c.plugin(ctx),
            Config::AssetsLastMoved(c) => c.plugin(ctx),
            Config::AssetsBalances(c) => c.plugin(ctx),
            Config::Handle(c) => c.plugin(),
            Config::StakeToPool(c) => c.plugin(),
        }
    }
}

pub enum Reducer {
    Utxo(utxo::Reducer),
    Parameters(parameters::Reducer),
    Metadata(metadata::Reducer),
    AssetsLastMoved(assets_last_moved::Reducer),
    AssetsBalances(assets_balances::Reducer),
    Handle(handle::Reducer),
    StakeToPool(stake_to_pool::Reducer),
}

impl Reducer {
    pub async fn reduce_block<'a>(
        &mut self,
        block: Arc<DecodedBlockAction<'a>>,
    ) -> Result<(), crate::Error> {
        match self {
            Reducer::Utxo(x) => x.reduce(block.as_ref()).await,
            Reducer::Parameters(x) => x.reduce(block.as_ref()).await,
            Reducer::Metadata(x) => x.reduce(block.as_ref()).await,
            Reducer::AssetsLastMoved(x) => x.reduce(block.as_ref()).await,
            Reducer::AssetsBalances(x) => x.reduce(block.as_ref()).await,
            Reducer::Handle(x) => x.reduce(block.as_ref()).await,
            Reducer::StakeToPool(x) => x.reduce(block.as_ref()).await,
        }
    }

    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<CRDTCommand> {
        match self {
            Reducer::Utxo(x) => &mut x.output,
            Reducer::Parameters(x) => &mut x.output,
            Reducer::Metadata(x) => &mut x.output,
            Reducer::AssetsLastMoved(x) => &mut x.output,
            Reducer::AssetsBalances(x) => &mut x.output,
            Reducer::Handle(x) => &mut x.output,
            Reducer::StakeToPool(x) => &mut x.output,
        }
    }
}
