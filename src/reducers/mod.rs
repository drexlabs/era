use std::sync::Arc;

use gasket::messaging::tokio::OutputPort;
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::model;
use crate::model::CRDTCommand;
use crate::pipeline::Context;

pub mod macros;

pub mod utils;

pub mod assets_balances;
pub mod assets_last_moved;
pub mod handles;
pub mod metadata;
pub mod parameters;
pub mod stake_to_pool;
pub mod utxo;

pub mod worker;

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    Utxo(utxo::Config),
    Parameters(parameters::Config),
    Metadata(metadata::Config),
    AssetsLastMoved(assets_last_moved::Config),
    AssetsBalances(assets_balances::Config),
    Handles(handles::Config),
    StakeToPool(stake_to_pool::Config),
}

impl Config {
    fn bootstrapper(self, ctx: Arc<Mutex<Context>>) -> Reducer {
        match self {
            Config::Utxo(c) => c.plugin(ctx),
            Config::Parameters(c) => c.plugin(ctx),
            Config::Metadata(c) => c.plugin(ctx),
            Config::AssetsLastMoved(c) => c.plugin(ctx),
            Config::AssetsBalances(c) => c.plugin(ctx),
            Config::Handles(c) => c.plugin(ctx),
            Config::StakeToPool(c) => c.plugin(),
        }
    }
}

#[derive(Clone)]
pub enum Reducer {
    Utxo(utxo::Reducer),
    Parameters(parameters::Reducer),
    Metadata(metadata::Reducer),
    AssetsLastMoved(assets_last_moved::Reducer),
    AssetsBalances(assets_balances::Reducer),
    Handle(handles::Reducer),
    StakeToPool(stake_to_pool::Reducer),
}

impl Reducer {
    pub async fn reduce_block<'b>(
        &mut self,
        block: Option<MultiEraBlock<'b>>,
        block_ctx: Option<model::BlockContext>,
        genesis_utxos: Option<Vec<GenesisUtxo>>,
        genesis_hash: Option<Hash<32>>,
        rollback: bool,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
    ) -> Result<(), gasket::framework::WorkerError> {
        match self {
            Reducer::Utxo(x) => {
                x.reduce(block, block_ctx, genesis_utxos, rollback, output.clone())
                    .await
            }
            Reducer::Parameters(x) => {
                x.reduce(block, genesis_utxos, genesis_hash, rollback, output)
                    .await
            }
            Reducer::Metadata(x) => x.reduce(block, rollback, output).await,
            Reducer::AssetsLastMoved(x) => x.reduce(block, output).await,
            Reducer::AssetsBalances(x) => {
                x.reduce(block, block_ctx, genesis_utxos, rollback, output)
                    .await
            }
            Reducer::Handle(x) => x.reduce(block, block_ctx, rollback, output).await,
            Reducer::StakeToPool(x) => x.reduce(block, rollback, output).await,
        }
    }
}
