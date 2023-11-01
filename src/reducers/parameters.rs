use std::sync::Arc;

use crate::crosscut::epochs::block_epoch;
use crate::model::{CRDTCommand, Value};
use crate::pipeline::Context;
use crate::{model, prelude::*};
use gasket::messaging::tokio::OutputPort;
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;
use tokio::sync::Mutex;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
    ctx: Arc<Mutex<Context>>,
}

impl Reducer {
    pub async fn reduce<'b>(
        &mut self,
        block: Option<MultiEraBlock<'b>>,
        genesis_utxos: Option<Vec<GenesisUtxo>>,
        genesis_hash: Option<Hash<32>>,
        rollback: bool,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
    ) -> Result<(), gasket::framework::WorkerError> {
        if rollback {
            return Ok(());
        }

        let def_key_prefix = "last_block";

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}", prefix),
            None => format!("{}", def_key_prefix.to_string()),
        };

        match (block, genesis_utxos, genesis_hash) {
            (Some(block), None, None) => {
                let mut member_keys = vec![
                    "epoch_no".into(),
                    "height".into(),
                    "slot_no".into(),
                    "block_hash".into(),
                    "block_era".into(),
                    "transactions_count".into(),
                ];
                let mut member_values = vec![
                    Value::BigInt(block_epoch(self.ctx.lock().await.chain.clone(), &block).into()),
                    Value::BigInt(block.number().into()),
                    Value::BigInt(block.slot().into()),
                    block.hash().to_string().into(),
                    block.era().to_string().into(),
                    Value::String(block.tx_count().to_string().into()), // using a string here to move fast.. some other shits up with bigint for this .into()
                ];

                if let Some(first_tx_hash) = block.txs().first() {
                    member_keys.push("first_transaction_hash".into());
                    member_values.push(first_tx_hash.hash().to_string().into())
                }

                if let Some(last_tx_hash) = block.txs().last() {
                    member_keys.push("last_transaction_hash".into());
                    member_values.push(last_tx_hash.hash().to_string().into())
                }

                output
                    .lock()
                    .await
                    .send(gasket::messaging::Message::from(
                        model::CRDTCommand::HashSetMulti(key, member_keys, member_values),
                    ))
                    .await
                    .or_panic()?;

                Ok(())
            }

            (None, Some(genesis_utxos), Some(genesis_hash)) => {
                let member_keys = vec![
                    "epoch_no".into(),
                    "height".into(),
                    "slot_no".into(),
                    "block_hash".into(),
                    "block_era".into(),
                    "transactions_count".into(),
                ];
                let member_values = vec![
                    Value::BigInt(0),
                    Value::BigInt(0),
                    Value::BigInt(0),
                    genesis_hash.to_vec().into(),
                    "Byron".to_string().into(),
                    Value::String(genesis_utxos.len().to_string().into()), // using a string here to move fast.. some other shits up with bigint for this .into()
                ];

                output
                    .lock()
                    .await
                    .send(gasket::messaging::Message::from(
                        model::CRDTCommand::HashSetMulti(key, member_keys, member_values),
                    ))
                    .await
                    .map_err(|_| gasket::framework::WorkerError::Send)?;

                Ok(())
            }
            _ => Err(gasket::framework::WorkerError::Panic),
        }
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Mutex<Context>>) -> super::Reducer {
        let reducer = Reducer { config: self, ctx };

        super::Reducer::Parameters(reducer)
    }
}
