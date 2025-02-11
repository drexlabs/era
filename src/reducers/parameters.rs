use std::sync::Arc;

use crate::crosscut::epochs::block_epoch;
use crate::model::{CRDTCommand, DecodedBlockAction, Value};
use crate::pipeline::Context;
use crate::{model, Error};
use gasket::messaging::OutputPort;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
    ctx: Arc<Context>,
    pub output: OutputPort<CRDTCommand>,
}

impl Reducer {
    pub async fn reduce<'a>(
        &mut self,
        block: &'a DecodedBlockAction<'a>,
    ) -> Result<(), crate::Error> {
        // todo: no rollback support
        if let DecodedBlockAction::Rollback(..) = block {
            return Ok(());
        }

        let def_key_prefix = "last_block";

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}", prefix),
            None => format!("{}", def_key_prefix.to_string()),
        };

        match block {
            // todo: no genesis support
            DecodedBlockAction::Genesis(..) => Ok(()),
            // todo: no genesis support
            DecodedBlockAction::Rollback(..) => Ok(()),

            DecodedBlockAction::Forward(block, _, _) => {
                let mut member_keys = vec![
                    "epoch_no".into(),
                    "height".into(),
                    "slot_no".into(),
                    "block_hash".into(),
                    "block_era".into(),
                    "transactions_count".into(),
                ];

                let mut member_values = vec![
                    Value::BigInt(block_epoch(self.ctx.chain.clone(), &block).into()),
                    Value::BigInt(block.number().into()),
                    Value::BigInt(block.slot().into()),
                    block.hash().to_string().into(),
                    block.era().to_string().into(),
                    Value::String(block.tx_count().to_string().into()), // using a string here to move fast.. some other shits up with bigint for this .into()
                ];

                if let Some(first_tx_hash) = block.txs().first() {
                    member_keys.push("first_transaction_hash".into());
                    member_values.push(first_tx_hash.hash().to_string().into());
                }

                if let Some(last_tx_hash) = block.txs().last() {
                    member_keys.push("last_transaction_hash".into());
                    member_values.push(last_tx_hash.hash().to_string().into());
                }

                self.output
                    .send(model::CRDTCommand::HashSetMulti(key, member_keys, member_values).into())
                    .await
                    .map_err(Error::reducer)
            }
        }
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Context>) -> super::Reducer {
        super::Reducer::Parameters(Reducer {
            config: self,
            output: Default::default(),
            ctx,
        })
    }
}
