use gasket::messaging::OutputPort;
use pallas_bech32::cip14::AssetFingerprint;
use std::sync::Arc;

use pallas::crypto::hash::Hash;
use serde::Deserialize;

use crate::model::{CRDTCommand, DecodedBlockAction};
use crate::pipeline::Context;
use crate::{crosscut, model};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub policy_ids_hex: Option<Vec<String>>,
}

pub struct Reducer {
    config: Config,
    output: OutputPort<CRDTCommand>,
    ctx: Arc<Context>,
}

impl Reducer {
    async fn process_asset(
        &self,
        policy: &Hash<28>,
        fingerprint: &str,
        timestamp: &str,
    ) -> CRDTCommand {
        let key = match &self.config.key_prefix {
            Some(prefix) => prefix.to_string(),
            None => "policy".to_string(),
        };

        model::CRDTCommand::HashSetValue(
            format!("{}.{}", key, hex::encode(policy)),
            fingerprint.to_string(),
            timestamp.to_string().into(),
        )
    }

    // todo, there is no rollback support here
    pub async fn reduce<'a>(
        &mut self,
        block: &'a DecodedBlockAction<'a>,
    ) -> Result<(), crate::Error> {
        match block {
            DecodedBlockAction::Genesis(_) => Ok(()),

            DecodedBlockAction::Forward(b, ..) | DecodedBlockAction::Rollback(b, ..) => {
                //let is_rollback = action.is_rollback(); // todo: no rollback support
                let time_provider = crosscut::time::NaiveProvider::new(Arc::clone(&self.ctx)).await;

                for tx in b.txs().into_iter() {
                    for (_, outp) in tx.produces().iter() {
                        for asset_group in outp.value().assets() {
                            for asset in asset_group.assets() {
                                let asset_name = hex::encode(asset.name());
                                let policy_hex = hex::encode(asset.policy());

                                if let Ok(fingerprint) =
                                    AssetFingerprint::from_parts(&policy_hex, asset_name.as_str())
                                {
                                    let fingerprint_str = fingerprint.finger_print().unwrap();

                                    self.output.send(
                                        self.process_asset(
                                            &asset.policy(),
                                            &fingerprint_str,
                                            &time_provider.slot_to_wallclock(b.slot()).to_string(),
                                        )
                                        .await
                                        .into(),
                                    );
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Context>) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            output: Default::default(),
            ctx,
        };

        super::Reducer::AssetsLastMoved(reducer)
    }
}
