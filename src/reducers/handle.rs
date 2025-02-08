use std::sync::Arc;

use futures::lock::Mutex;
use gasket::messaging::OutputPort;
use pallas::ledger::addresses::{Address, StakeAddress};
use serde::Deserialize;

use crate::model::{self};
use crate::model::{CRDTCommand, DecodedBlockAction};
use crate::pipeline::Context;

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub policy_id: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            key_prefix: Some(String::from("h")),
            policy_id: None,
        }
    }
}

pub struct Reducer {
    config: Config,
    ctx: Arc<Context>,
    output: OutputPort<CRDTCommand>,
}

impl Reducer {
    pub async fn reduce<'a>(
        &mut self,
        block: &'a DecodedBlockAction<'a>,
    ) -> Result<(), crate::Error> {
        match block {
            // todo: no genesis support
            DecodedBlockAction::Genesis(..) => Ok(()),

            action @ (DecodedBlockAction::Forward(b, c, _)
            | DecodedBlockAction::Rollback(b, c, _)) => {
                let is_rollback = action.is_rollback();
                for tx in b.txs().iter() {
                    // todo: real bad non-dry for rollback here
                    if is_rollback {
                        for input in tx.consumes() {
                            if let Ok(txo) = c.find_utxo(&input.output_ref()) {
                                let mut asset_names: Vec<String> = vec![];

                                for asset_list in txo.non_ada_assets() {
                                    for asset in asset_list.assets() {
                                        asset_names.push(hex::encode(asset.name()).to_string())
                                    }
                                }

                                if asset_names.is_empty() {
                                    return Ok(());
                                }

                                let address = &(txo.address().unwrap());
                                let soa = match address {
                                    Address::Shelley(s) => {
                                        match StakeAddress::try_from(s.clone()).ok() {
                                            Some(x) => x.to_bech32().unwrap_or(x.to_hex()),
                                            _ => address.to_bech32().unwrap_or(address.to_string()),
                                        }
                                    }

                                    Address::Byron(_) => {
                                        address.to_bech32().unwrap_or(address.to_string())
                                    }
                                    Address::Stake(stake) => {
                                        stake.to_bech32().unwrap_or(address.to_string())
                                    }
                                };

                                for asset_name in asset_names {
                                    self.output
                                        .send(
                                            model::CRDTCommand::any_write_wins(
                                                Some(
                                                    self.config
                                                        .key_prefix
                                                        .clone()
                                                        .unwrap_or_default()
                                                        .as_str(),
                                                ),
                                                format!("${}", asset_name),
                                                soa.to_string(),
                                            )
                                            .into(),
                                        )
                                        .await
                                        .map_err(crate::Error::reducer)?;

                                    self.output
                                        .send(
                                            model::CRDTCommand::any_write_wins(
                                                Some(
                                                    self.config
                                                        .key_prefix
                                                        .clone()
                                                        .unwrap_or_default()
                                                        .as_str(),
                                                ),
                                                soa.to_string(),
                                                format!("${}", asset_name),
                                            )
                                            .into(),
                                        )
                                        .await
                                        .map_err(crate::Error::reducer)?;
                                }
                            }
                        }
                    } else {
                        for (_, txo) in tx.produces() {
                            let mut asset_names: Vec<String> = vec![];

                            for asset_list in txo.non_ada_assets() {
                                for asset in asset_list.assets() {
                                    match String::from_utf8(asset.name().to_vec()) {
                                        Ok(asset_name) => asset_names.push(asset_name),
                                        Err(_) => log::warn!(
                                            "could not parse asset name {} not a valid ada handle?",
                                            ""
                                        ),
                                    };
                                }
                            }

                            if asset_names.is_empty() {
                                return Ok(());
                            }

                            let address = &(txo.address().unwrap());
                            let soa = match address {
                                Address::Shelley(s) => match StakeAddress::try_from(s.clone()).ok()
                                {
                                    Some(x) => x.to_bech32().unwrap_or(x.to_hex()),
                                    _ => address.to_bech32().unwrap_or(address.to_string()),
                                },

                                Address::Byron(_) => {
                                    address.to_bech32().unwrap_or(address.to_string())
                                }
                                Address::Stake(stake) => {
                                    stake.to_bech32().unwrap_or(address.to_string())
                                }
                            };

                            for asset_name in asset_names {
                                self.output
                                    .send(
                                        model::CRDTCommand::any_write_wins(
                                            Some(
                                                self.config
                                                    .key_prefix
                                                    .clone()
                                                    .unwrap_or_default()
                                                    .as_str(),
                                            ),
                                            format!("${}", asset_name),
                                            soa.to_string(),
                                        )
                                        .into(),
                                    )
                                    .await
                                    .map_err(crate::Error::reducer)?;

                                self.output
                                    .send(
                                        model::CRDTCommand::any_write_wins(
                                            Some(
                                                self.config
                                                    .key_prefix
                                                    .clone()
                                                    .unwrap_or_default()
                                                    .as_str(),
                                            ),
                                            soa.to_string(),
                                            format!("${}", asset_name),
                                        )
                                        .into(),
                                    )
                                    .await
                                    .map_err(crate::Error::reducer)?;
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
            config: Self {
                key_prefix: self.key_prefix,
                policy_id: self.policy_id,
            },
            output: Default::default(),
            ctx,
        };
        super::Reducer::Handle(reducer)
    }
}
