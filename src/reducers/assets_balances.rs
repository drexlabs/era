use crate::model::{CRDTCommand, DecodedBlockAction};
use crate::pipeline::Context;
use crate::{crosscut, model, prelude::*};
use gasket::messaging::OutputPort;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::traverse::{
    MultiEraAsset, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets,
};
use pallas_bech32::cip14::AssetFingerprint;
use serde::{Deserialize, Serialize};

use pallas::ledger::addresses::{Address, StakeAddress};
use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;

const ERROR_MSG: &str = "could not send gasket message from assets_balances reducer";

#[derive(Serialize, Deserialize)]
struct MultiAssetSingleAgg {
    #[serde(rename = "policyId")]
    policy_id: String,
    #[serde(rename = "assetName")]
    asset_name: String,
    quantity: i64,
    fingerprint: String,
}

#[derive(Deserialize)]
pub enum Projection {
    Cbor,
    Json,
}

#[derive(Serialize, Deserialize)]
struct PreviousOwnerAgg {
    address: String,
    transferred_out: i64,
}

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
}

pub struct Reducer {
    config: Config,
    ctx: Arc<Context>,
    output: OutputPort<CRDTCommand>,
}

impl Reducer {
    fn stake_or_address_from_address(&self, address: &Address) -> String {
        match address {
            Address::Shelley(s) => match StakeAddress::try_from(s.clone()).ok() {
                Some(x) => x.to_bech32().unwrap_or(x.to_hex()),
                _ => address.to_bech32().unwrap_or(address.to_string()),
            },

            Address::Byron(_) => address.to_bech32().unwrap_or(address.to_string()),
            Address::Stake(stake) => stake.to_bech32().unwrap_or(address.to_string()),
        }
    }

    fn calculate_address_asset_balance_offsets(
        &self,
        address: &str,
        lovelace: i64,
        assets_group: &Vec<MultiEraPolicyAssets>,
        spending: bool,
    ) -> (
        HashMap<String, HashMap<String, i64>>,
        HashMap<String, HashMap<String, Vec<(String, i64)>>>,
    ) {
        let mut fingerprint_tallies: HashMap<String, HashMap<String, i64>> = HashMap::new();
        let mut policy_asset_owners: HashMap<String, HashMap<String, Vec<(String, i64)>>> =
            HashMap::new();

        for assets_container in assets_group {
            for asset in assets_container.assets() {
                if let MultiEraAsset::AlonzoCompatibleOutput(policy_id, asset_name, quantity) =
                    asset
                {
                    let asset_name = hex::encode(asset_name.to_vec());
                    let encoded_policy_id = hex::encode(policy_id);

                    if let Ok(fingerprint) =
                        AssetFingerprint::from_parts(&encoded_policy_id, &asset_name)
                    {
                        if let Ok(fingerprint) = fingerprint.finger_print() {
                            let adjusted_quantity: i64 = match spending {
                                true => -(quantity as i64),
                                false => quantity as i64,
                            };

                            *fingerprint_tallies
                                .entry(address.clone().to_string())
                                .or_insert(HashMap::new())
                                .entry(fingerprint.clone())
                                .or_insert(0_i64) += adjusted_quantity;

                            policy_asset_owners
                                .entry(hex::encode(policy_id))
                                .or_insert(HashMap::new())
                                .entry(fingerprint)
                                .or_insert(Vec::new())
                                .push((address.to_string(), adjusted_quantity));
                        }
                    }
                };
            }
        }

        *fingerprint_tallies
            .entry(address.to_string())
            .or_insert(HashMap::new())
            .entry("lovelace".to_string())
            .or_insert(0) += lovelace;

        (fingerprint_tallies, policy_asset_owners)
    }

    async fn process_asset_movement<'a>(
        &mut self,
        soa: &str,
        lovelace: u64,
        assets: &Vec<MultiEraPolicyAssets<'a>>,
        spending: bool,
        time: u64,
    ) -> Result<(), Error> {
        let adjusted_lovelace = match spending {
            true => -(lovelace as i64),
            false => lovelace as i64,
        };

        let (fingerprint_tallies, policy_asset_owners) = self
            .calculate_address_asset_balance_offsets(&soa, adjusted_lovelace, &assets, spending);

        let prefix = self.config.key_prefix.clone().unwrap_or("w".to_string());

        if !fingerprint_tallies.is_empty() {
            for (soa, quantity_map) in fingerprint_tallies.clone() {
                for (fingerprint, quantity) in quantity_map {
                    if !fingerprint.is_empty() {
                        self.output
                            .send(
                                model::CRDTCommand::HashCounter(
                                    format!("{}.{}", prefix, soa),
                                    fingerprint.to_owned(),
                                    quantity,
                                )
                                .into(),
                            )
                            .await
                            .map_err(|e| Error::reducer(e))?;
                    }
                }

                self.output
                    .send(
                        model::CRDTCommand::AnyWriteWins(
                            format!("{}.l.{}", prefix, soa),
                            time.to_string().into(),
                        )
                        .into(),
                    )
                    .await
                    .map_err(|e| Error::reducer(e))?;
            }
        }

        if !policy_asset_owners.is_empty() {
            for (policy_id, asset_to_owner) in policy_asset_owners {
                if spending {
                    // may have lost some stuff in this reducer around this area
                    self.output
                        .send(
                            model::CRDTCommand::AnyWriteWins(
                                format!("{}.lp.{}", prefix, policy_id),
                                time.to_string().into(),
                            )
                            .into(),
                        )
                        .await
                        .map_err(|e| Error::reducer(e))?;
                }

                for (fingerprint, soas) in asset_to_owner {
                    for (soa, quantity) in soas {
                        if !soa.is_empty() {
                            if quantity != 0 {
                                self.output
                                    .send(
                                        model::CRDTCommand::HashCounter(
                                            format!("{}.owned.{}", prefix, fingerprint),
                                            soa.clone(),
                                            quantity,
                                        )
                                        .into(),
                                    )
                                    .await
                                    .map_err(|e| Error::reducer(e))?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // todo cleanup None case and need for Some wrapper in the internal match
    async fn process_received<'a>(
        &mut self,
        meo: Option<&MultiEraOutput<'a>>,
        genesis_utxo: Option<&GenesisUtxo>,
        rollback: bool,
        timeslot: u64,
    ) -> Result<(), Error> {
        match (meo, genesis_utxo) {
            (Some(meo), None) => {
                let received_to_soa = self.stake_or_address_from_address(&meo.address().unwrap());

                self.process_asset_movement(
                    received_to_soa.as_str(),
                    meo.value().coin(),
                    &meo.value().assets(),
                    rollback,
                    timeslot,
                )
                .await
            }

            (None, Some(genesis_utxo)) => {
                self.process_asset_movement(
                    hex::encode(genesis_utxo.1.to_vec()).as_str(),
                    genesis_utxo.2,
                    &Vec::default(),
                    rollback,
                    timeslot,
                )
                .await
            }

            _ => Ok(()),
        }
    }

    async fn process_spent<'a>(
        &mut self,
        mei: &MultiEraInput<'a>,
        ctx: &model::BlockContext,
        rollback: bool,
        timeslot: u64,
    ) -> Result<(), Error> {
        match ctx.find_utxo(&mei.output_ref()) {
            Ok(spent_output) => {
                let spent_from_soa =
                    self.stake_or_address_from_address(&spent_output.address().unwrap());

                let val = spent_output.value().clone();

                self.process_asset_movement(
                    &spent_from_soa,
                    spent_output.value().coin(),
                    &val.assets(),
                    !rollback,
                    timeslot,
                )
                .await
            }

            Err(_) => Ok(()), // todo: disgusting
        }
    }

    // todo refactor all these reducers
    pub async fn reduce<'a>(&mut self, block: &'a DecodedBlockAction<'a>) -> Result<(), Error> {
        match block {
            DecodedBlockAction::Genesis(genesis_utxo) => {
                for utxo in genesis_utxo {
                    self.process_received(None, Some(&utxo), false, 0).await?;
                }

                Ok(())
            }

            action @ (DecodedBlockAction::Forward(b, c, p)
            | DecodedBlockAction::Rollback(b, c, p)) => {
                let is_rollback = action.is_rollback();

                let slot = b.slot();
                let time_provider = crosscut::time::NaiveProvider::new(Arc::clone(&self.ctx)).await;

                for tx in b.txs() {
                    for consumes in tx.consumes().iter() {
                        self.process_spent(
                            consumes,
                            c,
                            is_rollback,
                            time_provider.slot_to_wallclock(slot),
                        )
                        .await?;
                    }

                    for (_, utxo_produced) in tx.produces().iter() {
                        self.process_received(
                            Some(&utxo_produced),
                            None,
                            is_rollback,
                            time_provider.slot_to_wallclock(slot),
                        )
                        .await?;
                    }
                }

                Ok(())
            }
        }
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Context>) -> super::Reducer {
        super::Reducer::AssetsBalances(Reducer {
            config: self,
            output: Default::default(),
            ctx,
        })
    }
}
