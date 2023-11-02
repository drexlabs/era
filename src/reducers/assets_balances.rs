use crate::model::CRDTCommand;
use crate::pipeline::Context;
use crate::{crosscut, model, prelude::*};
use gasket::framework::WorkerError;
use gasket::messaging::tokio::OutputPort;
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::ledger::traverse::{
    MultiEraAsset, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets,
};
use serde::{Deserialize, Serialize};

use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::ledger::addresses::{Address, StakeAddress};
use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
struct MultiAssetSingleAgg {
    #[serde(rename = "policyId")]
    policy_id: String,
    #[serde(rename = "assetName")]
    asset_name: String,
    quantity: i64,
    fingerprint: String,
}

#[derive(Deserialize, Copy, Clone)]
pub enum Projection {
    Cbor,
    Json,
}

#[derive(Serialize, Deserialize)]
struct PreviousOwnerAgg {
    address: String,
    transferred_out: i64,
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
}

fn asset_fingerprint(data_list: [&str; 2]) -> Result<String, bech32::Error> {
    let combined_parts = data_list.join("");
    let raw = hex::decode(combined_parts).unwrap();
    let mut hasher = Blake2bVar::new(20).unwrap();
    hasher.update(&raw);
    let mut buf = [0u8; 20];
    hasher.finalize_variable(&mut buf).unwrap();
    let base32_combined = buf.to_base32();
    bech32::encode("asset", base32_combined, Variant::Bech32)
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
    ctx: Arc<Mutex<Context>>,
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
        address: &String,
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

                    if let Ok(fingerprint) = asset_fingerprint([&encoded_policy_id, &asset_name]) {
                        if !fingerprint.is_empty() {
                            let adjusted_quantity: i64 = match spending {
                                true => -(quantity as i64),
                                false => quantity as i64,
                            };

                            *fingerprint_tallies
                                .entry(address.clone())
                                .or_insert(HashMap::new())
                                .entry(fingerprint.clone())
                                .or_insert(0_i64) += adjusted_quantity;

                            policy_asset_owners
                                .entry(hex::encode(policy_id))
                                .or_insert(HashMap::new())
                                .entry(fingerprint)
                                .or_insert(Vec::new())
                                .push((address.clone(), adjusted_quantity));
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
        &self,
        output: Arc<Mutex<OutputPort<model::CRDTCommand>>>,
        soa: &String,
        lovelace: u64,
        assets: &'a Vec<MultiEraPolicyAssets<'a>>,
        spending: bool,
        time: u64,
    ) -> Result<(), Error> {
        let adjusted_lovelace = match spending {
            true => -(lovelace as i64),
            false => lovelace as i64,
        };

        let (fingerprint_tallies, policy_asset_owners) =
            self.calculate_address_asset_balance_offsets(soa, adjusted_lovelace, assets, spending);

        let prefix = self.config.key_prefix.clone().unwrap_or("w".to_string());

        if !fingerprint_tallies.is_empty() {
            for (soa, quantity_map) in fingerprint_tallies.clone() {
                for (fingerprint, quantity) in quantity_map {
                    if !fingerprint.is_empty() {
                        output
                            .lock()
                            .await
                            .send(
                                model::CRDTCommand::HashCounter(
                                    format!("{}.{}", prefix, soa),
                                    fingerprint.to_owned(),
                                    quantity,
                                )
                                .into(),
                            )
                            .await
                            .map_err(|_| Error::gasket(WorkerError::Send))?;
                    }
                }

                output
                    .lock()
                    .await
                    .send(
                        model::CRDTCommand::AnyWriteWins(
                            format!("{}.l.{}", prefix, soa),
                            time.to_string().into(),
                        )
                        .into(),
                    )
                    .await
                    .map_err(|_| Error::gasket(WorkerError::Send))?;
            }
        }

        if !policy_asset_owners.is_empty() {
            for (policy_id, asset_to_owner) in policy_asset_owners {
                if spending {
                    // may have lost some stuff in this reducer around this area
                    output
                        .lock()
                        .await
                        .send(
                            model::CRDTCommand::AnyWriteWins(
                                format!("{}.lp.{}", prefix, policy_id),
                                time.to_string().into(),
                            )
                            .into(),
                        )
                        .await
                        .map_err(|_| Error::gasket(WorkerError::Send))?;
                }

                for (fingerprint, soas) in asset_to_owner {
                    for (soa, quantity) in soas {
                        if !soa.is_empty() {
                            if quantity != 0 {
                                output
                                    .lock()
                                    .await
                                    .send(
                                        model::CRDTCommand::HashCounter(
                                            format!("{}.owned.{}", prefix, fingerprint),
                                            soa.clone(),
                                            quantity,
                                        )
                                        .into(),
                                    )
                                    .await
                                    .map_err(|_| Error::gasket(WorkerError::Send))?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_received<'a>(
        &self,
        output: Arc<Mutex<OutputPort<model::CRDTCommand>>>,
        meo: Option<MultiEraOutput<'a>>,
        genesis_utxo: Option<GenesisUtxo>,
        rollback: bool,
        timeslot: u64,
    ) -> Result<(), Error> {
        match (meo, genesis_utxo) {
            (Some(meo), None) => {
                let received_to_soa = self.stake_or_address_from_address(&meo.address().unwrap());

                self.process_asset_movement(
                    output,
                    &received_to_soa,
                    meo.lovelace_amount(),
                    &meo.non_ada_assets(),
                    rollback,
                    timeslot,
                )
                .await
            }

            (None, Some(genesis_utxo)) => {
                self.process_asset_movement(
                    output,
                    &hex::encode(genesis_utxo.1.to_vec()),
                    genesis_utxo.2,
                    &Vec::default(),
                    false,
                    timeslot,
                )
                .await
            }

            _ => Err(Error::gasket(WorkerError::Panic)),
        }
    }

    async fn process_spent<'a>(
        &self,
        output: Arc<Mutex<OutputPort<model::CRDTCommand>>>,
        mei: &'a MultiEraInput<'a>,
        ctx: &model::BlockContext,
        rollback: bool,
        timeslot: u64,
    ) -> Result<(), Error> {
        match ctx.find_utxo(&mei.output_ref()) {
            Ok(spent_output) => {
                let spent_from_soa =
                    self.stake_or_address_from_address(&spent_output.address().unwrap());

                self.process_asset_movement(
                    output,
                    &spent_from_soa,
                    spent_output.lovelace_amount(),
                    &spent_output.non_ada_assets(),
                    !rollback,
                    timeslot,
                )
                .await
            }

            Err(_) => match ctx.find_genesis_utxo(&mei.output_ref()) {
                Ok(genesis_utxo) => {
                    self.process_asset_movement(
                        output,
                        &hex::encode(&genesis_utxo.1.to_vec()),
                        genesis_utxo.2,
                        &Default::default(),
                        !rollback,
                        timeslot,
                    )
                    .await
                }

                Err(e) => Err(e),
            },
        }
    }

    pub async fn reduce<'b>(
        &mut self,
        block: Option<MultiEraBlock<'b>>,
        block_ctx: Option<model::BlockContext>,
        genesis_utxos: Option<Vec<GenesisUtxo>>,
        rollback: bool,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
    ) -> Result<(), gasket::framework::WorkerError> {
        match (block, block_ctx, genesis_utxos) {
            (Some(block), Some(block_ctx), _) => {
                let slot = block.slot();
                let time_provider = crosscut::time::NaiveProvider::new(self.ctx.clone()).await;

                for tx in block.txs() {
                    for consumes in tx.consumes().iter() {
                        self.process_spent(
                            output.clone(),
                            consumes,
                            &block_ctx,
                            rollback,
                            time_provider.slot_to_wallclock(slot),
                        )
                        .await
                        .or_panic()?;
                    }

                    for (_, utxo_produced) in tx.produces().iter() {
                        self.process_received(
                            output.clone(),
                            Some(utxo_produced.clone()),
                            None,
                            rollback,
                            time_provider.slot_to_wallclock(slot),
                        )
                        .await
                        .or_panic()?;
                    }
                }

                Ok(())
            }

            (None, None, Some(genesis_utxos)) => {
                for utxo in genesis_utxos {
                    self.process_received(output.clone(), None, Some(utxo), false, 0)
                        .await
                        .or_panic()?;
                }

                Ok(())
            }

            _ => Err(gasket::framework::WorkerError::Panic),
        }
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Mutex<Context>>) -> super::Reducer {
        let reducer = Reducer { config: self, ctx };

        super::Reducer::AssetsBalances(reducer)
    }
}
