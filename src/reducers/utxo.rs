use gasket::messaging::OutputPort;
use log::warn;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::traverse::{MultiEraAsset, MultiEraOutput, OutputRef};
use pallas_bech32::cip14::AssetFingerprint;
use serde::{Deserialize, Serialize};

use crate::model::{CRDTCommand, DecodedBlockAction};
use crate::{model, prelude::*};

const ERROR_MSG: &str = "could not send gasket message from utxo reducer";

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub coin_key_prefix: Option<String>,
    pub datum_key_prefix: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            key_prefix: Some("tx".to_string()),
            coin_key_prefix: Some("c".to_string()),
            datum_key_prefix: Some("d".to_string()),
        }
    }
}

pub struct Reducer {
    pub config: Config,
    pub output: OutputPort<CRDTCommand>,
}

// hash and index are stored in the key
#[derive(Deserialize, Serialize)]
pub struct DropKingMultiAssetUTXO {
    policy_id: String,
    name: String,
    quantity: u64,
    tx_address: String,
    fingerprint: String,
}

// fn asset_fingerprint(_data_list: [&str; 2]) -> Result<String, Error> {
//     Ok("".to_string())
// }

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

    async fn tx_state(&mut self, soa: &str, tx_str: &str, should_exist: bool) -> Result<(), Error> {
        let msg = match should_exist {
            true => CRDTCommand::set_add(
                self.config.key_prefix.clone().as_deref(),
                &soa,
                tx_str.to_string(),
            ),

            false => CRDTCommand::set_remove(
                self.config.key_prefix.clone().as_deref(),
                &soa,
                tx_str.to_string(),
            ),
        };

        self.output
            .send(msg.into())
            .await
            .or_retry()
            .map_err(|a| Error::message(ERROR_MSG))
    }

    async fn coin_state(
        &mut self,
        address: &str,
        tx_str: &str,
        lovelace_amt: &str,
        should_exist: bool,
    ) -> Result<(), Error> {
        self.output
            .send(
                match should_exist {
                    true => CRDTCommand::set_add(
                        self.config.coin_key_prefix.clone().as_deref(),
                        tx_str,
                        format!("{}/{}", address, lovelace_amt),
                    ),

                    false => CRDTCommand::set_remove(
                        self.config.coin_key_prefix.clone().as_deref(),
                        tx_str,
                        format!("{}/{}", address, lovelace_amt),
                    ),
                }
                .into(),
            )
            .await
            .or_retry()
            .map_err(|_| Error::message(ERROR_MSG))
    }

    async fn token_state(
        &mut self,
        address: &str,
        tx_str: &str,
        policy_id: &str,
        fingerprint: &str,
        quantity: &str,
        should_exist: bool,
    ) -> Result<(), Error> {
        self.output
            .send(
                match should_exist {
                    true => CRDTCommand::set_add(
                        self.config.key_prefix.clone().as_deref(),
                        tx_str,
                        format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                    ),

                    _ => CRDTCommand::set_remove(
                        self.config.key_prefix.clone().as_deref(),
                        tx_str,
                        format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                    ),
                }
                .into(),
            )
            .await
            .or_retry()
            .map_err(|_| Error::message(ERROR_MSG))
    }

    async fn datum_state<'b>(
        &mut self,
        address: &str,
        tx_str: &str,
        utxo: &'b MultiEraOutput<'b>,
        should_exist: bool,
    ) -> Result<(), Error> {
        if let Some(output) = match utxo.datum() {
            Some(datum) => match datum {
                pallas::ledger::primitives::babbage::PseudoDatumOption::Data(datum) => {
                    let raw_cbor_bytes: &[u8] = datum.0.raw_cbor();

                    Some(match should_exist {
                        true => CRDTCommand::set_add(
                            self.config.datum_key_prefix.clone().as_deref(),
                            tx_str,
                            format!("{}/{}", address, hex::encode(raw_cbor_bytes)),
                        ),
                        false => CRDTCommand::set_remove(
                            self.config.datum_key_prefix.clone().as_deref(),
                            tx_str,
                            format!("{}/{}", address, hex::encode(raw_cbor_bytes)),
                        ),
                    })
                }

                _ => None,
            },
            None => None,
        } {
            return self
                .output
                .send(output.into())
                .await
                .or_retry()
                .map_err(|_| Error::message(ERROR_MSG));
        } else {
            return Ok(());
        }
    }

    async fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        rollback: bool,
    ) -> Result<(), Error> {
        let utxo = ctx.find_utxo(input).unwrap();

        let address = utxo.address().map(|x| x.to_string()).unwrap();

        let lovelace_amt = utxo.value().coin();
        let cloned_utxo = utxo.clone();
        let cloned_utxo_value = cloned_utxo.value();
        let non_ada_assets = cloned_utxo_value.assets();

        if let Ok(raw_address) = utxo.address() {
            let soa = self.stake_or_address_from_address(&raw_address);
            self.tx_state(
                soa.as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                rollback,
            )
            .await?;

            self.datum_state(
                soa.as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                &utxo,
                rollback,
            )
            .await?;

            self.coin_state(
                raw_address
                    .to_bech32()
                    .unwrap_or(raw_address.to_string())
                    .as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                lovelace_amt.to_string().as_str(),
                rollback,
            )
            .await?;
        }

        // Spend Native Tokens
        for asset_group in non_ada_assets {
            for asset in asset_group.assets() {
                if let MultiEraAsset::AlonzoCompatibleOutput(policy_id, asset_name, quantity) =
                    asset.clone()
                {
                    let asset_name = hex::encode(asset_name.to_vec());

                    if let Ok(fingerprint) =
                        AssetFingerprint::from_parts(&hex::encode(policy_id), &asset_name)
                    {
                        if let Ok(fingerprint) = fingerprint.finger_print() {
                            self.token_state(
                                &address,
                                format!("{}#{}", input.hash(), input.index()).as_str(),
                                &hex::encode(policy_id),
                                &fingerprint,
                                quantity.to_string().as_str(),
                                rollback,
                            )
                            .await?
                        }
                    }
                };
            }
        }

        Ok(())
    }

    async fn process_produced_txo<'b>(
        &mut self,
        tx_hash: &Hash<32>,
        tx_output: &'b MultiEraOutput<'b>,
        output_idx: usize,
        rollback: bool,
    ) -> Result<(), Error> {
        if let Ok(raw_address) = tx_output.address() {
            let tx_address = raw_address.to_bech32().unwrap_or(raw_address.to_string());

            self.coin_state(
                &tx_address,
                &format!("{}#{}", tx_hash, output_idx),
                tx_output.value().coin().to_string().as_str(),
                !rollback,
            )
            .await?;

            self.datum_state(
                &tx_address,
                &format!("{}#{}", tx_hash, output_idx),
                tx_output,
                !rollback,
            )
            .await?;

            for asset_group in tx_output.value().assets() {
                for asset in asset_group.assets() {
                    if let MultiEraAsset::AlonzoCompatibleOutput(policy_id, asset_name, quantity) =
                        asset
                    {
                        let asset_name = hex::encode(asset_name.to_vec());
                        let policy_id_str = hex::encode(policy_id);

                        if let Ok(fingerprint) = AssetFingerprint::from_parts(
                            &hex::encode(policy_id_str.clone()),
                            &asset_name,
                        ) {
                            if let Ok(fingerprint) = fingerprint.finger_print() {
                                if !fingerprint.is_empty() {
                                    self.token_state(
                                        &tx_address,
                                        format!("{}#{}", tx_hash, output_idx).as_str(),
                                        &policy_id_str,
                                        &fingerprint,
                                        quantity.to_string().as_str(),
                                        !rollback,
                                    )
                                    .await?;
                                }
                            }
                        }
                    };
                }
            }

            let soa = self.stake_or_address_from_address(&raw_address);
            self.tx_state(
                soa.as_str(),
                &format!("{}#{}", tx_hash, output_idx),
                !rollback,
            )
            .await?;
        }

        Ok(())
    }

    pub async fn reduce<'a>(&mut self, mode: &'a DecodedBlockAction<'a>) -> Result<(), Error> {
        match mode {
            DecodedBlockAction::Genesis(genesis_utxo) => {
                for utxo in genesis_utxo {
                    let address = hex::encode(utxo.1.to_vec());
                    let key = format!("{}#{}", hex::encode(utxo.0), 0);

                    //warn!("gonna do tx state");

                    self.tx_state(&address, &key, true).await?;

                    // warn!("gonna do coin state");

                    self.coin_state(&address, &key, utxo.2.to_string().as_str(), false)
                        .await?
                }

                Ok(())
            }

            action @ (DecodedBlockAction::Forward(b, c, _)
            | DecodedBlockAction::Rollback(b, c, _)) => {
                let is_rollback = action.is_rollback();
                for tx in b.txs() {
                    if tx.is_valid() {
                        for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                            self.process_consumed_txo(c, &consumed, is_rollback).await?;
                        }

                        for (idx, produced) in tx.produces().iter() {
                            self.process_produced_txo(
                                &tx.hash(),
                                produced,
                                idx.clone(),
                                is_rollback,
                            )
                            .await?;
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        super::Reducer::Utxo(Reducer {
            config: self,
            output: Default::default(),
        })
    }
}
