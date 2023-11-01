use std::ops::Deref;
use std::sync::Arc;

use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use pallas::codec::utils::KeyValuePairs;
use pallas::ledger::primitives::alonzo::{Metadatum, MetadatumLabel};
use pallas::ledger::traverse::MultiEraBlock;

use gasket::messaging::tokio::OutputPort;

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::crosscut;
use crate::model::{CRDTCommand, Delta};
use crate::pipeline::Context;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub royalty_key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
    ctx: Arc<Mutex<Context>>,
}

const CIP25_META_NFT: u64 = 721;
const CIP27_META_ROYALTIES: u64 = 777;

#[inline]
fn kv_pairs_to_hashmap(
    kv_pairs: &KeyValuePairs<Metadatum, Metadatum>,
) -> serde_json::Map<String, Value> {
    #[inline]
    fn metadatum_to_value(m: &Metadatum) -> Value {
        match m {
            Metadatum::Int(int_value) => Value::String(int_value.to_string()),
            Metadatum::Bytes(bytes) => Value::String(hex::encode(bytes.as_slice())),
            Metadatum::Text(text) => Value::String(text.clone()),
            Metadatum::Array(array) => {
                let json_array: Vec<Value> = array.iter().map(metadatum_to_value).collect();
                Value::Array(json_array)
            }
            Metadatum::Map(kv_pairs) => {
                let json_object = kv_pairs_to_hashmap(kv_pairs);
                Value::Object(json_object)
            }
        }
    }

    let mut hashmap = serde_json::Map::new();
    for (key, value) in kv_pairs.deref() {
        if let Metadatum::Text(key_str) = key {
            hashmap.insert(key_str.clone(), metadatum_to_value(value));
        }
    }

    hashmap
}

impl Reducer {
    fn find_metadata_policy_assets(
        &self,
        metadata: &Metadatum,
        target_policy_id: &str,
    ) -> Option<KeyValuePairs<Metadatum, Metadatum>> {
        match metadata {
            Metadatum::Map(kv) => {
                for (policy_label, policy_contents) in kv.iter() {
                    if let Metadatum::Text(policy_label) = policy_label {
                        if policy_label == target_policy_id {
                            if let Metadatum::Map(policy_inner_map) = policy_contents {
                                return Some(policy_inner_map.clone());
                            }
                        }
                    }
                }

                None
            }
            _ => None,
        }
    }

    fn asset_fingerprint(&self, data_list: [&str; 2]) -> Result<String, bech32::Error> {
        let combined_parts = data_list.join("");
        let raw = hex::decode(combined_parts).unwrap();

        let mut hasher = Blake2bVar::new(20).unwrap();
        hasher.update(&raw);
        let mut buf = [0u8; 20];
        hasher.finalize_variable(&mut buf).unwrap();
        let base32_combined = buf.to_base32();
        bech32::encode("asset", base32_combined, Variant::Bech32)
    }

    fn get_asset_label(&self, l: Metadatum) -> Result<String, &str> {
        match l {
            Metadatum::Text(l) => Ok(l),
            Metadatum::Int(l) => Ok(l.to_string()),
            Metadatum::Bytes(l) => Ok(String::from_utf8(l.to_vec())
                .unwrap_or_default()
                .to_string()),
            _ => Err("Malformed metadata label"),
        }
    }

    fn get_metadata_fragment(
        &self,
        asset_name: String,
        policy_id: String,
        asset_metadata: &KeyValuePairs<Metadatum, Metadatum>,
        cip: u64,
    ) -> String {
        let mut std_wrap_map = serde_json::Map::new();
        let mut policy_wrap_map = serde_json::Map::new();
        let mut asset_wrap_map = serde_json::Map::new();
        let asset_map = kv_pairs_to_hashmap(asset_metadata);

        asset_wrap_map.insert(asset_name, Value::Object(asset_map));

        if cip == CIP27_META_ROYALTIES {
            std_wrap_map.insert(cip.to_string(), Value::Object(asset_wrap_map));
        } else if cip == CIP25_META_NFT {
            policy_wrap_map.insert(policy_id, Value::Object(asset_wrap_map));
            std_wrap_map.insert(cip.to_string(), Value::Object(policy_wrap_map));
        }

        serde_json::to_string(&std_wrap_map).unwrap()
    }

    async fn extract_and_aggregate_cip_metadata(
        &self,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        cip: u64,
        policy_map: Metadatum,
        policy_id_str: String,
        asset_name_str: String,
        rollback: bool,
        prefix: &str,
        royalty_prefix: &str,
        timestamp: u64,
    ) -> Result<(), gasket::error::Error> {
        if let Some(policy_assets) = self.find_metadata_policy_assets(&policy_map, &policy_id_str) {
            let filtered_policy_assets = policy_assets.iter().find(|(l, _)| {
                let asset_label = self.get_asset_label(l.clone()).unwrap();
                asset_label.as_str() == asset_name_str
            });

            if let Some((_, Metadatum::Map(asset_metadata))) = filtered_policy_assets {
                if let Ok(fingerprint_str) = self.asset_fingerprint([
                    &policy_id_str.clone(),
                    hex::encode(&asset_name_str).as_str(),
                ]) {
                    // let metadata_final: Metadata = self.get_wrapped_metadata_fragment(
                    //     cip,
                    //     asset_name_str.clone(),
                    //     policy_id_str.clone(),
                    //     asset_metadata,
                    // );

                    let meta_payload = self.get_metadata_fragment(
                        asset_name_str,
                        policy_id_str.clone(),
                        asset_metadata,
                        cip,
                    );

                    if !meta_payload.is_empty()
                        && (cip == CIP25_META_NFT || cip == CIP27_META_ROYALTIES)
                    {
                        output
                            .lock()
                            .await
                            .send(gasket::messaging::Message::from(match cip {
                                CIP27_META_ROYALTIES => match rollback {
                                    false => CRDTCommand::last_write_wins(
                                        Some(&royalty_prefix),
                                        &policy_id_str,
                                        meta_payload,
                                        timestamp,
                                    ),
                                    true => CRDTCommand::sorted_set_remove(
                                        Some(&royalty_prefix),
                                        &policy_id_str,
                                        meta_payload,
                                        Delta::from(timestamp as i64),
                                    ),
                                },

                                CIP25_META_NFT => match rollback {
                                    false => CRDTCommand::last_write_wins(
                                        Some(&prefix),
                                        &fingerprint_str,
                                        meta_payload,
                                        timestamp,
                                    ),

                                    true => CRDTCommand::sorted_set_remove(
                                        Some(&prefix),
                                        &fingerprint_str,
                                        meta_payload,
                                        timestamp as i64,
                                    ),
                                },

                                _ => CRDTCommand::noop(), // this never gets called.. need to find a clean way not to need this
                            }))
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn reduce<'b>(
        &mut self,
        block: Option<MultiEraBlock<'b>>,
        rollback: bool,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
    ) -> Result<(), gasket::framework::WorkerError> {
        match block {
            Some(block) => {
                let prefix = self.config.key_prefix.as_deref().unwrap_or("m");
                let royalty_prefix = self.config.royalty_key_prefix.as_deref().unwrap_or("m.r");

                let time_provider = crosscut::time::NaiveProvider::new(self.ctx.clone()).await;

                for tx in block.txs() {
                    for asset_group in tx.mints() {
                        for multi_asset in asset_group.assets() {
                            let policy_id_str = hex::encode(multi_asset.policy());
                            if let Some(quantity) = multi_asset.mint_coin() {
                                let asset_name_str =
                                    match String::from_utf8(multi_asset.name().to_vec()) {
                                        Ok(asset_name) => asset_name,
                                        Err(_) => hex::encode(multi_asset.name()),
                                    };

                                if !policy_id_str.is_empty() {
                                    for supported_metadata_cip in
                                        vec![CIP25_META_NFT, CIP27_META_ROYALTIES]
                                    {
                                        if let Some(policy_map) = tx
                                            .metadata()
                                            .find(MetadatumLabel::from(supported_metadata_cip))
                                        {
                                            if quantity > -1 {
                                                self.extract_and_aggregate_cip_metadata(
                                                    output.clone(),
                                                    supported_metadata_cip,
                                                    policy_map.clone(),
                                                    policy_id_str.clone(),
                                                    asset_name_str.clone(),
                                                    rollback,
                                                    prefix,
                                                    royalty_prefix,
                                                    time_provider
                                                        .slot_to_wallclock(block.slot().clone()),
                                                )
                                                .await
                                                .unwrap_or(());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            None => {} // skip block if this is a genesis set
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Mutex<Context>>) -> super::Reducer {
        let worker = Reducer { config: self, ctx };

        super::Reducer::Metadata(worker)
    }
}
