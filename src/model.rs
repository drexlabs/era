use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};

use std::convert::Into;

use gasket::runtime::Policy;
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::{from_file, GenesisUtxo};
use pallas::{
    ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, OutputRef},
    network::miniprotocols::Point,
};

use serde::Deserialize;

use crate::pipeline::Context;
use crate::{crosscut, enrich, prelude::*, reducers, sources, storage};

#[derive(Deserialize)]
pub struct ConfigRoot {
    pub source: Option<sources::Config>,
    pub enrich: Option<enrich::Config>,
    pub reducers: Option<reducers::worker::Config>,
    pub storage: Option<storage::Config>,
    pub intersect: Option<crosscut::IntersectConfig>,
    pub finalize: Option<crosscut::FinalizeConfig>,
    pub chain: Option<crosscut::ChainConfig>,
    pub blocks: Option<crosscut::historic::BlockConfig>,
    pub block_data_policy: Option<crosscut::policies::BlockDataPolicy>, // todo revisit this
    pub genesis: Option<String>,
    pub tick_timeout: Option<Duration>,
    pub bootstrap_retry: Option<gasket::retries::Policy>,
    pub work_retry: Option<gasket::retries::Policy>,
    pub teardown_retry: Option<gasket::retries::Policy>,
}

impl ConfigRoot {
    pub fn new(with_file: &Option<std::path::PathBuf>) -> Result<Self, config::ConfigError> {
        let runtime_config = config::Config::builder();

        match with_file.as_ref().and_then(|x| x.to_str()) {
            Some(with_file) => {
                runtime_config.add_source(config::File::with_name(with_file).required(true))
            }

            None => runtime_config
                .add_source(config::File::with_name("/etc/era/config.toml").required(false))
                .add_source(config::File::with_name("~/.local/era/config.toml").required(false))
                .add_source(config::File::with_name("config.toml").required(false)),
        }
        .add_source(config::Environment::with_prefix("ERA").separator("_")) // consider making this configurable -- way later
        .build()?
        .try_deserialize()
    }

    pub fn take_gasket_policy(&mut self) -> Policy {
        Policy {
            tick_timeout: self.tick_timeout.take(),
            bootstrap_retry: self.bootstrap_retry.take().unwrap_or_default(),
            work_retry: self.work_retry.take().unwrap_or_default(),
            teardown_retry: self.teardown_retry.take().unwrap_or_default(),
        }
    }

    pub fn take_some_to_make_context(&mut self) -> Arc<Context> {
        let chain = self.chain.take().unwrap_or_default();
        let chaintag = match chain {
            crosscut::ChainConfig::Mainnet => "mainnet",
            crosscut::ChainConfig::Testnet => "testnet",
            crosscut::ChainConfig::PreProd => "preprod",
            crosscut::ChainConfig::Preview => "preview",
            crosscut::ChainConfig::Custom(_) => "custom", // todo: look into this
        };

        let genesis_path = &self
            .genesis
            .take()
            .unwrap_or(format!("assets/{}-byron-genesis.json", chaintag,));

        log::info!("making context: with genesis ({})", genesis_path);

        Arc::new(Context {
            chain: chain.into(),
            intersect: self.intersect.take(),
            finalize: self.finalize.take(),
            block_buffer: self.blocks.take().unwrap_or_default().into(),
            genesis_file: from_file(Path::new(genesis_path)).unwrap(),
        })
    }
}

impl From<ConfigRoot> for Context {
    fn from(config: ConfigRoot) -> Self {
        let chain = config.chain.unwrap().into();
        let chaintag = match chain {
            crosscut::ChainConfig::Mainnet => "mainnet",
            crosscut::ChainConfig::Testnet => "testnet",
            crosscut::ChainConfig::PreProd => "preprod",
            crosscut::ChainConfig::Preview => "preview",
            crosscut::ChainConfig::Custom(_) => "custom", // todo: look into this
        };

        Self {
            chain: chain.into(),
            intersect: config.intersect.into(),
            finalize: match config.finalize.unwrap().into() {
                Some(finalize_config) => Some(finalize_config.into()),
                None => None,
            },
            block_buffer: config.blocks.unwrap_or_default().into(),
            genesis_file: from_file(Path::new(
                &config
                    .genesis
                    .unwrap_or(format!("/etc/era/{}-byron-genesis.json", chaintag,)),
            ))
            .unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RawBlockPayload {
    Genesis,
    Forward(Vec<u8>),
    Rollback(Vec<u8>, (Point, u64)),
}

impl RawBlockPayload {
    pub fn roll_forward(block: Vec<u8>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Forward(block),
        }
    }

    pub fn roll_back(
        block: Vec<u8>,
        last_good_block_info: (Point, u64),
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Rollback(block, last_good_block_info),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockContext {
    utxos: HashMap<String, (Era, Vec<u8>)>,
    pub block_number: u64,
}

impl BlockContext {
    pub fn import_ref_output(&mut self, key: &OutputRef, era: Era, cbor: Vec<u8>) {
        self.utxos.insert(key.to_string(), (era, cbor));
    }

    pub fn find_utxo(&self, key: &OutputRef) -> Result<MultiEraOutput, Error> {
        let (era, cbor) = self
            .utxos
            .get(&key.to_string())
            .ok_or_else(|| Error::missing_utxo(key))?;

        match MultiEraOutput::decode(*era, cbor) {
            Ok(on_chain_output) => Ok(on_chain_output),
            Err(e) => Err(Error::missing_utxo(e)),
        }
    }

    pub fn get_all_keys(&self) -> Vec<String> {
        self.utxos.keys().map(|x| x.clone()).collect()
    }
}

pub enum DecodedBlockAction<'a> {
    Forward(MultiEraBlock<'a>, &'a BlockContext, Option<(Point, u64)>),
    Genesis(Vec<GenesisUtxo>), // This one probably needs to stay owned
    Rollback(MultiEraBlock<'a>, &'a BlockContext, Option<(Point, u64)>),
}

impl<'a> DecodedBlockAction<'a> {
    pub fn is_rollback(&self) -> bool {
        match self {
            Self::Rollback(..) => true,
            _ => false,
        }
    }
}

impl<'a> From<&'a EnrichedBlockPayload> for DecodedBlockAction<'a> {
    fn from(enriched_block_payload: &'a EnrichedBlockPayload) -> Self {
        let decoded = enriched_block_payload.decode_block();
        match enriched_block_payload {
            EnrichedBlockPayload::Forward(_, block_context, _) => {
                DecodedBlockAction::Forward(decoded.unwrap(), block_context, None)
            }
            EnrichedBlockPayload::Genesis(genesis_utxo) => {
                DecodedBlockAction::Genesis(genesis_utxo.clone()) // This clone might still be needed
            }
            EnrichedBlockPayload::Rollback(_, block_context, point) => {
                DecodedBlockAction::Rollback(decoded.unwrap(), block_context, point.clone())
            }
        }
    }
}

#[derive(Clone)] // unfortunate that clone needs to be allowed here, gasket clones messages :(
pub enum EnrichedBlockPayload {
    Forward(Vec<u8>, BlockContext, Option<(Point, u64)>),
    Genesis(Vec<GenesisUtxo>),
    Rollback(Vec<u8>, BlockContext, Option<(Point, u64)>),
}

impl EnrichedBlockPayload {
    pub fn genesis(self) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message { payload: self }
    }

    pub fn forward(block: Vec<u8>, ctx: BlockContext) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Forward(block, ctx, None),
        }
    }

    pub fn rollback(
        block: Vec<u8>,
        ctx: BlockContext,
        last_good_block: (Point, u64),
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Rollback(block, ctx, Some(last_good_block)),
        }
    }

    pub fn decode_block(&self) -> Option<MultiEraBlock> {
        match self {
            Self::Forward(block, _, _) | Self::Rollback(block, _, _) => {
                MultiEraBlock::decode(block).ok()
            }

            _ => None,
        }
    }
}

pub type Set = String;
pub type Member = String;
pub type Key = String;
pub type Delta = i64;
pub type Timestamp = u64;

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
    BigInt(i128),
    Cbor(Vec<u8>),
    Json(serde_json::Value),
}

impl From<String> for Value {
    fn from(x: String) -> Self {
        Value::String(x)
    }
}

impl From<Vec<u8>> for Value {
    fn from(x: Vec<u8>) -> Self {
        Value::Cbor(x)
    }
}

impl From<serde_json::Value> for Value {
    fn from(x: serde_json::Value) -> Self {
        Value::Json(x)
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CRDTCommand {
    BlockStarting(Point),
    SetAdd(Set, Member),
    SetRemove(Set, Member),
    SortedSetAdd(Set, Member, Delta),
    SortedSetRemove(Set, Member, Delta),
    SortedSetMemberRemove(Set, Member),
    GrowOnlySetAdd(Set, Member),
    LastWriteWins(Key, Value, Timestamp),
    AnyWriteWins(Key, Value),
    Spoil(Key),
    PNCounter(Key, Delta),
    HashCounter(Key, Member, Delta),
    HashSetValue(Key, Member, Value),
    HashSetMulti(Key, Vec<Member>, Vec<Value>),
    HashUnsetKey(Key, Member),
    UnsetKey(Key),
    BlockFinished(Point, Vec<u8>, i64, u64, i64, bool),
    Noop,
}

impl CRDTCommand {
    pub fn block_starting(
        block: Option<&MultiEraBlock>,
        genesis_hash: Option<&Hash<32>>,
    ) -> CRDTCommand {
        match (block, genesis_hash) {
            (Some(block), None) => {
                log::debug!("block starting");
                let hash = block.hash();
                let slot = block.slot();
                let point = Point::Specific(slot, hash.to_vec());
                CRDTCommand::BlockStarting(point)
            }

            (_, Some(genesis_hash)) => {
                log::debug!("block starting");
                let point = Point::Specific(0, genesis_hash.to_vec());
                CRDTCommand::BlockStarting(point)
            }

            _ => CRDTCommand::Noop, // never called normally
        }
    }

    pub fn set_add(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SetAdd(key, member)
    }

    pub fn set_remove(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SetRemove(key, member)
    }

    pub fn noop() -> CRDTCommand {
        CRDTCommand::Noop
    }

    pub fn sorted_set_add(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SortedSetAdd(key, member, delta)
    }

    pub fn sorted_set_remove(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SortedSetRemove(key, member, delta)
    }

    pub fn sorted_set_member_remove(
        prefix: Option<&str>,
        key: &str,
        member: String,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SortedSetMemberRemove(key, member)
    }

    pub fn any_write_wins<K, V>(prefix: Option<&str>, key: K, value: V) -> CRDTCommand
    where
        K: ToString,
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::AnyWriteWins(key, value.into())
    }

    pub fn spoil<K>(prefix: Option<&str>, key: K) -> CRDTCommand
    where
        K: ToString,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::Spoil(key)
    }

    pub fn last_write_wins<V>(
        prefix: Option<&str>,
        key: &str,
        value: V,
        ts: Timestamp,
    ) -> CRDTCommand
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::LastWriteWins(key, value.into(), ts)
    }

    pub fn hash_set_value<V>(
        prefix: Option<&str>,
        key: &str,
        member: String,
        value: V,
    ) -> CRDTCommand
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashSetValue(key, member, value.into())
    }

    pub fn unset_key(prefix: Option<&str>, key: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::UnsetKey(key)
    }

    pub fn hash_del_key(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashUnsetKey(key, member)
    }

    pub fn hash_counter(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashCounter(key, member, delta)
    }

    pub fn block_finished(
        point: Point,
        block_bytes: Vec<u8>, // make this take pointer to parsed block
        era: i64,
        tx_len: u64,
        block_number: i64,
        is_rollback: bool,
    ) -> CRDTCommand {
        log::debug!("block finished {:?}", point);
        CRDTCommand::BlockFinished(point, block_bytes, era, tx_len, block_number, is_rollback)
    }
}
