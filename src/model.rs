use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};

use std::convert::Into;

use gasket::metrics::Readings;
use gasket::runtime::{Policy, TetherState};
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::{from_file, GenesisUtxo};
use pallas::{
    ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, OutputRef},
    network::miniprotocols::Point,
};

use serde::Deserialize;

use crate::pipeline::{Context, StageTypes};
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
    pub display: Option<crate::pipeline::console::Config>,
    pub pipeline: Option<crate::pipeline::Config>,
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

    pub fn gasket_policy(&self) -> Policy {
        Policy {
            tick_timeout: self.tick_timeout,
            bootstrap_retry: self.bootstrap_retry.clone().unwrap_or_default(),
            work_retry: self.work_retry.clone().unwrap_or_default(),
            teardown_retry: self.teardown_retry.clone().unwrap_or_default(),
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
    Rollback(Vec<Vec<u8>>),
}

impl RawBlockPayload {
    pub fn forward(block: Vec<u8>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Forward(block),
        }
    }

    pub fn backward(blocks: Vec<Vec<u8>>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Rollback(blocks),
        }
    }
}

pub struct TetherSnapshot {
    pub name: String,
    pub state: TetherState,
    pub snapshot: MetricsSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeteredNumber {
    value: u64,
}

impl Default for MeteredNumber {
    fn default() -> Self {
        Self { value: 0 }
    }
}

impl MeteredNumber {
    pub fn set(&mut self, to: u64) {
        self.value = to;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeteredString {
    value: String,
}

impl Default for MeteredString {
    fn default() -> Self {
        Self { value: "".into() }
    }
}

impl MeteredString {
    pub fn set(&mut self, to: &str) {
        self.value = to.to_string();
    }
}

impl From<&str> for MeteredString {
    fn from(item: &str) -> Self {
        MeteredString {
            value: item.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MeteredValue {
    Numerical(MeteredNumber),
    Label(MeteredString),
}

impl MeteredValue {
    pub fn set_num(&mut self, to: u64) {
        match self {
            MeteredValue::Numerical(metered_number) => {
                metered_number.set(to);
            }
            MeteredValue::Label(_) => {}
        }
    }

    pub fn set_str(&mut self, to: &str) {
        match self {
            MeteredValue::Numerical(_) => {}
            MeteredValue::Label(metered_string) => {
                metered_string.set(to);
            }
        }
    }

    pub fn get_num(&self) -> u64 {
        match self {
            MeteredValue::Numerical(metered_number) => metered_number.value.clone(),
            MeteredValue::Label(_) => 0,
        }
    }

    pub fn get_string(&self) -> String {
        match self {
            MeteredValue::Numerical(_) => "".into(),
            MeteredValue::Label(metered_string) => metered_string.value.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ProgramOutput {
    LogBatch(Vec<(String, String)>),
    Metrics(MetricsSnapshot),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub timestamp: Option<Duration>,
    pub chain_bar_depth: Option<MeteredValue>,
    pub chain_bar_progress: Option<MeteredValue>,
    pub blocks_processed: Option<MeteredValue>,
    pub transactions: Option<MeteredValue>,
    pub chain_era: Option<MeteredValue>,
    pub sources_status: Option<MeteredValue>,
    pub enrich_status: Option<MeteredValue>,
    pub reducer_status: Option<MeteredValue>,
    pub storage_status: Option<MeteredValue>,
    pub enrich_hit: Option<MeteredValue>,
    pub enrich_miss: Option<MeteredValue>,
    pub blocks_ingested: Option<MeteredValue>,
}

// todo: we need to not need this. it is atrocious and will miss metrics if we forget to add if branches
pub fn merge_metrics_snapshots(snapshots: &[MetricsSnapshot]) -> MetricsSnapshot {
    snapshots.iter().fold(
        MetricsSnapshot {
            timestamp: None,
            chain_bar_depth: None,
            chain_bar_progress: None,
            blocks_processed: None,
            transactions: None,
            chain_era: None,
            sources_status: None,
            enrich_status: None,
            reducer_status: None,
            storage_status: None,
            enrich_hit: None,
            enrich_miss: None,
            blocks_ingested: None,
        },
        |mut acc, snapshot| {
            // For each field, if the current snapshot has Some value, replace the accumulated value
            if let Some(val) = &snapshot.timestamp {
                acc.timestamp.replace(val.clone());
            }
            if let Some(val) = &snapshot.chain_bar_depth {
                acc.chain_bar_depth.replace(val.clone());
            }
            if let Some(val) = &snapshot.chain_bar_progress {
                acc.chain_bar_progress.replace(val.clone());
            }
            if let Some(val) = &snapshot.blocks_processed {
                acc.blocks_processed.replace(val.clone());
            }
            if let Some(val) = &snapshot.transactions {
                acc.transactions.replace(val.clone());
            }
            if let Some(val) = &snapshot.chain_era {
                acc.chain_era.replace(val.clone());
            }
            if let Some(val) = &snapshot.sources_status {
                acc.sources_status.replace(val.clone());
            }
            if let Some(val) = &snapshot.enrich_status {
                acc.enrich_status.replace(val.clone());
            }
            if let Some(val) = &snapshot.reducer_status {
                acc.reducer_status.replace(val.clone());
            }
            if let Some(val) = &snapshot.storage_status {
                acc.storage_status.replace(val.clone());
            }
            if let Some(val) = &snapshot.enrich_hit {
                acc.enrich_hit.replace(val.clone());
            }
            if let Some(val) = &snapshot.enrich_miss {
                acc.enrich_miss.replace(val.clone());
            }
            if let Some(val) = &snapshot.blocks_ingested {
                acc.blocks_ingested.replace(val.clone());
            }
            acc
        },
    )
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: Some(Default::default()),
            chain_era: Some(MeteredValue::Label(Default::default())),
            chain_bar_depth: Some(MeteredValue::Numerical(Default::default())),
            chain_bar_progress: Some(MeteredValue::Numerical(Default::default())),
            blocks_processed: Some(MeteredValue::Numerical(Default::default())),
            transactions: Some(MeteredValue::Numerical(Default::default())),
            sources_status: Some(MeteredValue::Label(Default::default())),
            enrich_status: Some(MeteredValue::Label(Default::default())),
            reducer_status: Some(MeteredValue::Label(Default::default())),
            storage_status: Some(MeteredValue::Label(Default::default())),
            enrich_hit: Some(MeteredValue::Numerical(Default::default())),
            enrich_miss: Some(MeteredValue::Numerical(Default::default())),
            blocks_ingested: Some(MeteredValue::Numerical(Default::default())),
        }
    }
}

fn merge_field<T: Clone + PartialEq>(target: &mut Option<T>, source: &Option<T>) {
    if let Some(source_value) = source {
        match target {
            Some(target_value) if target_value == source_value => {} // Skip if values are equal
            _ => *target = Some(source_value.clone()),               // Update if different or None
        }
    }
}

impl MetricsSnapshot {
    pub fn get_metrics_key(&self, prop_name: &str) -> Option<MeteredValue> {
        match prop_name {
            "chain_era" => Some(self.chain_era.clone().unwrap()),
            "chain_bar_depth" => Some(self.chain_bar_depth.clone().unwrap()),
            "chain_bar_progress" => Some(self.chain_bar_progress.clone().unwrap()),
            "blocks_processed" => Some(self.blocks_processed.clone().unwrap()),
            "transactions" => Some(self.transactions.clone().unwrap()),
            _ => None,
        }
    }

    pub fn merge(&mut self, other: &MetricsSnapshot) {
        merge_field(&mut self.timestamp, &other.timestamp);
        merge_field(&mut self.chain_bar_depth, &other.chain_bar_depth);
        merge_field(&mut self.chain_bar_progress, &other.chain_bar_progress);
        merge_field(&mut self.blocks_processed, &other.blocks_processed);
        merge_field(&mut self.transactions, &other.transactions);
        merge_field(&mut self.chain_era, &other.chain_era);
        merge_field(&mut self.sources_status, &other.sources_status);
        merge_field(&mut self.enrich_status, &other.enrich_status);
        merge_field(&mut self.reducer_status, &other.reducer_status);
        merge_field(&mut self.storage_status, &other.storage_status);
        merge_field(&mut self.enrich_hit, &other.enrich_hit);
        merge_field(&mut self.enrich_miss, &other.enrich_miss);
        merge_field(&mut self.blocks_ingested, &other.blocks_ingested);
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
    Forward(MultiEraBlock<'a>, &'a BlockContext, Option<Point>),
    Genesis(Vec<GenesisUtxo>), // This one probably needs to stay owned
    Rollback(MultiEraBlock<'a>, &'a BlockContext, Option<Point>),
}

impl<'a> DecodedBlockAction<'a> {
    pub fn is_rollback(&self) -> bool {
        match self {
            Self::Rollback(..) => true,
            _ => false,
        }
    }

    pub fn parsed_block(&self) -> Option<&MultiEraBlock> {
        match self {
            Self::Forward(block, ..) | Self::Rollback(block, ..) => Some(block),
            _ => None,
        }
    }

    pub fn block_era(&self) -> Era {
        match self {
            Self::Rollback(block, ..) | Self::Forward(block, ..) => block.era(),
            _ => Era::Byron,
        }
    }

    pub fn block_rolling_to_hash(&self) -> Hash<32> {
        match self {
            Self::Genesis(_) => Hash::new(
                hex::decode("5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb")
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ),
            Self::Rollback(block, ..) | Self::Forward(block, ..) => block.hash(),
        }
    }

    pub fn block_rolling_to_point(&self) -> Point {
        match self {
            Self::Genesis(_) => Point::new(0 as u64, self.block_rolling_to_hash().to_vec()),
            Self::Rollback(block, ..) | Self::Forward(block, ..) => {
                Point::new(block.slot(), block.hash().to_vec())
            }
        }
    }

    pub fn block_tx_count(&self) -> usize {
        match self {
            Self::Genesis(utxo) => utxo.len(),
            Self::Rollback(block, ..) | Self::Forward(block, ..) => block.tx_count(),
        }
    }

    pub fn block_number(&self) -> i64 {
        match self {
            Self::Genesis(_) => -1,
            Self::Rollback(block, ..) | Self::Forward(block, ..) => block.number() as i64,
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
    Forward(Vec<u8>, BlockContext, Option<Point>),
    Genesis(Vec<GenesisUtxo>),
    Rollback(Vec<u8>, BlockContext, Option<Point>),
}

impl EnrichedBlockPayload {
    pub fn forward(block: Vec<u8>, ctx: BlockContext) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Forward(block, ctx, None),
        }
    }

    pub fn rollback(
        block: Vec<u8>,
        ctx: BlockContext,
        previous_block_point: Point,
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Rollback(block, ctx, Some(previous_block_point)),
        }
    }

    // possibly should be removed in favor of the into() -> ReducerBlockAction or whatever its called
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
    BlockFinished(Point, Option<Vec<u8>>, i64, u64, i64, bool, bool),
    Noop,
}

impl CRDTCommand {
    pub fn block_starting(block_action: &DecodedBlockAction) -> CRDTCommand {
        CRDTCommand::BlockStarting(block_action.block_rolling_to_point())
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
        block_bytes: Option<Vec<u8>>,
        era: i64,
        tx_len: u64,
        block_number: i64,
        is_rollback: bool,
        is_genesis: bool,
    ) -> CRDTCommand {
        log::debug!("block finished {:?}", point);
        CRDTCommand::BlockFinished(
            point,
            block_bytes,
            era,
            tx_len,
            block_number,
            is_rollback,
            is_genesis,
        )
    }
}
