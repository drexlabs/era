use std::{collections::HashMap, fmt::Debug};

use std::convert::Into;

use pallas::codec::minicbor;
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::traverse::MultiEraPolicyAssets;
use pallas::{
    ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, OutputRef},
    network::miniprotocols::Point,
};

use pallas_addresses::{Address, Error as AddressError};
use pallas_primitives::babbage::MintedDatumOption;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub enum RawBlockPayload {
    RollForwardGenesis,
    RollForward(Vec<u8>),
    RollBack(Vec<u8>, (Point, u64)),
}

impl RawBlockPayload {
    pub fn roll_forward(block: Vec<u8>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(block),
        }
    }

    pub fn roll_back(
        block: Vec<u8>,
        last_good_block_info: (Point, u64),
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(block, last_good_block_info),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockContext {
    utxos: HashMap<String, (Era, Vec<u8>)>,
    pub block_number: u64,
}

#[derive(Clone, Debug)]
pub enum BlockOrigination<'b> {
    Chain(MultiEraOutput<'b>),
    Genesis(GenesisUtxo),
}

impl<'b> BlockOrigination<'b> {
    pub fn address(&self) -> Result<Address, AddressError> {
        match self {
            BlockOrigination::Chain(output) => output.address(),
            BlockOrigination::Genesis(genesis) => Ok(Address::Byron(genesis.1.clone())),
        }
    }

    pub fn lovelace_amount(&self) -> u64 {
        match self {
            BlockOrigination::Chain(output) => output.lovelace_amount(),
            BlockOrigination::Genesis(genesis) => genesis.2,
        }
    }

    pub fn non_ada_assets(&'b self) -> Vec<MultiEraPolicyAssets<'b>> {
        match self {
            BlockOrigination::Chain(output) => output.non_ada_assets(),
            BlockOrigination::Genesis(_) => vec![],
        }
    }

    pub fn datum(&self) -> Option<MintedDatumOption> {
        match self {
            BlockOrigination::Chain(output) => output.datum(),
            BlockOrigination::Genesis(_) => None,
        }
    }
}

impl BlockContext {
    pub fn import_ref_output(&mut self, key: &OutputRef, era: Era, cbor: Vec<u8>) {
        self.utxos.insert(key.to_string(), (era, cbor));
    }

    pub fn find_utxo(&self, key: &OutputRef) -> Result<BlockOrigination, Error> {
        let (era, cbor) = self
            .utxos
            .get(&key.to_string())
            .ok_or_else(|| Error::missing_utxo(key))?;

        match MultiEraOutput::decode(*era, cbor) {
            Ok(on_chain_output) => Ok(BlockOrigination::Chain(on_chain_output)),
            Err(e) => Err(Error::missing_utxo(e)),
        }
    }

    pub fn get_all_keys(&self) -> Vec<String> {
        self.utxos.keys().map(|x| x.clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub enum EnrichedBlockPayload {
    RollForward(Vec<u8>, BlockContext),
    RollForwardGenesis(Vec<GenesisUtxo>),
    RollBack(Vec<u8>, BlockContext, (Point, u64)),
}

impl EnrichedBlockPayload {
    pub fn roll_forward_genesis(utxos: Vec<GenesisUtxo>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForwardGenesis(utxos),
        }
    }

    pub fn roll_forward(block: Vec<u8>, ctx: BlockContext) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(block, ctx),
        }
    }

    pub fn roll_back(
        block: Vec<u8>,
        ctx: BlockContext,
        last_good_block_info: (Point, u64),
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(block, ctx, last_good_block_info),
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
    BlockFinished(Point, Option<Vec<u8>>, bool),
    Noop,
}

impl CRDTCommand {
    pub fn block_starting(
        block: Option<MultiEraBlock>,
        genesis_hash: Option<Hash<32>>,
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
        block_bytes: Option<Vec<u8>>,
        rollback: bool,
    ) -> CRDTCommand {
        log::debug!("block finished {:?}", point);
        CRDTCommand::BlockFinished(point, block_bytes, rollback)
    }
}
