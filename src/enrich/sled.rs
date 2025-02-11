use std::sync::Arc;

use gasket::framework::*;

use gasket::messaging::{InputPort, OutputPort};
use log::warn;
use pallas::ledger::configs::byron::{genesis_utxos, GenesisUtxo};

use pallas::ledger::traverse::MultiEraOutput;
use pallas::network::miniprotocols::Point;
use pallas::{
    codec::minicbor,
    ledger::traverse::{Era, MultiEraBlock, MultiEraTx, OutputRef},
};

use serde::Deserialize;
use sled::IVec;

use crate::pipeline::Context;
use crate::{
    model::{self, BlockContext, EnrichedBlockPayload, RawBlockPayload},
    pipeline, Error,
};

const DB_CACHE_BYTES: usize = 1073741824;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub db_path: Option<String>,
    pub rollback_db_path: Option<String>,
}

impl Config {
    pub fn bootstrapper(mut self, ctx: Arc<pipeline::Context>) -> Stage {
        self.rollback_db_path = Some(ctx.as_ref().block_buffer.config.rollback_db_path.clone());

        Stage {
            config: self,
            enrich_inserts: Default::default(),
            enrich_removes: Default::default(),
            enrich_matches: Default::default(),
            enrich_mismatches: Default::default(),
            enrich_blocks: Default::default(),
            enrich_transactions: Default::default(),
            enrich_cancelled_empty_tx: Default::default(),
            input: Default::default(),
            output: Default::default(),
            ctx,
        }
    }
}

// bool - true = genesis txs
struct SledTxValue(u16, Vec<u8>);

impl TryInto<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_into(self) -> Result<IVec, Self::Error> {
        let SledTxValue(era, body) = self;
        minicbor::to_vec((era, body))
            .map(|x| IVec::from(x))
            .map_err(crate::Error::cbor)
    }
}

impl TryFrom<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_from(value: IVec) -> Result<Self, Self::Error> {
        let (tag, body): (u16, Vec<u8>) = minicbor::decode(&value).map_err(crate::Error::cbor)?;

        Ok(SledTxValue(tag, body))
    }
}

#[inline]
fn fetch_referenced_utxo<'a>(
    db: &sled::Db,
    utxo_ref: OutputRef,
) -> Result<Option<(OutputRef, Era, Vec<u8>)>, crate::Error> {
    if let Some(ivec) = db
        .get(utxo_ref.to_string().as_bytes())
        .map_err(crate::Error::storage)?
    {
        let SledTxValue(era, cbor) = ivec.try_into().map_err(crate::Error::storage)?;
        let era: Era = era.try_into().map_err(crate::Error::storage)?;

        Ok(Some((utxo_ref.clone(), era, cbor)))
    } else {
        Err(Error::MissingUtxo(utxo_ref.to_string()))
    }
}

pub struct Worker {
    enrich_db: Option<sled::Db>,
    rollback_db: Option<sled::Db>,
}

impl Worker {
    fn db_refs_all(&self) -> Result<Option<(&sled::Db, &sled::Db)>, ()> {
        match (self.db_ref_enrich(), self.db_ref_rollback()) {
            (Some(db), Some(consumed_ring)) => Ok(Some((db, consumed_ring))),
            _ => Err(()),
        }
    }

    fn db_ref_enrich(&self) -> Option<&sled::Db> {
        match self.enrich_db.as_ref() {
            None => None,
            Some(db) => Some(db),
        }
    }

    fn db_ref_rollback(&self) -> Option<&sled::Db> {
        match self.rollback_db.as_ref() {
            None => None,
            Some(db) => Some(db),
        }
    }

    fn insert_produced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
        inserts: &gasket::metrics::Counter,
    ) -> Result<(), crate::Error> {
        let mut insert_batch = sled::Batch::default();
        let mut amt = 0;

        for tx in txs.iter() {
            for (idx, output) in tx.produces() {
                let value: IVec = SledTxValue(tx.era() as u16, output.encode()).try_into()?;
                insert_batch.insert(format!("{}#{}", tx.hash(), idx).as_bytes(), value);
                amt += 1;
            }
        }

        db.apply_batch(insert_batch)
            .map_err(crate::Error::storage)?;

        inserts.inc(amt);

        Ok(())
    }

    fn insert_genesis_utxo(
        &self,
        db: &sled::Db,
        genesis_utxo: &GenesisUtxo,
        inserts: &gasket::metrics::Counter,
    ) -> Result<(), crate::Error> {
        let address = pallas::ledger::primitives::byron::Address {
            payload: genesis_utxo.1.payload.clone(),
            crc: genesis_utxo.1.crc,
        };

        let byron_output = pallas::ledger::primitives::byron::TxOut {
            address,
            amount: genesis_utxo.2,
        };

        let byron_txo = MultiEraOutput::from_byron(&byron_output);

        let value: IVec = SledTxValue(Era::Byron as u16, byron_txo.encode()).try_into()?;

        db.insert(format!("{}#0", genesis_utxo.0).as_bytes(), value)
            .map_err(Error::storage)?;

        inserts.inc(1);

        Ok(())
    }

    fn remove_produced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
        enrich_removes: &gasket::metrics::Counter,
    ) -> Result<(), crate::Error> {
        let mut remove_batch = sled::Batch::default();
        let mut amt = 0;

        for tx in txs.iter() {
            for (idx, _) in tx.produces() {
                amt += txs.len() as u64;

                remove_batch.remove(format!("{}#{}", tx.hash(), idx).as_bytes());
            }
        }

        db.apply_batch(remove_batch)
            .map_err(crate::Error::storage)?;

        enrich_removes.inc(amt);

        Ok(())
    }

    #[inline]
    fn par_fetch_referenced_utxos(
        &self,
        db: &sled::Db,
        block_number: u64,
        txs: &[MultiEraTx],
    ) -> (BlockContext, u64, u64) {
        let mut ctx = BlockContext::default();

        let mut match_count: u64 = 0;
        let mut mismatch_count: u64 = 0;

        ctx.block_number = block_number.clone();

        let required: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.requires())
            .map(|input| input.output_ref())
            .collect();

        let matches: Result<Vec<_>, crate::Error> = required
            .into_iter()
            .map(|utxo_ref| fetch_referenced_utxo(db, utxo_ref))
            .collect();

        match matches {
            Ok(results) => {
                for result in results {
                    match result {
                        Some((key, era, cbor)) => {
                            ctx.import_ref_output(&key, era, cbor);
                            match_count += 1;
                        }
                        None => {
                            mismatch_count += 1;
                        }
                    }
                }
            }
            Err(_) => mismatch_count += txs.len() as u64,
        }

        (ctx, match_count, mismatch_count)
    }

    fn get_removed(
        &self,
        consumed_ring: &sled::Db,
        key: &[u8],
    ) -> Result<Option<IVec>, crate::Error> {
        consumed_ring.get(key).map_err(crate::Error::storage)
    }

    fn remove_consumed_utxos(
        &self,
        db: &sled::Db,
        consumed_ring: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<(Result<(), ()>, u64), ()> {
        let mut remove_batch = sled::Batch::default();
        let mut current_values_batch = sled::Batch::default();

        let mut remove_count: u64 = 0;

        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        for key in keys.iter() {
            if let Some(current_value) = db
                .get(key.to_string())
                .map_err(crate::Error::storage)
                .unwrap()
            {
                current_values_batch.insert(key.to_string().as_bytes(), current_value);
            }

            remove_batch.remove(key.to_string().as_bytes());
            remove_count += 1;
        }

        let result: Result<(), ()> = match (
            db.apply_batch(remove_batch),
            consumed_ring.apply_batch(current_values_batch),
        ) {
            (Ok(()), Ok(())) => Ok(()),
            _ => Err(()),
        };

        Ok((result, remove_count))
    }

    fn replace_consumed_utxos(
        &self,
        db: &sled::Db,
        consumed_ring: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<(), crate::Error> {
        let mut insert_batch = sled::Batch::default();
        let mut remove_batch = sled::Batch::default();

        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        for key in keys.iter().rev() {
            if let Ok(Some(existing_value)) =
                self.get_removed(consumed_ring, key.to_string().as_bytes())
            {
                insert_batch.insert(key.to_string().as_bytes(), existing_value);
                remove_batch.remove(key.to_string().as_bytes());
            }
        }

        let result = match (
            db.apply_batch(insert_batch),
            consumed_ring.apply_batch(remove_batch),
        ) {
            (Ok(_), Ok(_)) => Ok(()),
            _ => Err(crate::Error::storage("failed to roll back consumed utxos")),
        };

        result
    }
}

#[derive(Stage)]
#[stage(
    name = "enrich-sled",
    unit = "RawBlockPayload",
    worker = "Worker" // todo: should be fine
)]
pub struct Stage {
    pub config: Config,
    pub ctx: Arc<Context>,

    pub input: InputPort<RawBlockPayload>,
    pub output: OutputPort<EnrichedBlockPayload>,

    #[metric]
    pub enrich_inserts: gasket::metrics::Counter,
    #[metric]
    pub enrich_blocks: gasket::metrics::Counter,
    #[metric]
    pub enrich_removes: gasket::metrics::Counter,
    #[metric]
    pub enrich_matches: gasket::metrics::Counter,
    #[metric]
    pub enrich_mismatches: gasket::metrics::Counter,
    #[metric]
    pub enrich_cancelled_empty_tx: gasket::metrics::Counter,
    #[metric]
    pub enrich_transactions: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let enrich_config = sled::Config::default()
            .path(
                stage
                    .config
                    .clone()
                    .db_path
                    .unwrap_or("/etc/era/enrich_main".to_string()),
            )
            .cache_capacity(1073741824);

        let rollback_config = sled::Config::default()
            .path(
                stage
                    .config
                    .clone()
                    .rollback_db_path
                    .unwrap_or("/etc/era/enrich_rollbacks".to_string()),
            )
            .cache_capacity(1073741824);

        Ok(Worker {
            enrich_db: Some(enrich_config.open().or_panic()?),
            rollback_db: Some(rollback_config.open().or_panic()?),
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<RawBlockPayload>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(
        &mut self,
        unit: &RawBlockPayload,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        match self.db_refs_all() {
            Ok(db_refs) => match db_refs {
                Some((db, consumed_ring)) => match unit {
                    model::RawBlockPayload::Genesis => {
                        let all = genesis_utxos(&stage.ctx.genesis_file).clone();

                        for utxo in all {
                            self.insert_genesis_utxo(&db, &utxo, &stage.enrich_inserts)
                                .map_err(crate::Error::storage)
                                .or_panic()?;
                        }

                        stage
                            .output
                            .send(
                                EnrichedBlockPayload::Genesis(genesis_utxos(
                                    &stage.ctx.genesis_file,
                                ))
                                .into(),
                            )
                            .await
                            .or_panic()?;

                        Ok(())
                    }

                    model::RawBlockPayload::Forward(cbor) => {
                        let block = MultiEraBlock::decode(&cbor)
                            .map_err(crate::Error::cbor)
                            .or_panic()?;

                        let txs = block.txs();

                        let (ctx, match_count, mismatch_count) =
                            self.par_fetch_referenced_utxos(db, block.number(), &txs);

                        stage.enrich_matches.inc(match_count);
                        stage.enrich_mismatches.inc(mismatch_count);

                        let (_, removed_count) =
                            self.remove_consumed_utxos(db, consumed_ring, &txs).unwrap(); // not handling error, todo

                        stage.enrich_removes.inc(removed_count);

                        stage.enrich_blocks.inc(1);
                        let generated_ctx = Some(ctx);

                        self.insert_produced_utxos(db, &txs, &stage.enrich_inserts)
                            .map_err(crate::Error::storage)
                            .or_panic()?;

                        stage
                            .output
                            .send(
                                model::EnrichedBlockPayload::Forward(
                                    cbor.clone(),
                                    generated_ctx.unwrap_or_default(),
                                    None,
                                )
                                .into(),
                            )
                            .await
                            .or_panic()
                    }

                    model::RawBlockPayload::Rollback(blocks) => {
                        log::warn!("rolling back in enrich {:?}", blocks);

                        for (i, raw_block) in blocks.iter().enumerate() {
                            let next_block_for_rollback = blocks.get(i + 1);

                            match MultiEraBlock::decode(&raw_block) {
                                Ok(block) => {
                                    let txs = block.txs();

                                    self.replace_consumed_utxos(db, consumed_ring, &txs)
                                        .or_panic()?;

                                    let (ctx, match_count, mismatch_count) =
                                        self.par_fetch_referenced_utxos(db, block.number(), &txs); // todo: RED FLAG .. NEED TO THINK THRU THE BLOCK NUMBER HERE AND RELATED STUFF

                                    stage.enrich_matches.inc(match_count);
                                    stage.enrich_mismatches.inc(mismatch_count);

                                    // Revert Anything to do with this block from consumed ring
                                    self.remove_produced_utxos(db, &txs, &stage.enrich_removes)
                                        .map_err(crate::Error::storage)
                                        .or_panic()?;

                                    let latest_block =
                                        stage.ctx.block_buffer.get_block_latest().unwrap();

                                    let multi_era_latest = MultiEraBlock::decode(
                                        next_block_for_rollback.unwrap_or(&latest_block),
                                    )
                                    .unwrap();

                                    // Could probably delete rolled back blocks from buffer, but also probably doesn't matter
                                    stage
                                        .output
                                        .send(
                                            model::EnrichedBlockPayload::rollback(
                                                raw_block.clone(),
                                                ctx,
                                                Point::Specific(
                                                    multi_era_latest.slot(),
                                                    multi_era_latest.hash().to_vec(),
                                                ),
                                            )
                                            .into(),
                                        )
                                        .await
                                        .or_panic()
                                }

                                Err(_) => Err(WorkerError::Panic),
                            }?;
                        }

                        stage.enrich_blocks.inc(1);

                        Ok(())
                    }
                },

                None => Err(WorkerError::Retry),
            },

            Err(_) => Err(WorkerError::Retry),
        }
    }
}
