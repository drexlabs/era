use std::sync::Arc;

use async_trait;
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::{ledger::traverse::MultiEraBlock, network::miniprotocols::Point};
use tokio::sync::Mutex;

use crate::model::{CRDTCommand, EnrichedBlockPayload};
use crate::pipeline::Context;

use crate::{model, reducers};

use super::Reducer;

use gasket::framework::*;
use gasket::messaging::tokio::{connect_ports, InputPort, OutputPort};

pub struct _Config {
    _reducers: Vec<Reducer>,
} // todo use this like all the other gasket items

pub struct Worker {}

impl Worker {
    // todo cut down on clones here
    async fn reduce_block(
        &mut self,
        block_raw: Vec<u8>,
        genesis_utxos: Option<Vec<GenesisUtxo>>,
        byron_genesis_hash: Option<Hash<32>>,
        rollback: bool,
        block_ctx: Option<model::BlockContext>,
        last_good_block_rollback_info: Option<(Point, u64)>,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        reducers: &mut Vec<Reducer>,
        ops_count: &gasket::metrics::Counter,
        reducer_errors: &gasket::metrics::Counter,
    ) -> Result<(), WorkerError> {
        let mut point: Point = Point::Origin;

        let mut handles = Vec::new();

        match (
            !block_raw.is_empty(),
            byron_genesis_hash,
            genesis_utxos.to_owned(),
        ) {
            (true, None, None) => {
                match MultiEraBlock::decode(&block_raw) {
                    Ok(block_parsed) => {
                        point = match rollback {
                            true => {
                                let (last_point, _) =
                                    last_good_block_rollback_info.clone().unwrap();
                                last_point
                            }
                            false => {
                                Point::Specific(block_parsed.slot(), block_parsed.hash().to_vec())
                            }
                        };

                        output
                            .lock()
                            .await
                            .send(
                                model::CRDTCommand::block_starting(
                                    Some(block_parsed.clone()),
                                    None,
                                )
                                .into(),
                            )
                            .await
                            .map_err(|_| WorkerError::Send)?;

                        for reducer in reducers {
                            handles.push(reducer.reduce_block(
                                Some(block_parsed.clone()),
                                block_ctx.clone(),
                                genesis_utxos.clone(),
                                byron_genesis_hash.clone(),
                                rollback.clone(),
                                output.clone(),
                            ));
                        }

                        let results = futures::future::join_all(handles).await;

                        let mut n = 0;

                        for res in results {
                            match res {
                                Ok(_) => {
                                    n += 1;
                                    ops_count.inc(1);
                                }
                                Err(e) => {
                                    log::warn!("reducer error! {:?} {}", e, n); // todo panic here and interrupt
                                    reducer_errors.inc(1);
                                    n += 1;
                                }
                            };
                        }

                        Ok(())
                    }

                    Err(_) => Err(gasket::framework::WorkerError::Panic),
                }
            }

            (false, Some(byron_genesis_hash), Some(_)) => {
                point = Point::Specific(0, byron_genesis_hash.to_vec());

                output
                    .lock()
                    .await
                    .send(
                        model::CRDTCommand::block_starting(None, Some(byron_genesis_hash.clone()))
                            .into(),
                    )
                    .await
                    .map_err(|_| WorkerError::Send)?;

                for reducer in reducers {
                    handles.push(reducer.reduce_block(
                        None,
                        None,
                        genesis_utxos.clone(),
                        Some(byron_genesis_hash.clone()),
                        rollback.clone(),
                        output.clone(),
                    ));
                }

                let results = futures::future::join_all(handles).await;

                let mut n = 0;

                for res in results {
                    match res {
                        Ok(_) => {
                            n += 1;
                            ops_count.inc(1);
                        }
                        Err(e) => {
                            log::warn!("reducer error! {:?} {}", e, n); // todo panic here and interrupt
                            reducer_errors.inc(1);
                            n += 1;
                        }
                    };
                }

                Ok(())
            }

            _ => Err(gasket::framework::WorkerError::Panic),
        }?;

        output
            .lock()
            .await
            .send(
                model::CRDTCommand::block_finished(
                    point.clone(),
                    match block_raw.is_empty() {
                        true => None,
                        false => Some(block_raw.clone()),
                    },
                    rollback,
                )
                .into(),
            )
            .await
            .map_err(|_| WorkerError::Send)
    }
}

pub fn bootstrap(
    ctx: Arc<Mutex<Context>>,
    reducers: Vec<reducers::Config>,
    storage_input: &mut InputPort<CRDTCommand>,
) -> Stage {
    let mut output: OutputPort<CRDTCommand> = Default::default();
    connect_ports(&mut output, storage_input, 100);

    let stage = Stage {
        ctx: ctx.clone(),
        reducers: reducers
            .into_iter()
            .map(|x| x.bootstrapper(ctx.clone()))
            .collect(),
        input: Default::default(),
        output: Arc::new(Mutex::new(output)),
        ops_count: Default::default(),
        reducer_errors: Default::default(),
    };

    stage
}

#[derive(Stage)]
#[stage(
    name = "n2n-reducers",
    unit = "EnrichedBlockPayload",
    worker = "Worker"
)]
pub struct Stage {
    reducers: Vec<Reducer>,
    ctx: Arc<Mutex<Context>>,

    pub input: InputPort<EnrichedBlockPayload>,
    pub output: Arc<Mutex<OutputPort<CRDTCommand>>>,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    reducer_errors: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<EnrichedBlockPayload>, WorkerError> {
        match stage.input.recv().await {
            Ok(c) => Ok(WorkSchedule::Unit(c.payload)),
            Err(_) => Err(WorkerError::Retry),
        }
    }

    async fn execute(
        &mut self,
        unit: &EnrichedBlockPayload,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        match unit {
            model::EnrichedBlockPayload::RollForward(block, ctx) => self.reduce_block(
                block.clone(),
                None,
                None,
                false,
                Some(ctx.clone()),
                None,
                stage.output.clone(),
                &mut stage.reducers,
                &stage.ops_count,
                &stage.reducer_errors,
            ),

            model::EnrichedBlockPayload::RollBack(block, ctx, last_block_rollback_info) => self
                .reduce_block(
                    block.clone(),
                    None,
                    None,
                    false,
                    Some(ctx.clone()),
                    Some(last_block_rollback_info.clone()),
                    stage.output.clone(),
                    &mut stage.reducers,
                    &stage.ops_count,
                    &stage.reducer_errors,
                ),

            model::EnrichedBlockPayload::RollForwardGenesis(utxos) => self.reduce_block(
                vec![],
                Some(utxos.clone()),
                Some(Hash::<32>::new(
                    hex::decode(stage.ctx.lock().await.chain.byron_known_hash.clone())
                        .unwrap()
                        .try_into()
                        .expect("invalid byron block hash"),
                )),
                false,
                None,
                None,
                stage.output.clone(),
                &mut stage.reducers,
                &stage.ops_count,
                &stage.reducer_errors,
            ),
        }
        .await
    }
}
