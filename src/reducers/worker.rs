use futures::future::join_all;
use gasket::runtime::{Policy, Tether};
use std::sync::Arc;

use async_trait;
use gasket::messaging::{InputPort, OutputPort};
use pallas::network::miniprotocols::Point;
use serde::Deserialize;

use crate::model::{CRDTCommand, DecodedBlockAction, EnrichedBlockPayload};
use crate::pipeline::Context;

use super::Reducer;

use gasket::framework::*;

#[derive(Deserialize)]
pub struct Config(Vec<crate::reducers::Config>);

impl Config {
    pub fn bootstrapper<'a>(self, ctx: Arc<Context>) -> Bootstrapper {
        let mut reducers: Vec<Reducer> = Default::default();
        for reducer_config in self.0 {
            reducers.push(reducer_config.bootstrapper(Arc::clone(&ctx)));
        }

        Bootstrapper {
            stage: Stage {
                reducers,
                output: Default::default(),
                input: Default::default(),
                ops_count: Default::default(),
                reducer_errors: Default::default(),
            },
        }
    }
}

pub struct Bootstrapper {
    stage: Stage,
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<CRDTCommand> {
        &mut self.stage.output
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<EnrichedBlockPayload> {
        &mut self.stage.input
    }

    pub fn spawn_stage(self, policy: Policy) -> Tether {
        gasket::runtime::spawn_stage(self.stage, policy)
    }
}

pub struct Worker {}

impl Worker {}

#[derive(Stage)]
#[stage(
    name = "n2n-reducers",
    unit = "EnrichedBlockPayload",
    worker = "Worker"
)]
pub struct Stage {
    pub reducers: Vec<Reducer>,

    pub output: OutputPort<CRDTCommand>,
    pub input: InputPort<EnrichedBlockPayload>,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    reducer_errors: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
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
        let point: Option<Point>;

        match unit {
            EnrichedBlockPayload::Genesis(genesis_utxo) => {
                if let Some((byron_last_hash, _, _)) = genesis_utxo.last() {
                    point = Some(Point::Specific(0, byron_last_hash.to_vec()));
                }

                stage
                    .output
                    .send(
                        CRDTCommand::block_starting(
                            None,
                            Some(&pallas::crypto::hash::Hash::new([0; 32])),
                        )
                        .into(),
                    )
                    .await
                    .map_err(|r| WorkerError::Send)
            }

            action @ (EnrichedBlockPayload::Forward(raw, block_context, last_good)
            | EnrichedBlockPayload::Rollback(raw, block_context, last_good)) => {
                match action.decode_block() {
                    Some(parsed_block) => {
                        let rollback = match action {
                            EnrichedBlockPayload::Rollback(_, _, _) => true,
                            _ => false,
                        };

                        let reducer_block_ac: Arc<DecodedBlockAction> = Arc::new(action.into());
                        let parsed_block = Arc::new(parsed_block);

                        {
                            stage
                                .output
                                .send(
                                    CRDTCommand::block_starting(Some(parsed_block.as_ref()), None)
                                        .into(),
                                )
                                .await
                                .map_err(|_| WorkerError::Send)?;
                        }

                        let reducer_futures = stage
                            .reducers
                            .iter_mut()
                            .map(|reducer| {
                                let reducer_block = Arc::clone(&reducer_block_ac);
                                reducer.reduce_block(reducer_block)
                            })
                            .collect::<Vec<_>>();

                        join_all(reducer_futures)
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|_| WorkerError::Panic)?;

                        stage
                            .output
                            .send(
                                CRDTCommand::block_finished(
                                    Point::new(parsed_block.slot(), parsed_block.hash().to_vec()), // todo: confirm this is the correct point
                                    raw.clone(),
                                    parsed_block.era() as i64,
                                    parsed_block.tx_count() as u64,
                                    parsed_block.number() as i64,
                                    rollback,
                                )
                                .into(),
                            )
                            .await
                            .map_err(|_| WorkerError::Send)
                    }

                    None => Err(WorkerError::Panic),
                }
            }
        }
    }
}
