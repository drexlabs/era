use futures::future::join_all;
use gasket::messaging::tokio::funnel_ports;
use gasket::runtime::{Policy, Tether};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::Era;
use std::sync::Arc;

use async_trait;
use gasket::messaging::{InputPort, OutputPort};
use pallas::network::miniprotocols::Point;
use serde::Deserialize;

use crate::model::{CRDTCommand, DecodedBlockAction, EnrichedBlockPayload};
use crate::pipeline::{Context, GASKET_CAP};

use super::Reducer;

use gasket::framework::*;

#[derive(Deserialize)]
pub struct Config(Vec<crate::reducers::Config>);

impl Config {
    pub fn bootstrapper<'a>(
        self,
        ctx: Arc<Context>,
        input: &mut InputPort<CRDTCommand>,
    ) -> Bootstrapper {
        let mut reducers: Vec<Reducer> = Default::default();
        let mut reducer_outputs: Vec<&mut OutputPort<CRDTCommand>> = Default::default();

        for reducer_config in self.0 {
            let r = reducer_config.bootstrapper(Arc::clone(&ctx));
            reducers.push(r);
        }

        for reducer in &mut reducers {
            reducer_outputs.push(reducer.borrow_output_port());
        }

        let mut output: OutputPort<CRDTCommand> = Default::default();

        reducer_outputs.push(&mut output);

        funnel_ports(reducer_outputs, input, GASKET_CAP);

        Bootstrapper {
            stage: Stage {
                reducers,
                output,
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
        let raw_block: Option<Vec<u8>> = None;

        let reducer_block: Arc<DecodedBlockAction> = Arc::new(unit.into());

        let worker_orchestrator_block_ref = reducer_block.as_ref();

        stage
            .output
            .send(CRDTCommand::block_starting(worker_orchestrator_block_ref).into())
            .await
            .map_err(|_| WorkerError::Send)?;

        if worker_orchestrator_block_ref.parsed_block().is_some()
            && worker_orchestrator_block_ref
                .parsed_block()
                .unwrap()
                .tx_count()
                > 0
        {
            let reducer_futures = stage
                .reducers
                .iter_mut()
                .map(|reducer| {
                    let reducer_block = Arc::clone(&reducer_block);
                    reducer.reduce_block(reducer_block)
                })
                .collect::<Vec<_>>();

            join_all(reducer_futures)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .or_panic()?;
        }

        stage
            .output
            .send(
                CRDTCommand::block_finished(
                    worker_orchestrator_block_ref.block_rolling_to_point(),
                    raw_block,
                    worker_orchestrator_block_ref.block_era() as i64,
                    worker_orchestrator_block_ref.block_tx_count() as u64,
                    worker_orchestrator_block_ref.block_number(),
                    match unit {
                        EnrichedBlockPayload::Rollback(_, _, _) => true,
                        _ => false,
                    },
                    match unit {
                        EnrichedBlockPayload::Genesis(_) => true,
                        _ => false,
                    },
                )
                .into(),
            )
            .await
            .map_err(|_| WorkerError::Send)
    }
}
