use gasket::{
    framework::*,
    messaging::{InputPort, OutputPort},
};
use pallas::network::miniprotocols::Point;

use crate::model::{BlockContext, EnrichedBlockPayload, RawBlockPayload};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Config {}

impl Config {
    pub fn bootstrapper(self) -> Stage {
        Stage {
            _config: self,
            ops_count: Default::default(),
            input: Default::default(),
            output: Default::default(),
        }
    }
}

#[derive(Stage)]
#[stage(name = "enrich-skip", unit = "RawBlockPayload", worker = "Worker")]
pub struct Stage {
    _config: Config,
    pub input: InputPort<RawBlockPayload>,
    pub output: OutputPort<EnrichedBlockPayload>,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

pub struct Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
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
        match unit {
            RawBlockPayload::Forward(cbor) => stage
                .output
                .send(
                    EnrichedBlockPayload::Forward(
                        cbor.clone(),
                        BlockContext::default(),
                        Some((Point::new(0, Default::default()), 0)),
                    )
                    .into(),
                )
                .await
                .map_err(|e| WorkerError::Send),
            RawBlockPayload::Rollback(cbor, last_good_block_info_rollback) => stage
                .output
                .send(
                    EnrichedBlockPayload::Rollback(
                        cbor.clone(),
                        BlockContext::default(),
                        Some(last_good_block_info_rollback.clone()),
                    )
                    .into(),
                )
                .await
                .map_err(|e| WorkerError::Send),
            RawBlockPayload::Genesis => stage
                .output
                .send(EnrichedBlockPayload::Genesis(Default::default()).into())
                .await
                .map_err(|e| WorkerError::Send), // todo: send genesis?
        }
    }
}
