use gasket::framework::*;
use gasket::messaging::tokio::{InputPort, OutputPort};

use crate::model::{BlockContext, EnrichedBlockPayload, RawBlockPayload};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Config {}

impl Config {
    pub fn bootstrapper(self) -> Stage {
        Stage {
            _config: self,
            input: Default::default(),
            output: Default::default(),
            ops_count: Default::default(),
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
            RawBlockPayload::RollForward(cbor) => {
                stage
                    .output
                    .send(EnrichedBlockPayload::roll_forward(
                        cbor.clone(),
                        BlockContext::default(),
                    ))
                    .await
                    .unwrap();
            }
            RawBlockPayload::RollBack(cbor, last_good_block_info_rollback) => {
                stage
                    .output
                    .send(EnrichedBlockPayload::roll_back(
                        cbor.clone(),
                        BlockContext::default(),
                        last_good_block_info_rollback.clone(),
                    ))
                    .await
                    .unwrap();
            }
            RawBlockPayload::RollForwardGenesis => {}
        };

        Ok(())
    }
}
