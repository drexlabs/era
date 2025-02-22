use gasket::framework::{AsWorkError, WorkSchedule, WorkerError};
use gasket::messaging::{InputPort, OutputPort};
use gasket::runtime::Policy;
use gasket_log::model::Log;

use crate::model::ProgramOutput;

#[derive(gasket::framework::Stage)]
#[stage(name = "log-to-program-output", unit = "Vec<Log>", worker = "Worker")]
pub struct Stage {
    pub input: InputPort<Vec<Log>>,
    pub output: OutputPort<ProgramOutput>,
}

impl Stage {
    pub fn new() -> Self {
        Self {
            input: Default::default(),
            output: Default::default(),
        }
    }

    pub fn input(&mut self) -> &mut InputPort<Vec<Log>> {
        &mut self.input
    }

    pub fn output(&mut self) -> &mut OutputPort<ProgramOutput> {
        &mut self.output
    }

    pub fn spawn(self, policy: Policy) -> gasket::runtime::Tether {
        gasket::runtime::spawn_stage(self, policy)
    }
}

pub struct Worker;

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self)
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<Vec<Log>>, WorkerError> {
        Ok(match stage.input.recv().await {
            Ok(logs) => WorkSchedule::Unit(logs.payload),
            Err(_) => WorkSchedule::Idle,
        })
    }

    async fn execute(&mut self, logs: &Vec<Log>, stage: &mut Stage) -> Result<(), WorkerError> {
        let program_output = ProgramOutput::Log(logs.clone());

        stage
            .output
            .send(program_output.into())
            .await
            .map_err(|_| WorkerError::Send)
    }
}
