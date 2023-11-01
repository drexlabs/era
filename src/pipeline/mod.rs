pub mod console;

use std::sync::Arc;

use crate::{crosscut, enrich, reducers, sources, storage};

use gasket::{
    framework::*,
    messaging::tokio::connect_ports,
    retries,
    runtime::{spawn_stage, Policy, StagePhase, Tether, TetherState},
};
use pallas::ledger::{configs::byron::GenesisFile, traverse::wellknown::GenesisValues};
use tokio::sync::Mutex;

pub enum StageTypes {
    Source,
    Enrich,
    Reduce,
    Storage,
    Unknown,
}

impl std::convert::From<&str> for StageTypes {
    fn from(item: &str) -> Self {
        if item.contains("source") {
            return StageTypes::Source;
        }

        if item.contains("enrich") {
            return StageTypes::Enrich;
        }

        if item.contains("reduce") {
            return StageTypes::Reduce;
        }

        if item.contains("storage") {
            return StageTypes::Storage;
        }

        StageTypes::Unknown
    }
}

#[derive(Stage)]
#[stage(name = "pipeline-bootstrapper", unit = "()", worker = "Pipeline")]
pub struct Stage {
    pub ctx: Arc<Mutex<Context>>,
    pub sources_config: Option<sources::Config>,
    pub enrich_config: Option<enrich::Config>,
    pub reducer_config: Option<Vec<reducers::Config>>,
    pub storage_config: Option<storage::Config>,
    pub args_console: Option<console::Mode>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Pipeline {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        console::initialize(Some(stage.args_console.clone().unwrap())).await;

        let mut pipe = Self {
            policy: Policy {
                tick_timeout: None,
                bootstrap_retry: retries::Policy::default(),
                work_retry: retries::Policy::default(),
                teardown_retry: retries::Policy::default(),
            },

            tethers: Default::default(),
        };

        console::refresh(stage.ctx.clone(), &stage.args_console, &pipe).await?;

        let enrich = stage.enrich_config.clone().unwrap();

        let rollback_db_path = stage
            .ctx
            .lock()
            .await
            .block_buffer
            .config
            .rollback_db_path
            .clone();

        let mut enrich_stage = enrich
            .bootstrapper(stage.ctx.clone(), rollback_db_path)
            .unwrap();

        let enrich_input_port = enrich_stage.borrow_input_port();

        let storage = stage.storage_config.as_ref().unwrap();
        let mut storage_stage = storage.clone().bootstrapper(stage.ctx.clone()).unwrap();

        let source = stage.sources_config.as_ref().unwrap();
        let mut source_stage = source
            .clone()
            .bootstrapper(stage.ctx.clone(), storage_stage.build_cursor())
            .unwrap();

        let mut reducer = reducers::worker::bootstrap(
            stage.ctx.clone(),
            stage.reducer_config.clone().unwrap(),
            storage_stage.borrow_input_port(),
        );

        connect_ports(source_stage.borrow_output_port(), enrich_input_port, 100);
        connect_ports(enrich_stage.borrow_output_port(), &mut reducer.input, 100);

        pipe.tethers.push(storage_stage.spawn_stage(&pipe));
        pipe.tethers.push(spawn_stage(reducer, pipe.policy.clone()));
        pipe.tethers.push(enrich_stage.spawn_stage(&pipe));

        let mut startup_error = false;

        loop {
            let mut bootstrapping_consumers = false;
            for tether in &pipe.tethers {
                match tether.check_state() {
                    TetherState::Blocked(_) => {
                        bootstrapping_consumers = true;
                    }
                    TetherState::Dropped => {
                        startup_error = true;
                        break;
                    }
                    TetherState::Alive(s) => match s {
                        StagePhase::Bootstrap => {
                            bootstrapping_consumers = true;
                        }
                        StagePhase::Teardown => {
                            startup_error = true;
                        }
                        StagePhase::Ended => {
                            startup_error = true;
                        }
                        _ => {}
                    },
                }
            }

            if startup_error {
                return Err(WorkerError::Panic);
            }

            if !bootstrapping_consumers {
                break;
            }
        }

        pipe.tethers.push(source_stage.spawn_stage(&pipe));

        return Ok(pipe);
    }

    async fn schedule(&mut self, _: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        Ok(WorkSchedule::Unit(()))
    }

    async fn execute(&mut self, _: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        console::refresh(stage.ctx.clone(), &stage.args_console, self).await
    }
}

pub struct Pipeline {
    pub policy: Policy,
    pub tethers: Vec<Tether>,
}

pub fn i64_to_string(mut i: i64) -> String {
    let mut bytes = Vec::new();

    while i != 0 {
        bytes.push((i & 0xFF) as u8);
        i >>= 8;
    }

    let s = std::string::String::from_utf8(bytes).unwrap();

    s.chars().rev().collect::<String>()
}

impl Pipeline {
    pub fn bootstrap(
        ctx: Arc<Mutex<Context>>,
        sources_config: sources::Config,
        enrich_config: enrich::Config,
        reducer_config: Vec<reducers::Config>,
        storage_config: storage::Config,
        args_console: console::Mode,
    ) -> Stage {
        Stage {
            ctx,
            sources_config: Some(sources_config),
            storage_config: Some(storage_config),
            enrich_config: Some(enrich_config),
            reducer_config: Some(reducer_config),
            args_console: Some(args_console),
        }
    }

    pub fn should_stop(&mut self) -> bool {
        self.tethers
            .iter()
            .any(|tether| match tether.check_state() {
                gasket::runtime::TetherState::Alive(_) => false,
                _ => true,
            })
    }

    pub fn shutdown(self) {
        for tether in self.tethers {
            let state = tether.check_state();
            log::warn!("dismissing stage: {} with state {:?}", tether.name(), state);
            tether.dismiss_stage().expect("stage stops");

            // Can't join the stage because there's a risk of deadlock, usually
            // because a stage gets stuck sending into a port which depends on a
            // different stage not yet dismissed. The solution is to either create a
            // DAG of dependencies and dismiss in the correct order, or implement a
            // 2-phase teardown where ports are disconnected and flushed
            // before joining the stage.

            //tether.join_stage();
        }
    }
}

pub struct Context {
    pub chain: GenesisValues,
    pub intersect: crosscut::IntersectConfig,
    pub finalize: Option<crosscut::FinalizeConfig>,
    pub block_buffer: crosscut::historic::BufferBlocks,
    pub error_policy: crosscut::policies::RuntimePolicy,
    pub genesis_file: GenesisFile,
}
