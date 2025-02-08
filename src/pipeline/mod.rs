pub mod console;

use std::{cell::RefCell, sync::Arc};

use crate::{crosscut, model::ConfigRoot};

use gasket::{
    framework::*,
    messaging::tokio::connect_ports,
    runtime::{Policy, StagePhase, Tether, TetherState},
};
use pallas::ledger::{configs::byron::GenesisFile, traverse::wellknown::GenesisValues};

static GASKET_CAP: usize = 100;

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
    config: RefCell<ConfigRoot>,
    args_console: Arc<console::Mode>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Pipeline {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let mut config = stage.config.borrow_mut();

        console::initialize(stage.args_console.as_ref()).await;

        console::refresh(stage.args_console.as_ref(), None).await?;

        let mut pipe = Self {
            policy: config.take_gasket_policy(),
            tethers: Default::default(),
            chain_config: Arc::new(config.chain.take().unwrap_or_default()),
            ctx: config.take_some_to_make_context(),
        };

        console::refresh(stage.args_console.as_ref(), Some(&pipe)).await?;

        let enrich = config.enrich.take().unwrap_or_default();

        log::warn!("getting latest intersection (this may take awhile)");

        console::refresh(stage.args_console.as_ref(), Some(&pipe)).await?;

        let mut enrich_stage = enrich.bootstrapper(Arc::clone(&pipe.ctx));

        console::refresh(stage.args_console.as_ref(), Some(&pipe)).await?;

        let mut storage_stage = config
            .storage
            .take()
            .unwrap()
            .bootstrapper(Arc::clone(&pipe.ctx))
            .unwrap();

        let mut source_stage = config
            .source
            .take()
            .unwrap()
            .bootstrapper(Arc::clone(&pipe.ctx), storage_stage.build_cursor())
            .unwrap();

        let mut reducers_stage = config
            .reducers
            .take()
            .unwrap()
            .bootstrapper(Arc::clone(&pipe.ctx));

        connect_ports(
            source_stage.borrow_output_port(),
            enrich_stage.borrow_input_port(),
            GASKET_CAP,
        );

        connect_ports(
            enrich_stage.borrow_output_port(),
            reducers_stage.borrow_input_port(),
            GASKET_CAP,
        );

        connect_ports(
            reducers_stage.borrow_output_port(),
            storage_stage.borrow_input_port(),
            GASKET_CAP,
        );

        pipe.tethers
            .push(storage_stage.spawn_stage(pipe.policy.clone()));

        pipe.tethers
            .push(reducers_stage.spawn_stage(pipe.policy.clone()));

        let mut tether_error = false;

        log::warn!("pipeline: spawning tethers");

        loop {
            console::refresh(stage.args_console.as_ref(), Some(&pipe)).await?;

            let mut bootstrapping_consumers = false;
            for tether in &pipe.tethers {
                console::refresh(stage.args_console.as_ref(), Some(&pipe)).await?;
                match tether.check_state() {
                    TetherState::Blocked(_) => {
                        bootstrapping_consumers = true;
                    }
                    TetherState::Dropped => {
                        log::warn!("pipeline: dropped tether {}", tether.name());
                        tether_error = true;
                        break;
                    }
                    TetherState::Alive(s) => match s {
                        StagePhase::Bootstrap => {
                            bootstrapping_consumers = true;
                        }
                        StagePhase::Teardown => {
                            log::warn!("pipeline: tore down {}", tether.name());
                            tether_error = true;
                        }
                        StagePhase::Ended => {
                            log::warn!("pipeline: ended {}", tether.name());
                            tether_error = true;
                        }
                        StagePhase::Working => {
                            bootstrapping_consumers = false;
                        }
                    },
                }
            }

            if tether_error {
                log::warn!("pipeline: tether startup error");
                return Err(WorkerError::Panic);
            }

            if !bootstrapping_consumers {
                break;
            }
        }

        pipe.tethers
            .push(enrich_stage.spawn_stage(pipe.policy.clone()));

        log::warn!("pipeline: reached data source tether");
        pipe.tethers
            .push(source_stage.spawn_stage(pipe.policy.clone()));

        log::warn!("pipeline: all tethers are alive");

        return Ok(pipe);
    }

    async fn schedule(&mut self, _: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        Ok(WorkSchedule::Unit(()))
    }

    async fn execute(&mut self, _: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        console::refresh(&stage.args_console, Some(self)).await
    }
}

pub struct Pipeline {
    pub policy: Policy,
    pub tethers: Vec<Tether>,
    pub ctx: Arc<Context>,
    pub chain_config: Arc<crosscut::ChainConfig>,
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
    pub fn bootstrap_gasket_stage(
        config: RefCell<ConfigRoot>,
        args_console: Arc<console::Mode>,
    ) -> Stage {
        Stage {
            config,
            args_console,
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

            // Note: Below possibly isn't true anymore due to gasket-rs updates. need to investigate
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
    pub intersect: Option<crosscut::IntersectConfig>,
    pub finalize: Option<crosscut::FinalizeConfig>,
    pub block_buffer: crosscut::historic::BufferBlocks,
    pub genesis_file: GenesisFile,
}
