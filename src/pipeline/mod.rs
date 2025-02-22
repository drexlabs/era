pub mod console;
pub mod logs;

use crate::prelude::GasketStage;

use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::sync::Arc;

use crate::{crosscut, model::ConfigRoot};

use crate::model::{merge_metrics_snapshots, MeteredValue, MetricsSnapshot, ProgramOutput};
use gasket::messaging::tokio::broadcast_port;
use gasket::messaging::OutputPort;
use gasket::metrics::Reading;
use gasket::runtime::spawn_stage;
use gasket::{
    framework::*,
    messaging::tokio::connect_ports,
    runtime::{Policy, StagePhase, Tether, TetherState},
};
use gasket_log::{debug, warn, InputPort};
use pallas::ledger::{configs::byron::GenesisFile, traverse::wellknown::GenesisValues};
use serde::Deserialize;

pub static GASKET_CAP: usize = 10000;

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

pub struct Bootstrapper {
    pub stage: Stage,
}

impl Bootstrapper {
    pub fn spawn_stage(self, policy: Policy) -> gasket::runtime::Tether {
        gasket::runtime::spawn_stage(self.stage, policy)
    }
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub display_mode: Option<console::Mode>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            display_mode: Some(console::Mode::Plain),
        }
    }
}

impl Config {
    pub fn bootstrapper(self, root_config: RefCell<ConfigRoot>, ctx: Arc<Context>) -> Bootstrapper {
        Bootstrapper {
            stage: Stage {
                output: Default::default(),
                config: root_config,
                stage_config: self,
                ctx,
            },
        }
    }
}

#[derive(Stage)]
#[stage(
    name = "pipeline-bootstrapper",
    unit = "Vec<MetricsSnapshot>",
    worker = "Pipeline"
)]
pub struct Stage {
    pub config: RefCell<ConfigRoot>,
    pub output: OutputPort<ProgramOutput>,
    pub stage_config: Config,
    pub ctx: Arc<Context>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Pipeline {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("[{}] Bootstrapping", stage.name());
        let mut config = stage.config.borrow_mut();

        let mut pipe = Self {
            policy: config.gasket_policy(),
            tethers: Default::default(),
            tether_states: Default::default(),
            chain_config: Arc::new(config.chain.take().unwrap_or_default()),
        };

        let enrich = config.enrich.take().unwrap_or_default();
        let mut enrich_stage = enrich.bootstrapper(Arc::clone(&stage.ctx));

        let mut storage_stage = config
            .storage
            .take()
            .unwrap()
            .bootstrapper(Arc::clone(&stage.ctx));

        let mut source_stage = config
            .source
            .take()
            .unwrap()
            .bootstrapper(Arc::clone(&stage.ctx), storage_stage.cursor());

        let mut reducers_stage = config
            .reducers
            .take()
            .unwrap()
            .bootstrapper(Arc::clone(&stage.ctx), storage_stage.borrow_input_port());

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

        pipe.tethers
            .push(enrich_stage.spawn_stage(pipe.policy.clone()));

        pipe.tethers
            .push(source_stage.spawn_stage(pipe.policy.clone()));

        warn!("bootstrapped pipeline!!");
        return Ok(pipe);
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Vec<MetricsSnapshot>>, WorkerError> {
        let mut accum_snap = MetricsSnapshot {
            timestamp: None,
            chain_bar_depth: None,
            chain_bar_progress: None,
            blocks_processed: None,
            transactions: None,
            chain_era: None,
            sources_status: None,
            enrich_status: None,
            reducer_status: None,
            storage_status: None,
            enrich_hit: None,
            enrich_miss: None,
            blocks_ingested: None,
        };

        let accumulated_state: Vec<MetricsSnapshot> = self
            .tethers
            .iter()
            .map(|tether| {
                let snapshot_status_text = match tether.check_state() {
                    TetherState::Dropped => "dropped!",
                    TetherState::Blocked(_) => "blocked",
                    TetherState::Alive(a) => match a {
                        StagePhase::Bootstrap => "⚠",
                        StagePhase::Working => "⚙",
                        StagePhase::Teardown => "⚠",
                        StagePhase::Ended => "ended",
                    },
                };

                match tether.name() {
                    "source" => accum_snap
                        .sources_status
                        .replace(MeteredValue::Label(snapshot_status_text.into())),
                    "enrich" => accum_snap
                        .enrich_status
                        .replace(MeteredValue::Label(snapshot_status_text.into())),
                    "reduce" => accum_snap
                        .reducer_status
                        .replace(MeteredValue::Label(snapshot_status_text.into())),
                    "storage" => accum_snap
                        .storage_status
                        .replace(MeteredValue::Label(snapshot_status_text.into())),
                    _ => None,
                };

                tether.read_metrics().map(|readings| {
                    readings.iter().fold(
                        accum_snap.clone(),
                        |mut snapshot, (metric_key_name, reading)| {
                            let t_name = tether.name();

                            match (t_name, &**metric_key_name, reading) {
                                // todo: lmfao clearly the wrong approach is being used by me here. &**, ghastly.
                                (_, "chain_tip", Reading::Gauge(x)) => {
                                    let mut a = snapshot
                                        .chain_bar_depth
                                        .clone()
                                        .unwrap_or(MeteredValue::Numerical(Default::default()));
                                    a.set_num(x.clone() as u64);
                                    snapshot.chain_bar_depth.replace(a);

                                    snapshot
                                }
                                (_, "last_block", Reading::Gauge(x)) => {
                                    let mut a = snapshot
                                        .chain_bar_progress
                                        .clone()
                                        .unwrap_or(MeteredValue::Numerical(Default::default()));
                                    a.set_num(x.clone() as u64);
                                    snapshot.chain_bar_progress.replace(a);

                                    snapshot
                                }
                                (_, "blocks_processed", Reading::Count(x)) => {
                                    let mut a = snapshot
                                        .blocks_processed
                                        .clone()
                                        .unwrap_or(MeteredValue::Numerical(Default::default()));
                                    a.set_num(x.clone());
                                    snapshot.blocks_processed.replace(a);

                                    snapshot
                                }
                                // (_, "received_blocks", Reading::Count(x)) => {
                                //     self.received_blocks.set_position(x);
                                //     self.received_blocks.set_message(state);
                                // }
                                // (_, "ops_count", Reading::Count(x)) => {
                                //     self.reducer_ops_count.set_position(x);
                                //     self.reducer_ops_count.set_message(state);
                                // }
                                // (_, "reducer_errors", Reading::Count(x)) => {
                                //     self.reducer_errors.set_position(x);
                                //     self.reducer_errors.set_message(state);
                                // }
                                // (_, "storage_ops", Reading::Count(x)) => {
                                //     self.storage_ops_count.set_position(x);
                                //     self.storage_ops_count.set_message(state);
                                // }
                                (_, "transactions_finalized", Reading::Count(x)) => {
                                    let mut a = snapshot
                                        .transactions
                                        .clone()
                                        .unwrap_or(MeteredValue::Numerical(Default::default()));
                                    a.set_num(x.clone());
                                    snapshot.transactions.replace(a);

                                    snapshot
                                }
                                // (_, "enrich_cancelled_empty_tx", Reading::Count(x)) => {
                                //     self.enrich_skipped_empty.set_position(x);
                                //     self.enrich_skipped_empty.set_message(state);
                                // }
                                // (_, "enrich_removes", Reading::Count(x)) => {
                                //     snapshot.enrich_hit.set_num(x);
                                // }
                                (_, "enrich_matches", Reading::Count(x)) => {
                                    let mut a = snapshot
                                        .enrich_hit
                                        .clone()
                                        .unwrap_or(MeteredValue::Numerical(Default::default()));
                                    a.set_num(x.clone());
                                    snapshot.enrich_hit.replace(a);

                                    snapshot
                                }
                                (_, "blocks_ingested", Reading::Count(x)) => {
                                    let mut a = snapshot
                                        .blocks_ingested
                                        .clone()
                                        .unwrap_or(MeteredValue::Numerical(Default::default()));
                                    a.set_num(x.clone());

                                    snapshot.blocks_ingested.replace(a);

                                    snapshot
                                }
                                // (_, "enrich_blocks", Reading::Count(x)) => {
                                //     self.enrich_blocks.set_position(x);
                                //     self.enrich_blocks.set_message(state);
                                // }
                                // (_, "historic_blocks", Reading::Count(x)) => {
                                //     self.historic_blocks.set_position(x);
                                //     self.historic_blocks.set_message("");
                                // }
                                // (_, "historic_blocks_removed", Reading::Count(x)) => {
                                //     self.historic_blocks_removed.set_position(x);
                                //     self.historic_blocks_removed.set_message("");
                                // }
                                (_, "chain_era", Reading::Gauge(x)) => {
                                    if x.clone() > 0 {
                                        let mut a = snapshot
                                            .chain_era
                                            .clone()
                                            .unwrap_or(MeteredValue::Label(Default::default()));
                                        a.set_str(i64_to_string(x.clone()).as_str());

                                        snapshot.chain_era.replace(a);
                                    }

                                    snapshot
                                }
                                _ => snapshot,
                            }
                        },
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .or_retry()?;

        Ok(WorkSchedule::Unit(accumulated_state))
    }

    async fn execute(
        &mut self,
        unit: &Vec<MetricsSnapshot>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        match stage
            .config
            .borrow()
            .display
            .clone()
            .unwrap_or_default()
            .mode
        {
            console::Mode::TUI => {
                stage
                    .output
                    .borrow_mut()
                    .send(gasket::messaging::Message {
                        payload: ProgramOutput::Metrics(unit.clone()),
                    })
                    .await
                    .or_retry()?;
            }

            console::Mode::Plain => {}
        };

        Ok(())
    }
}

pub struct Pipeline {
    pub policy: Policy,
    pub tethers: Vec<Tether>,
    pub chain_config: Arc<crosscut::ChainConfig>,
    pub tether_states: Vec<TetherState>,
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
    pub fn wait_for_tethers(&mut self) {
        for tether in &self.tethers {
            tether.wait_state(TetherState::Alive(StagePhase::Working));
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
            warn!("dismissing stage: {} with state {:?}", tether.name(), state);
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
