use std::sync::Arc;

use futures::TryFutureExt;
use gasket::messaging::OutputPort;
use gasket_log::{info, warn};
use pallas::ledger::configs::byron::genesis_utxos;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::NodeClient;
use pallas::network::miniprotocols::chainsync::NextResponse;
use pallas::network::miniprotocols::Point;

use gasket::framework::{Stage as StageTrait, *};

use crate::model::{EnrichedBlockPayload, RawBlockPayload};
use crate::pipeline::Context;
use crate::{crosscut, sources, storage};

pub struct Worker {
    min_depth: usize,
    peer: Option<NodeClient>,
    at_origin: bool,
}

impl Worker {}

#[derive(Stage)]
#[stage(name = "sources-n2c", unit = "RawBlockPayload", worker = "Worker")]
pub struct Stage {
    pub config: sources::n2c::Config,
    pub cursor: storage::Cursor,
    pub ctx: Arc<Context>,

    pub output: OutputPort<RawBlockPayload>,

    #[metric]
    pub chain_tip: gasket::metrics::Gauge,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let ctx = Arc::clone(&stage.ctx);

        warn!(
            "pipeline: attempting to connect! to the network {}",
            stage.name()
        );

        let peer_session = NodeClient::connect(&stage.config.path, stage.ctx.chain.magic.clone())
            .await
            .or_retry()?;

        let mut worker = Self {
            min_depth: stage.config.min_depth.unwrap_or_default(),
            peer: Some(peer_session),
            at_origin: false,
        };

        let peer = worker.peer.as_mut().unwrap();

        match stage.cursor.clone().last_point().unwrap() {
            Some(x) => {
                info!("found existing cursor in storage plugin: {:?}", x);
                let point: Point = x.try_into().unwrap();
                //stage.last_block.set(point.slot_or_default() as i64);
                peer.chainsync()
                    .find_intersect(vec![point])
                    .await
                    .or_panic()?;
            }
            None => match ctx.as_ref().intersect.clone().unwrap() {
                crosscut::IntersectConfig::Origin => {
                    worker.at_origin = true;

                    peer.chainsync().intersect_origin().await.or_panic()?;
                }
                crosscut::IntersectConfig::Tip => {
                    peer.chainsync().intersect_tip().await.or_panic()?;
                }
                crosscut::IntersectConfig::Point(_, _) => {
                    let point = &stage.ctx.intersect.clone().unwrap().get_point().unwrap();

                    peer.chainsync()
                        .find_intersect(vec![point.clone()])
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Fallbacks(_) => {
                    let points = &stage
                        .ctx
                        .intersect
                        .clone()
                        .unwrap()
                        .get_fallbacks()
                        .unwrap();

                    peer.chainsync()
                        .find_intersect(points.to_owned())
                        .await
                        .or_panic()?;
                }
            },
        }

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<RawBlockPayload>, WorkerError> {
        let peer = self.peer.as_mut().unwrap();

        Ok(match self.at_origin {
            true => {
                self.at_origin = false;
                WorkSchedule::Unit(RawBlockPayload::Genesis)
            }
            false => match peer.chainsync().has_agency() {
                true => match peer.chainsync().request_next().await.or_restart() {
                    Ok(next) => match next {
                        NextResponse::RollForward(cbor, t) => {
                            stage.chain_tip.set(t.1 as i64);
                            WorkSchedule::Unit(RawBlockPayload::Forward(cbor.0))
                        }

                        NextResponse::RollBackward(p, t) => {
                            let mut blocks: Vec<Vec<u8>> = Default::default();

                            stage.ctx.block_buffer.enqueue_rollback_batch(&p);

                            while let Ok(Some(block)) = stage.ctx.block_buffer.rollback_pop() {
                                blocks.push(block);
                            }

                            WorkSchedule::Unit(RawBlockPayload::Rollback(blocks))
                        }

                        NextResponse::Await => WorkSchedule::Idle,
                    },
                    Err(_) => WorkSchedule::Idle,
                },
                false => match peer.chainsync().recv_while_must_reply().await.or_restart() {
                    Ok(n) => match n {
                        NextResponse::RollForward(cbor, t) => {
                            stage.chain_tip.set(t.1 as i64);
                            WorkSchedule::Unit(RawBlockPayload::Forward(cbor.0))
                        }
                        NextResponse::RollBackward(p, t) => {
                            stage.chain_tip.set(t.1 as i64);
                            let mut blocks: Vec<Vec<u8>> = Default::default();

                            stage.ctx.block_buffer.enqueue_rollback_batch(&p);

                            while let Ok(Some(block)) = stage.ctx.block_buffer.rollback_pop() {
                                blocks.push(block);
                            }

                            WorkSchedule::Unit(RawBlockPayload::Rollback(blocks))
                        }
                        NextResponse::Await => WorkSchedule::Idle,
                    },
                    Err(_) => {
                        info!("ready for next block");
                        WorkSchedule::Idle
                    }
                },
            },
        })
    }

    async fn execute(
        &mut self,
        unit: &RawBlockPayload,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        stage
            .output
            .send(unit.clone().into())
            .await
            .map_err(|_| WorkerError::Send)
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        //self.peer.as_mut().unwrap().abort();

        Ok(())
    }
}
