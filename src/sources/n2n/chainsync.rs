use std::sync::Arc;

use crate::prelude::GasketStage;
use gasket::messaging::OutputPort;
use gasket_log::{debug, info, warn};
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas::network::miniprotocols::Point;

use gasket::framework::*;

use crate::model::RawBlockPayload;
use crate::pipeline::Context;
use crate::{crosscut, sources, storage, Error};

fn to_traverse<'b>(header: &'b HeaderContent) -> Result<MultiEraHeader<'b>, Error> {
    MultiEraHeader::decode(
        header.variant,
        header.byron_prefix.map(|x| x.0),
        &header.cbor,
    )
    .map_err(Error::cbor)
}

pub struct Worker {
    min_depth: usize,
    peer: Option<PeerClient>,
    byron_genesis_sent: bool,
    is_origin_start: bool,
}

impl Worker {}

#[derive(Stage)]
#[stage(name = "sources-n2n", unit = "RawBlockPayload", worker = "Worker")]
pub struct Stage {
    pub config: sources::n2n::Config,
    pub cursor: storage::Cursor,
    pub ctx: Arc<Context>,

    pub output: OutputPort<RawBlockPayload>,

    #[metric]
    pub chain_tip: gasket::metrics::Gauge,

    #[metric]
    pub historic_blocks_removed: gasket::metrics::Counter,

    #[metric]
    pub blocks_ingested: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("[{}] Bootstrapping", stage.name());

        let peer_session =
            PeerClient::connect(&stage.config.address, stage.ctx.chain.magic.clone())
                .await
                .or_retry()?;

        let mut worker = Self {
            min_depth: stage.config.min_depth.unwrap_or(10 as usize),
            peer: Some(peer_session),
            is_origin_start: false,
            byron_genesis_sent: false,
        };

        let peer = worker.peer.as_mut().unwrap();

        let ctx = Arc::clone(&stage.ctx);

        match stage
            .cursor
            .clone()
            .last_point()
            .map_err(|_| WorkerError::Retry)?
        {
            Some(x) => {
                info!(
                    "[{}] found existing cursor in storage plugin: {:?}",
                    stage.name(),
                    x
                );
                let point: Point = x.try_into().unwrap();
                info!("chainsync: cursor as i see it {:?}", point);
                peer.chainsync
                    .find_intersect(vec![point])
                    .await
                    .map(|_point| Ok(worker))
                    .map_err(|_| WorkerError::Retry)?
            }
            None => match ctx.as_ref().intersect.clone().unwrap() {
                crosscut::IntersectConfig::Origin => {
                    worker.is_origin_start = true;

                    peer.chainsync
                        .intersect_origin()
                        .await
                        .map(|_point| Ok(worker))
                        .map_err(|_| WorkerError::Retry)?
                }
                crosscut::IntersectConfig::Tip => {
                    info!("chainsync: at tip");
                    peer.chainsync
                        .intersect_tip()
                        .await
                        .map(|_point| Ok(worker))
                        .map_err(|_| WorkerError::Retry)?
                }
                crosscut::IntersectConfig::Point(_, _) => {
                    info!("chainsync: at point");
                    let point = stage
                        .ctx
                        .as_ref()
                        .intersect
                        .clone()
                        .unwrap()
                        .get_point()
                        .expect("point value");
                    peer.chainsync
                        .find_intersect(vec![point.clone()])
                        .await
                        .map(|_point| Ok(worker))
                        .map_err(|_| WorkerError::Retry)?
                }
                crosscut::IntersectConfig::Fallbacks(_) => {
                    info!("chainsync: at fallbacks");
                    let points = &stage
                        .ctx
                        .intersect
                        .clone()
                        .unwrap()
                        .get_fallbacks()
                        .expect("fallback values");
                    peer.chainsync
                        .find_intersect(points.clone())
                        .await
                        .map(|_point| Ok(worker))
                        .map_err(|_| WorkerError::Retry)?
                }
            },
        }
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<RawBlockPayload>, WorkerError> {
        if self.is_origin_start && !self.byron_genesis_sent {
            self.byron_genesis_sent = true;
            warn!("[{}] scheduling genesis transactions", stage.name());

            return Ok(WorkSchedule::Unit(RawBlockPayload::Genesis));
        }

        let peer = self.peer.as_mut().unwrap();

        match peer.chainsync.has_agency() {
            true => match peer.chainsync.request_next().await.or_restart() {
                Ok(next) => match next {
                    NextResponse::RollForward(h, t) => {
                        stage.chain_tip.set(t.1 as i64);
                        let parsed_headers = to_traverse(&h);

                        if let Ok(parsed_headers) = parsed_headers {
                            match peer
                                .blockfetch
                                .fetch_single(Point::Specific(
                                    parsed_headers.slot(),
                                    parsed_headers.hash().to_vec(),
                                ))
                                .await
                            {
                                Ok(static_single) => {
                                    stage.blocks_ingested.inc(1);
                                    Ok(WorkSchedule::Unit(RawBlockPayload::Forward(static_single)))
                                }
                                Err(_) => {
                                    warn!("big problem");
                                    Ok(WorkSchedule::Idle)
                                }
                            }
                        } else {
                            warn!("big problem");
                            Ok(WorkSchedule::Idle)
                        }
                    }

                    NextResponse::RollBackward(p, t) => {
                        stage.chain_tip.set(t.1 as i64);
                        let mut blocks: Vec<Vec<u8>> = Default::default();

                        stage.ctx.block_buffer.enqueue_rollback_batch(&p);

                        while let Ok(Some(block)) = stage.ctx.block_buffer.rollback_pop() {
                            blocks.push(block);
                        }

                        Ok(WorkSchedule::Unit(RawBlockPayload::Rollback(blocks)))
                    }

                    NextResponse::Await => Ok(WorkSchedule::Idle),
                },
                Err(_) => {
                    warn!("got no response");
                    Ok(WorkSchedule::Idle)
                }
            },
            false => match peer.chainsync.recv_while_must_reply().await.or_restart() {
                Ok(n) => match n {
                    NextResponse::RollForward(h, t) => {
                        warn!("chainsync (no agency): got block {:?}", t);

                        stage.chain_tip.set(t.1 as i64);
                        let parsed_headers = to_traverse(&h);

                        if let Ok(parsed_headers) = parsed_headers {
                            match peer
                                .blockfetch
                                .fetch_single(Point::Specific(
                                    parsed_headers.slot(),
                                    parsed_headers.hash().to_vec(),
                                ))
                                .await
                            {
                                Ok(static_single) => {
                                    Ok(WorkSchedule::Unit(RawBlockPayload::Forward(static_single)))
                                }
                                Err(_) => Ok(WorkSchedule::Idle),
                            }
                        } else {
                            Ok(WorkSchedule::Idle)
                        }
                    }

                    NextResponse::RollBackward(p, t) => {
                        stage.chain_tip.set(t.1 as i64);
                        let mut blocks: Vec<Vec<u8>> = Default::default();

                        stage.ctx.block_buffer.enqueue_rollback_batch(&p);

                        while let Ok(Some(block)) = stage.ctx.block_buffer.rollback_pop() {
                            blocks.push(block);
                        }

                        Ok(WorkSchedule::Unit(RawBlockPayload::Rollback(blocks)))
                    }

                    NextResponse::Await => Ok(WorkSchedule::Idle),
                },
                Err(_) => Ok(WorkSchedule::Idle),
            },
        }
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
