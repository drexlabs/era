use std::sync::Arc;

use gasket::messaging::tokio::OutputPort;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas::network::miniprotocols::Point;

use gasket::framework::*;
use tokio::sync::Mutex;

use crate::model::RawBlockPayload;
use crate::pipeline::Context;
use crate::{crosscut, sources, storage, Error};

use crate::prelude::*;

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
}

impl Worker {}

#[derive(Stage)]
#[stage(name = "sources-n2n", unit = "Vec<RawBlockPayload>", worker = "Worker")]
pub struct Stage {
    pub config: sources::n2n::Config,
    pub cursor: storage::Cursor,
    pub ctx: Arc<Mutex<Context>>,

    pub output: OutputPort<RawBlockPayload>,

    #[metric]
    pub chain_tip: gasket::metrics::Gauge,

    #[metric]
    pub historic_blocks_removed: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let peer_session = PeerClient::connect(
            &stage.config.address,
            stage.ctx.lock().await.chain.magic.clone(),
        )
        .await
        .unwrap();

        let mut worker = Self {
            min_depth: stage.config.min_depth.unwrap_or(10 as usize),
            peer: Some(peer_session),
        };

        let peer = worker.peer.as_mut().unwrap();

        match stage.cursor.clone().last_point().unwrap() {
            Some(x) => {
                log::info!("found existing cursor in storage plugin: {:?}", x);
                let point: Point = x.try_into().unwrap();
                peer.chainsync
                    .find_intersect(vec![point])
                    .await
                    .map_err(crate::Error::ouroboros)
                    .unwrap();
            }
            None => match &stage.ctx.lock().await.intersect {
                crosscut::IntersectConfig::Origin => {
                    peer.chainsync
                        .intersect_origin()
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Tip => {
                    peer.chainsync
                        .intersect_tip()
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Point(_, _) => {
                    let point = &stage
                        .ctx
                        .lock()
                        .await
                        .intersect
                        .get_point()
                        .expect("point value");
                    peer.chainsync
                        .find_intersect(vec![point.clone()])
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Fallbacks(_) => {
                    let points = &stage
                        .ctx
                        .lock()
                        .await
                        .intersect
                        .get_fallbacks()
                        .expect("fallback values");
                    peer.chainsync
                        .find_intersect(points.clone())
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
            },
        }

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Vec<RawBlockPayload>>, WorkerError> {
        let peer = self.peer.as_mut().unwrap();

        async fn flush_buffered_blocks(
            blocks_client: &mut crosscut::historic::BufferBlocks,
        ) -> Vec<RawBlockPayload> {
            match blocks_client.block_mem_take_all().to_owned() {
                Some(blocks) => blocks,
                None => vec![],
            }
        }

        Ok(WorkSchedule::Unit(match peer.chainsync.has_agency() {
            true => match peer.chainsync.request_next().await.or_restart() {
                Ok(next) => match next {
                    NextResponse::RollForward(h, t) => {
                        stage.chain_tip.set(t.1 as i64);
                        let parsed_headers = to_traverse(&h);
                        match parsed_headers {
                            Ok(parsed_headers) => {
                                match peer
                                    .blockfetch
                                    .fetch_single(Point::Specific(
                                        parsed_headers.slot(),
                                        parsed_headers.hash().to_vec(),
                                    ))
                                    .await
                                {
                                    Ok(static_single) => {
                                        stage.ctx.lock().await.block_buffer.block_mem_add(
                                            RawBlockPayload::RollForward(static_single),
                                        );

                                        match stage
                                            .ctx
                                            .lock()
                                            .await
                                            .block_buffer
                                            .block_mem_size()
                                            .to_owned()
                                            >= self.min_depth
                                        {
                                            true => {
                                                flush_buffered_blocks(
                                                    &mut stage.ctx.lock().await.block_buffer,
                                                )
                                                .await
                                            }

                                            false => {
                                                vec![]
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        flush_buffered_blocks(
                                            &mut stage.ctx.lock().await.block_buffer,
                                        )
                                        .await
                                    }
                                }
                            }
                            Err(_) => {
                                vec![]
                            }
                        }
                    }

                    NextResponse::RollBackward(p, t) => {
                        let mut blocks =
                            flush_buffered_blocks(&mut stage.ctx.lock().await.block_buffer).await;

                        stage
                            .ctx
                            .lock()
                            .await
                            .block_buffer
                            .enqueue_rollback_batch(&p);
                        stage.historic_blocks_removed.inc(1);

                        loop {
                            let pop_rollback_block =
                                stage.ctx.lock().await.block_buffer.rollback_pop();

                            if let Some(last_good_block) =
                                stage.ctx.lock().await.block_buffer.get_block_latest()
                            {
                                if let Ok(parsed_last_good_block) =
                                    MultiEraBlock::decode(&last_good_block)
                                {
                                    let last_good_point = Point::Specific(
                                        parsed_last_good_block.slot(),
                                        parsed_last_good_block.hash().to_vec(),
                                    );

                                    stage.chain_tip.set(parsed_last_good_block.slot() as i64);

                                    if let Some(rollback_cbor) = pop_rollback_block {
                                        blocks.push(RawBlockPayload::RollBack(
                                            rollback_cbor.clone(),
                                            (last_good_point, parsed_last_good_block.number()),
                                        ));
                                    } else {
                                        break;
                                    }
                                }
                            } else {
                                if let Some(rollback_cbor) = pop_rollback_block {
                                    stage.chain_tip.set(0 as i64);

                                    blocks.push(RawBlockPayload::RollBack(
                                        rollback_cbor.clone(),
                                        (Point::Origin, 0),
                                    ));
                                } else {
                                    break;
                                }
                            }
                        }

                        blocks
                    }

                    NextResponse::Await => vec![],
                },
                Err(_) => {
                    log::warn!("got no response");
                    vec![]
                }
            },
            false => match peer.chainsync.recv_while_must_reply().await.or_restart() {
                Ok(n) => {
                    let mut blocks =
                        flush_buffered_blocks(&mut stage.ctx.lock().await.block_buffer).await;

                    match n {
                        NextResponse::RollForward(h, t) => {
                            log::warn!("rolling forward");

                            let parsed_headers = to_traverse(&h);
                            match parsed_headers {
                                Ok(parsed_headers) => {
                                    match peer
                                        .blockfetch
                                        .fetch_single(Point::Specific(
                                            parsed_headers.slot(),
                                            parsed_headers.hash().to_vec(),
                                        ))
                                        .await
                                        .or_retry()
                                    {
                                        Ok(static_single) => {
                                            blocks
                                                .push(RawBlockPayload::RollForward(static_single));
                                        }
                                        Err(_) => {}
                                    }
                                }
                                Err(_) => {}
                            }

                            blocks
                        }
                        NextResponse::RollBackward(p, t) => {
                            log::warn!("rolling back");

                            stage
                                .ctx
                                .lock()
                                .await
                                .block_buffer
                                .enqueue_rollback_batch(&p);

                            loop {
                                let pop_rollback_block =
                                    stage.ctx.lock().await.block_buffer.rollback_pop();
                                stage.historic_blocks_removed.inc(1);

                                if let Some(last_good_block) =
                                    stage.ctx.lock().await.block_buffer.get_block_latest()
                                {
                                    if let Ok(parsed_last_good_block) =
                                        MultiEraBlock::decode(&last_good_block)
                                    {
                                        let last_good_point = Point::Specific(
                                            parsed_last_good_block.slot(),
                                            parsed_last_good_block.hash().to_vec(),
                                        );

                                        if let Some(rollback_cbor) = pop_rollback_block {
                                            blocks.push(RawBlockPayload::RollBack(
                                                rollback_cbor.clone(),
                                                (last_good_point, parsed_last_good_block.number()),
                                            ));
                                        } else {
                                            break;
                                        }
                                    }
                                } else {
                                    if let Some(rollback_cbor) = pop_rollback_block {
                                        blocks.push(RawBlockPayload::RollBack(
                                            rollback_cbor.clone(),
                                            (Point::Origin, 0),
                                        ));
                                    } else {
                                        break;
                                    }
                                }
                            }

                            blocks
                        }
                        NextResponse::Await => blocks,
                    }
                }
                Err(_) => vec![],
            },
        }))
    }

    async fn execute(
        &mut self,
        unit: &Vec<RawBlockPayload>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        for raw_block_payload in unit {
            stage
                .output
                .send(gasket::messaging::Message {
                    payload: raw_block_payload.clone(),
                })
                .await
                .or_retry()?;
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        self.peer.as_mut().unwrap().abort();

        Ok(())
    }
}
