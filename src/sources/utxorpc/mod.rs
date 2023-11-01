use std::sync::Arc;

use futures::StreamExt;
use gasket::framework::*;

use gasket::messaging::tokio::OutputPort;
use log::*;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Streaming;

use crate::crosscut::PointArg;
use crate::model::RawBlockPayload;
use crate::{crosscut, storage::Cursor};

use utxorpc::proto::sync::v1::any_chain_block::Chain;
use utxorpc::proto::sync::v1::chain_sync_service_client::ChainSyncServiceClient;
use utxorpc::proto::sync::v1::follow_tip_response::Action;
use utxorpc::proto::sync::v1::{BlockRef, DumpHistoryRequest, FollowTipRequest, FollowTipResponse};

use crate::pipeline::Context;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub address: String,
    pub min_depth: Option<usize>,
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<Mutex<Context>>, cursor: Cursor) -> Stage {
        Stage {
            config: self,
            cursor,
            ctx,
            output: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }
}

pub struct Worker {
    client: ChainSyncServiceClient<Channel>,
    stream: Option<Streaming<FollowTipResponse>>,
    block_ref: Option<BlockRef>,
}

impl Worker {
    async fn process_next(&self, stage: &mut Stage, action: &Action) -> Result<(), WorkerError> {
        match action {
            Action::Apply(block) => {
                if let Some(chain) = &block.chain {
                    match chain {
                        Chain::Cardano(block) => {
                            if block.body.is_some() {
                                let header = block.header.as_ref().unwrap();

                                let block_body = block.body.as_ref().unwrap();

                                // stage
                                //     .output
                                //     .send(RawBlockPayload::roll_forward(block_body.into()))
                                //     .await
                                //     .unwrap();

                                // stage.chain_tip.set(header.slot as i64);
                            }
                        }
                        Chain::Raw(bytes) => {
                            let block = MultiEraBlock::decode(bytes).or_panic()?;

                            stage
                                .output
                                .send(RawBlockPayload::roll_forward(bytes.to_vec()))
                                .await
                                .unwrap();

                            //stage.chain_tip.set(block.slot() as i64);
                        }
                    }
                }
            }
            Action::Undo(block) => {
                if let Some(chain) = &block.chain {
                    match chain {
                        Chain::Cardano(block) => {
                            if block.body.is_some() {
                                let header = block.header.as_ref().unwrap();

                                let block = block.body.as_ref().unwrap();

                                // for tx in block.tx.clone() {
                                //     let evt = ChainEvent::Undo(
                                //         Point::Specific(header.slot, header.hash.to_vec()),
                                //         Record::ParsedTx(tx),
                                //     );

                                //     stage.output.send(evt.into()).await.or_panic()?;
                                //     stage.chain_tip.set(header.slot as i64);
                                // }
                            }
                        }
                        Chain::Raw(bytes) => {
                            // let block = MultiEraBlock::decode(bytes).or_panic()?;

                            // stage
                            //     .output
                            //     .send(RawBlockPayload::roll_back(bytes.to_vec()))
                            //     .await
                            //     .unwrap();

                            // stage.chain_tip.set(block.slot() as i64);
                        }
                    }
                }
            }
            Action::Reset(reset) => {
                // stage
                //     .output
                //     .send(ChainEvent::Reset(Point::new(reset.index, reset.hash.to_vec())).into())
                //     .await
                //     .or_panic()?;

                // stage.chain_tip.set(reset.index as i64);
            }
        }

        Ok(())
    }

    async fn next_stream(&mut self) -> Result<WorkSchedule<Vec<Action>>, WorkerError> {
        if self.stream.is_none() {
            let stream = self
                .client
                .follow_tip(FollowTipRequest::default())
                .await
                .or_restart()?
                .into_inner();

            self.stream = Some(stream);
        }

        let result = self.stream.as_mut().unwrap().next().await;

        if result.is_none() {
            return Ok(WorkSchedule::Idle);
        }

        let result = result.unwrap();
        if let Err(err) = result {
            error!("{err}");
            return Err(WorkerError::Retry);
        }

        let response: FollowTipResponse = result.unwrap();
        if response.action.is_none() {
            return Ok(WorkSchedule::Idle);
        }

        let action = response.action.unwrap();

        Ok(WorkSchedule::Unit(vec![action]))
    }

    async fn next_dump_history(&mut self) -> Result<WorkSchedule<Vec<Action>>, WorkerError> {
        let dump_history_request = DumpHistoryRequest {
            start_token: self.block_ref.clone(),
            max_items: 100,
            ..Default::default()
        };

        let result = self
            .client
            .dump_history(dump_history_request)
            .await
            .or_restart()?
            .into_inner();

        self.block_ref = result.next_token;

        if !result.block.is_empty() {
            let actions: Vec<Action> = result.block.into_iter().map(Action::Apply).collect();
            return Ok(WorkSchedule::Unit(actions));
        }

        Ok(WorkSchedule::Idle)
    }
}

#[derive(Stage)]
#[stage(name = "source-utxorpc", unit = "Vec<Action>", worker = "Worker")]
pub struct Stage {
    config: Config,
    cursor: Cursor,
    ctx: Arc<Mutex<Context>>,
    pub output: OutputPort<RawBlockPayload>,

    #[metric]
    pub chain_tip: gasket::metrics::Gauge,

    #[metric]
    pub block_count: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("connecting {}", stage.config.address.clone());

        let client = ChainSyncServiceClient::connect(stage.config.address.clone())
            .await
            .or_retry()?;

        let mut point: Option<(u64, Vec<u8>)> = match stage.ctx.lock().await.intersect.clone() {
            crosscut::IntersectConfig::Point(slot, hash) => Some((slot, hash.into())),
            _ => None,
        };

        if let Some(latest_point) = stage.cursor.clone().last_point().unwrap() {
            point = match latest_point {
                PointArg::Specific(slot, hash) => Some((slot, hash.into_bytes())),
                _ => None,
            };
        }

        let block_ref = point.map(|(slot, hash)| BlockRef {
            index: slot,
            hash: hash.into(),
        });

        Ok(Self {
            client,
            stream: None,
            block_ref,
        })
    }

    async fn schedule(&mut self, _: &mut Stage) -> Result<WorkSchedule<Vec<Action>>, WorkerError> {
        if self.block_ref.is_some() {
            return self.next_dump_history().await;
        }

        self.next_stream().await
    }

    async fn execute(&mut self, unit: &Vec<Action>, stage: &mut Stage) -> Result<(), WorkerError> {
        for action in unit {
            self.process_next(stage, action).await.or_retry()?;
        }

        Ok(())
    }
}
