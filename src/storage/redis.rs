use std::str::FromStr;
use std::sync::Arc;

use gasket::framework::*;

use gasket::messaging::InputPort;
use log::warn;
use redis::{Cmd, Commands, ConnectionLike, ToRedisArgs};
use serde::Deserialize;

use crate::model::{CRDTCommand, Member, Value};
use crate::pipeline::Context;
use crate::{crosscut, model};

impl ToRedisArgs for model::Value {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            model::Value::String(x) => x.write_redis_args(out),
            model::Value::BigInt(x) => x.to_string().write_redis_args(out),
            model::Value::Cbor(x) => x.write_redis_args(out),
            model::Value::Json(x) => todo!("{}", x),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub connection_params: String,
    pub cursor_key: Option<String>,
}

impl Config {
    pub fn bootstrapper(&self, ctx: Arc<Context>) -> Stage {
        Stage {
            config: self.clone(),
            cursor: Cursor {
                config: self.clone(),
            },
            input: Default::default(),
            storage_ops: Default::default(),
            chain_era: Default::default(),
            last_block: Default::default(),
            blocks_processed: Default::default(),
            transactions_finalized: Default::default(),
            ctx,
        }
    }

    pub fn cursor_key(&self) -> &str {
        self.cursor_key.as_deref().unwrap_or("_cursor")
    }
}

#[derive(Clone)]
pub struct Cursor {
    config: Config,
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        let mut connection = redis::Client::open(self.config.connection_params.clone())
            .and_then(|x| x.get_connection())
            .map_err(crate::Error::storage)?;

        let raw: Option<String> = connection
            .get(&self.config.cursor_key())
            .map_err(crate::Error::storage)?;

        let point = match raw {
            Some(x) => Some(crosscut::PointArg::from_str(&x)?),
            None => None,
        };

        Ok(point)
    }
}

#[derive(Stage)]
#[stage(name = "storage-redis", unit = "CRDTCommand", worker = "Worker")]
pub struct Stage {
    config: Config,
    pub cursor: Cursor,
    pub ctx: Arc<Context>,

    pub input: InputPort<CRDTCommand>,

    #[metric]
    storage_ops: gasket::metrics::Counter,

    #[metric]
    chain_era: gasket::metrics::Gauge,

    #[metric]
    last_block: gasket::metrics::Gauge,

    #[metric]
    blocks_processed: gasket::metrics::Counter,

    #[metric]
    transactions_finalized: gasket::metrics::Counter,
}

pub struct Worker {
    connection: Option<redis::Connection>,
}

// Hack to encode era
pub fn string_to_i64(s: String) -> i64 {
    let bytes = s.into_bytes();
    let mut result: i64 = 0;

    for &b in bytes.iter() {
        assert!(b < 128); // Ensures ascii
        result <<= 8;
        result |= i64::from(b);
    }

    // If the string is less than 8 characters, left pad with zeros.
    for _ in 0..8usize.saturating_sub(bytes.len()) {
        result <<= 8;
    }

    result
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        log::info!("connecting to redis");
        Ok(Self {
            connection: Some(
                redis::Client::open(stage.config.connection_params.clone())
                    .and_then(|redis_client| redis_client.get_connection())
                    .or_retry()?,
            ),
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<CRDTCommand>, WorkerError> {
        stage
            .input
            .recv()
            .await
            .or_retry()
            .map(|msg| WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &CRDTCommand, stage: &mut Stage) -> Result<(), WorkerError> {
        stage.storage_ops.inc(1);

        match unit {
            CRDTCommand::Noop => Ok(()),
            model::CRDTCommand::BlockStarting(_) => redis::cmd("MULTI")
                .query(self.connection.as_mut().unwrap())
                .or_retry(),

            CRDTCommand::GrowOnlySetAdd(key, member) => self
                .connection
                .as_mut()
                .unwrap()
                .sadd(key, member)
                .or_retry(),

            CRDTCommand::SetAdd(key, member) => self
                .connection
                .as_mut()
                .unwrap()
                .sadd(key, member)
                .or_retry(),

            CRDTCommand::SetRemove(key, member) => self
                .connection
                .as_mut()
                .unwrap()
                .srem(key, member)
                .or_retry(),

            CRDTCommand::LastWriteWins(key, member, ts) => self
                .connection
                .as_mut()
                .unwrap()
                .zadd(key, member, ts)
                .or_retry(),

            CRDTCommand::SortedSetAdd(key, member, delta) => self
                .connection
                .as_mut()
                .unwrap()
                .zincr(key, member, delta)
                .or_retry(),

            CRDTCommand::SortedSetMemberRemove(key, member) => self
                .connection
                .as_mut()
                .unwrap()
                .zrem(&key, member)
                .or_retry(),

            CRDTCommand::SortedSetRemove(key, member, delta) => self
                .connection
                .as_mut()
                .unwrap()
                .zrembyscore(&key, member, delta)
                .or_retry(),

            CRDTCommand::Spoil(key) => self.connection.as_mut().unwrap().del(key).or_retry(),

            CRDTCommand::AnyWriteWins(key, value) => {
                self.connection.as_mut().unwrap().set(key, value).or_retry()
            }

            CRDTCommand::PNCounter(key, delta) => self
                .connection
                .as_mut()
                .unwrap()
                .req_command(
                    &Cmd::new()
                        .arg("INCRBYFLOAT")
                        .arg(key)
                        .arg(delta.to_string()),
                )
                .and_then(|_| Ok(()))
                .or_retry(),

            CRDTCommand::HashSetMulti(key, members, values) => {
                let mut tuples: Vec<(Member, Value)> = vec![];
                for (index, member) in members.iter().enumerate() {
                    tuples.push((member.to_owned(), values[index].clone()));
                }

                self.connection
                    .as_mut()
                    .unwrap()
                    .hset_multiple(key, &tuples)
                    .or_retry()
            }

            CRDTCommand::HashSetValue(key, member, value) => self
                .connection
                .as_mut()
                .unwrap()
                .hset(key, member, value)
                .or_retry(),

            CRDTCommand::HashCounter(key, member, delta) => self
                .connection
                .as_mut()
                .unwrap()
                .req_command(
                    &Cmd::new()
                        .arg("HINCRBYFLOAT")
                        .arg(key.clone())
                        .arg(member.clone())
                        .arg(delta.to_string()),
                )
                .and_then(|_| Ok(()))
                .or_retry(),

            CRDTCommand::HashUnsetKey(key, member) => self
                .connection
                .as_mut()
                .unwrap()
                .hdel(member, key)
                .or_retry(),

            CRDTCommand::UnsetKey(key) => self.connection.as_mut().unwrap().del(key).or_retry(),

            CRDTCommand::BlockFinished(
                point,
                bytes,
                era,
                tx_len,
                block_number,
                rollback,
                is_genesis,
            ) => {
                let cursor_str = crosscut::PointArg::from(point.clone()).to_string();
                stage.chain_era.set(era.clone());

                let connection = self.connection.as_mut().unwrap();
                connection
                    .set(stage.config.cursor_key(), &cursor_str)
                    .or_panic()?;

                if let Some(bytes) = bytes {
                    if !bytes.is_empty() {
                        stage
                            .ctx
                            .block_buffer
                            .insert_block(&point, bytes)
                            .or_panic()?; // if this panics, then state is probably in the unrecoverable place from here on out
                    }
                }

                redis::cmd("EXEC").query(connection).or_retry()?;

                warn!("donzeo");

                stage.blocks_processed.inc(1);

                if *is_genesis {
                    warn!("[redis] byron genesis transactions indexed");
                }

                stage.transactions_finalized.inc(tx_len.clone());
                stage.last_block.set(block_number.clone());

                // i dont think i need this because of my pop routine
                // if *rollback {
                //     stage.ctx.block_buffer.remove_block(&point).or_panic()?;
                // }

                Ok(())
            }
        }
    }
}
