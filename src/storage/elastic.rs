use std::sync::Arc;

use elasticsearch::Elasticsearch;

use elasticsearch::http::response::Response;

use futures::stream::StreamExt;

use gasket::framework::*;
use gasket::messaging::tokio::InputPort;

use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::sync::Mutex;

use crate::{
    crosscut,
    model::{self, CRDTCommand},
    pipeline::Context,
    Error,
};

impl From<model::Value> for JsonValue {
    fn from(other: model::Value) -> JsonValue {
        match other {
            model::Value::String(x) => json!(x),
            model::Value::Cbor(x) => json!(hex::encode(x)),
            model::Value::BigInt(x) => json!(x),
            model::Value::Json(x) => x,
            // model::Value::BigInt(x) => json!({ "value": x }),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub connection_url: String,
    pub worker_threads: Option<usize>,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<Mutex<Context>>) -> Stage {
        Stage {
            config: self.clone(),
            input: Default::default(),
            ops_count: Default::default(),
            cursor: Cursor {},
            ctx,
        }
    }
}

#[derive(Clone)]
pub struct Cursor {}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        Ok(None)
    }
}

const BATCH_SIZE: usize = 40;

#[derive(Default)]
struct Batch {
    block_end: Option<CRDTCommand>,
    items: Vec<CRDTCommand>,
}

async fn recv_batch(input: &mut InputPort<CRDTCommand>) -> Result<Batch, gasket::error::Error> {
    let mut batch = Batch::default();

    loop {
        match input.recv().await {
            Ok(x) => match x.payload {
                CRDTCommand::BlockStarting(_) => (),
                CRDTCommand::BlockFinished(_, _, _) => {
                    batch.block_end = Some(x.payload);
                    return Ok(batch);
                }
                _ => {
                    batch.items.push(x.payload);
                }
            },
            _ => return Err(gasket::error::Error::RecvError),
        };

        if batch.items.len() >= BATCH_SIZE {
            return Ok(batch);
        }
    }
}

type ESResult = Result<Response, elasticsearch::Error>;

async fn apply_command(cmd: CRDTCommand, client: &Elasticsearch) -> Option<ESResult> {
    match cmd {
        CRDTCommand::BlockStarting(_) => None,
        CRDTCommand::AnyWriteWins(key, value) => client
            .index(elasticsearch::IndexParts::IndexId("era", &key))
            .body::<JsonValue>(json!({ "key": &key, "value": JsonValue::from(value) }))
            .send()
            .await
            .into(),
        CRDTCommand::BlockFinished(_, _, _) => {
            log::warn!("Elasticsearch storage doesn't support cursors ATM");
            None
        }
        _ => todo!(),
    }
}

async fn apply_batch(batch: Batch, client: &Elasticsearch) -> Result<(), gasket::error::Error> {
    let mut stream = futures::stream::iter(batch.items)
        .map(|cmd| apply_command(cmd, client))
        .buffer_unordered(10);

    while let Some(op) = stream.next().await {
        if let Some(result) = op {
            // TODO: we panic because retrying a partial batch might yield weird results.
            // Once we have a two-phase commit mechanism in the input port, we can switch
            // back to retying instead of panicking.
            result
                .map(|x| x.error_for_status_code())
                .map_err(|e| Error::StorageError(e.to_string()))
                .unwrap()
                .unwrap();
        }
    }

    // we process the block end after the rest of the commands to ensure that no
    // other change from the block remains pending in the async queue
    if let Some(block_end) = batch.block_end {
        if let Some(result) = apply_command(block_end, client).await {
            result
                .and_then(|x| x.error_for_status_code())
                .map_err(|e| Error::StorageError(e.to_string()))
                .unwrap();
        }
    }

    Ok(())
}

#[derive(Stage)]
#[stage(
    name = "storage-elasticsearch",
    unit = "CRDTCommand",
    worker = "Worker"
)]
pub struct Stage {
    config: Config,
    pub cursor: Cursor,
    pub ctx: Arc<Mutex<Context>>,

    pub input: InputPort<CRDTCommand>,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

pub struct Worker {
    client: Option<Elasticsearch>,
}

// todo this is trash. schedule and execute both going out and trying to find work their own dumb ways
#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<CRDTCommand>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, _: &CRDTCommand, stage: &mut Stage) -> Result<(), WorkerError> {
        let batch = recv_batch(&mut stage.input).await.unwrap();
        let count = batch.items.len();
        let client = self.client.as_ref().unwrap();

        apply_batch(batch, client).await.unwrap();
        stage.ops_count.inc(count as u64);
        Ok(())
    }

    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let url = elasticsearch::http::Url::parse(&stage.config.connection_url)
            .map_err(|err| Error::ConfigError(err.to_string()))
            .or_panic()
            .unwrap();

        let auth = (&stage.config.username, &stage.config.password);

        let pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);

        let transport = elasticsearch::http::transport::TransportBuilder::new(pool);

        let transport = if let (Some(username), Some(password)) = auth {
            transport.auth(elasticsearch::auth::Credentials::Basic(
                username.clone(),
                password.clone(),
            ))
        } else {
            transport
        };

        let client = Elasticsearch::new(
            transport
                .cert_validation(elasticsearch::cert::CertificateValidation::None)
                .build()
                .or_retry()
                .unwrap(),
        )
        .into();

        Ok(Self { client })
    }
}
