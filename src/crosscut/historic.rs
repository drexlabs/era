use crate::{model::RawBlockPayload, Error};
use crossbeam_queue::SegQueue;
use pallas::network::miniprotocols::Point;
use serde::{Deserialize, Serialize};
use sled::IVec;
use std::fs::create_dir_all;

const DISK_COMPRESSION_FACTOR: i32 = 20;
const SYSTEM_PAGE_CACHE_BYTES: u64 = 1073741824;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct BlockConfig {
    pub db_path: String,
    pub rollback_db_path: String,
}

impl Default for BlockConfig {
    fn default() -> Self {
        BlockConfig {
            db_path: "/opt/era/block_buffer".to_string(),
            rollback_db_path: "/opt/era/consumed_buffer".to_string(),
        }
    }
}

impl From<BlockConfig> for BufferBlocks {
    fn from(config: BlockConfig) -> Self {
        BufferBlocks::open_db(config)
    }
}

pub struct BufferBlocks {
    pub config: BlockConfig,
    db: Option<sled::Db>,
    rollback_queue: SegQueue<(String, Vec<u8>)>,
    buffer: SegQueue<RawBlockPayload>,
}

fn to_zero_padded_string(point: &Point) -> String {
    let block_identifier = match point.clone() {
        Point::Origin => String::from("ORIGIN"),
        Point::Specific(_, block_hash) => String::from(hex::encode(block_hash)),
    };

    // This is needed so that the strings stored in jsonb will be ordered properly when they contain integers (zero padding)
    return format!(
        "{:0>width$}{}",
        point.slot_or_default(),
        block_identifier,
        width = 15
    );
}

impl BufferBlocks {
    fn open_db(config: BlockConfig) -> Self {
        let db_path = config.clone().db_path;

        if let Err(_) = create_dir_all(&db_path) {
            log::info!("sled: database already exists ({})", db_path);
        } else {
            log::info!("sled: database being created ({})", db_path);
        }

        log::info!("sled: opening database ({})", db_path);

        let db = sled::Config::default()
            .path(db_path)
            .use_compression(true)
            .compression_factor(DISK_COMPRESSION_FACTOR)
            .cache_capacity(SYSTEM_PAGE_CACHE_BYTES)
            .open()
            .unwrap();

        log::info!("sled: block buffer db opened");

        BufferBlocks {
            config,
            db: Some(db),
            rollback_queue: Default::default(),
            buffer: Default::default(),
        }
    }

    pub fn block_mem_add(&self, block_msg_payload: RawBlockPayload) {
        self.buffer.push(block_msg_payload);
    }

    pub fn block_mem_take_all(&self) -> Option<Vec<RawBlockPayload>> {
        let mut blocks = Vec::new();
        while let Some(block) = self.buffer.pop() {
            blocks.push(block);
        }
        if blocks.is_empty() {
            None
        } else {
            Some(blocks)
        }
    }

    pub fn block_mem_size(&self) -> usize {
        self.buffer.len()
    }

    fn get_db_ref(&self) -> &sled::Db {
        self.db.as_ref().unwrap()
    }

    fn get_rollback_range(&self, from: &Point) -> Vec<(String, Vec<u8>)> {
        let mut blocks_to_roll_back: Vec<(String, Vec<u8>)> = vec![];
        let db = self.get_db_ref();
        let key = to_zero_padded_string(from);
        let mut last_seen_slot_key = key.clone();

        while let Ok(Some((next_key, next_block))) = db.get_gt(last_seen_slot_key.as_bytes()) {
            last_seen_slot_key = String::from_utf8(next_key.to_vec()).unwrap();
            blocks_to_roll_back.push((last_seen_slot_key.clone(), next_block.to_vec()));
        }

        blocks_to_roll_back
    }

    pub fn close(self) -> Result<usize, crate::Error> {
        self.get_db_ref().flush().map_err(crate::Error::storage)
    }

    pub fn insert_block(&self, point: &Point, block: &Vec<u8>) -> Result<Option<IVec>, Error> {
        let key = to_zero_padded_string(point);
        self.get_db_ref()
            .insert(key.as_bytes(), sled::IVec::from(block.clone()))
            .map_err(crate::Error::storage)
    }

    pub fn remove_block(&self, point: &Point) -> Result<Option<IVec>, Error> {
        let key = to_zero_padded_string(point);
        self.get_db_ref()
            .remove(key.as_bytes())
            .map_err(crate::Error::storage)
    }

    pub fn get_block_at_point(&self, point: &Point) -> Option<Vec<u8>> {
        let key = to_zero_padded_string(point);
        self.get_db_ref()
            .get(key.as_bytes())
            .ok()
            .flatten()
            .map(|i| i.to_vec())
    }

    pub fn get_block_latest(&self) -> Option<Vec<u8>> {
        self.get_db_ref()
            .last()
            .ok()
            .flatten()
            .map(|(_, i)| i.to_vec())
    }

    pub fn enqueue_rollback_batch(&self, from: &Point) -> usize {
        let blocks_to_roll_back = self.get_rollback_range(from);
        for block in blocks_to_roll_back {
            self.rollback_queue.push(block);
        }

        self.rollback_queue.len()
    }

    pub fn rollback_pop(&self) -> Result<Option<Vec<u8>>, Error> {
        if let Some((key, block)) = self.rollback_queue.pop() {
            self.get_db_ref()
                .remove(key.as_bytes())
                .map_err(Error::storage)?;
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }

    pub fn get_current_queue_depth(&self) -> usize {
        self.rollback_queue.len()
    }
}
