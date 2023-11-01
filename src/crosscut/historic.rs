use crate::{model::RawBlockPayload, Error};
use pallas::network::miniprotocols::Point;
use serde::{Deserialize, Serialize};
use std::{mem, path::PathBuf};

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

#[derive(Clone)]
pub struct BufferBlocks {
    pub config: BlockConfig,
    db: Option<sled::Db>,
    queue: Vec<(String, Vec<u8>)>, //rollback queue.. badly named todo rename
    buffer: Vec<RawBlockPayload>,
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
        let db = sled::Config::default()
            .path(config.clone().db_path)
            .cache_capacity(1073741824)
            .open()
            .unwrap();

        log::error!("opened block buffer db");
        let queue: Vec<(String, Vec<u8>)> = Vec::default();

        BufferBlocks {
            config,
            db: Some(db),
            queue,
            buffer: Default::default(),
        }
    }

    pub fn block_mem_add(&mut self, block_msg_payload: RawBlockPayload) {
        self.buffer.push(block_msg_payload.to_owned());
    }

    pub fn block_mem_take_all(&mut self) -> Option<Vec<RawBlockPayload>> {
        let empty: Vec<RawBlockPayload> = vec![];
        let blocks = mem::replace(&mut self.buffer, empty);
        match blocks.is_empty() {
            true => None,
            false => Some(blocks),
        }
    }

    pub fn block_mem_size(&self) -> usize {
        self.buffer.len()
    }

    fn get_db_ref(&self) -> &sled::Db {
        self.db.as_ref().unwrap()
    }

    fn get_rollback_range(&mut self, from: &Point) -> Vec<(String, Vec<u8>)> {
        let mut blocks_to_roll_back: Vec<(String, Vec<u8>)> = vec![];

        let db = self.get_db_ref();

        let key = to_zero_padded_string(from);

        let mut last_seen_slot_key = key.clone();

        while let Some((next_key, next_block)) = db.get_gt(last_seen_slot_key.as_bytes()).unwrap() {
            last_seen_slot_key = String::from_utf8(next_key.to_vec()).unwrap();
            blocks_to_roll_back.push((last_seen_slot_key.clone(), next_block.to_vec()));
        }

        if !blocks_to_roll_back.is_empty() {
            self.queue = blocks_to_roll_back;
        }

        self.queue.clone()
    }

    pub fn close(self) {
        self.get_db_ref().flush().unwrap_or_default();
    }

    pub fn insert_block(&mut self, point: &Point, block: &Vec<u8>) {
        let key = to_zero_padded_string(point);
        let db = self.get_db_ref();
        db.insert(key.as_bytes(), sled::IVec::from(block.clone()))
            .expect("todo map storage error");
    }

    pub fn remove_block(&mut self, point: &Point) {
        let key = to_zero_padded_string(point);
        let db = self.get_db_ref();
        db.remove(key.as_bytes()).expect("todo map storage error");
    }

    pub fn get_block_at_point(&self, point: &Point) -> Option<Vec<u8>> {
        let key = to_zero_padded_string(point);

        match self.get_db_ref().get(key.as_bytes()) {
            Ok(block) => match block {
                Some(block) => Some(block.to_vec()),
                None => None,
            },
            Err(_) => None,
        }
    }

    pub fn get_block_latest(&self) -> Option<Vec<u8>> {
        match self.get_db_ref().last() {
            Ok(block) => match block {
                Some((_, block)) => Some(block.to_vec()),
                None => None,
            },
            Err(_) => None,
        }
    }

    pub fn enqueue_rollback_batch(&mut self, from: &Point) -> usize {
        self.get_rollback_range(&from.clone()).len()
    }

    pub fn rollback_pop(&mut self) -> Option<Vec<u8>> {
        match self.queue.pop() {
            None => None,
            Some((popped_key, popped)) => {
                let _ = self
                    .get_db_ref()
                    .remove(popped_key.as_bytes())
                    .map_err(Error::storage);
                Some(popped)
            }
        }
    }

    pub fn get_current_queue_depth(&mut self) -> usize {
        self.queue.len()
    }
}
