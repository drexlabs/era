use bech32::{self, Error, ToBase32};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use hex::{self};
use pallas::ledger::traverse::{block, MultiEraBlock, MultiEraOutput, MultiEraTx, OutputRef};
use std::error::Error as Err;

use crate::model::BlockContext;

const ASSET_DATA: &str = "asset";

pub struct AssetFingerprint {
    hash_buf: [u8; 20],
}

impl AssetFingerprint {
    pub fn from_parts(policy_id: &str, asset_name: &str) -> Result<AssetFingerprint, Box<dyn Err>> {
        let mut hasher = Blake2bVar::new(20).unwrap();
        let c = format!("{policy_id}{asset_name}");
        let raw = hex::decode(c)?;
        hasher.update(raw.as_slice());
        let mut buf = [0u8; 20];
        hasher.finalize_variable(&mut buf)?;

        Ok(AssetFingerprint { hash_buf: buf })
    }

    pub fn fingerprint(&self) -> Result<String, Error> {
        bech32::encode(
            ASSET_DATA,
            self.hash_buf.to_base32(),
            bech32::Variant::Bech32,
        )
    }
}
