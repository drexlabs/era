use gasket::messaging::OutputPort;
use pallas::ledger::primitives::alonzo::{self, PoolKeyhash, StakeCredential};
use serde::Deserialize;

use crate::model::{CRDTCommand, DecodedBlockAction};
use crate::Error;

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
    output: OutputPort<CRDTCommand>,
}

impl Reducer {
    fn registration(&self, cred: &StakeCredential, pool: &PoolKeyhash) -> CRDTCommand {
        let key = match cred {
            StakeCredential::AddrKeyhash(x) => x.to_string(),
            StakeCredential::ScriptHash(x) => x.to_string(),
        };

        let value = pool.to_string();

        CRDTCommand::any_write_wins(self.config.key_prefix.as_deref(), &key, value)
    }

    fn deregistration(&self, cred: &StakeCredential) -> CRDTCommand {
        let key = match cred {
            StakeCredential::AddrKeyhash(x) => x.to_string(),
            StakeCredential::ScriptHash(x) => x.to_string(),
        };

        CRDTCommand::spoil(self.config.key_prefix.as_deref(), &key)
    }

    pub async fn reduce<'a>(&mut self, block: &'a DecodedBlockAction<'a>) -> Result<(), Error> {
        match block {
            // todo: no genesis support
            DecodedBlockAction::Genesis(genesis_utxo) => Ok(()),

            action @ (DecodedBlockAction::Forward(b, c, p)
            | DecodedBlockAction::Rollback(b, c, p)) => {
                let is_rollback = action.is_rollback();
                for tx in b.txs() {
                    if tx.is_valid() {
                        for cert in tx.certs() {
                            if let Some(cert) = cert.as_alonzo() {
                                match cert {
                                    alonzo::Certificate::StakeDelegation(cred, pool) => {
                                        if !is_rollback {
                                            self.output
                                                .send(self.registration(&cred, &pool).into())
                                                .await
                                                .map_err(crate::Error::reducer)?
                                        } else {
                                            self.output
                                                .send(self.deregistration(&cred).into())
                                                .await
                                                .map_err(crate::Error::reducer)?
                                        }
                                    }

                                    alonzo::Certificate::StakeDeregistration(cred) => {
                                        if !is_rollback {
                                            self.output
                                                .send(self.deregistration(&cred).into())
                                                .await
                                                .map_err(crate::Error::reducer)?
                                        } else {
                                            // unrepairable -- so we don't have full rollback support yet here
                                        }
                                    }

                                    _ => {}
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            output: Default::default(),
        };
        super::Reducer::StakeToPool(reducer)
    }
}
