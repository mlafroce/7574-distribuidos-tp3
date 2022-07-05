use crate::messages::BulkBuilder;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use log::info;
use serde_json;

#[derive(PartialEq)]
pub enum Checkpoint<S: Serialize> {
    Clean,
    Processed { state: S },
    Sent,
}

pub struct TransactionLog {
    log: File,
}

impl TransactionLog {
    pub fn new(path: &str) -> io::Result<Self> {
        let log = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self { log })
    }

    pub fn load_state<S: Serialize>(
        &mut self,
        bulk_output: &BulkBuilder,
    ) -> io::Result<Checkpoint<S>> {
        let mut text = String::new();
        self.log.read_to_string(&mut text)?;
        for line in text.lines().rev() {
            println!("{}", line);
        }
        Ok(Checkpoint::Clean)
    }

    pub fn save_state<S: Serialize>(&mut self, state: S) -> io::Result<()> {
        let state = serde_json::to_string(&state).unwrap();
        self.log.write_all(state.as_bytes())
    }

    pub fn save_sent(&self) {}
    pub fn save_clean(&self) {}
}
