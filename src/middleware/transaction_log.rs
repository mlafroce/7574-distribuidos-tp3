use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, Write};
use serde::de::DeserializeOwned;
use serde_json;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum Checkpoint<S> where S: std::clone::Clone {
    Clean,
    Processed { state: S },
    Sent,
    Confirmed,
    EndOfStream,
    ServiceFinished,
}

pub struct TransactionLog {
    log: File,
    path: String,
}

impl TransactionLog {
    pub fn new(path: &str) -> io::Result<Self> {
        let log = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)?;
        Ok(Self { log, path: path.to_string() })
    }

    pub fn load_state<S: DeserializeOwned + std::fmt::Debug + std::clone::Clone + std::default::Default>(
        &mut self,
    ) -> io::Result<S> {
        let mut text = String::new();
        self.log.rewind()?;
        // TODO: load last lines, not the whole log!
        self.log.read_to_string(&mut text)?;
        let lines = text.lines();
        let last_processed =  lines
            .rev()
            .flat_map(|s| {
                serde_json::from_str::<Checkpoint<S>>(s)
            })
            .find(|c| matches!(c, Checkpoint::Processed{state: _}));
        if let Some(Checkpoint::Processed {state}) = last_processed {
            Ok(state)
        } else {
            Ok(S::default())
        }
    }

    pub fn load_checkpoint<S: DeserializeOwned + std::clone::Clone>(
        &mut self,
    ) -> io::Result<Checkpoint<S>> {
        let mut text = String::new();
        self.log.read_to_string(&mut text)?;
        let lines = text.lines();
        let last_checkpoint =  lines.rev().flat_map(|s| {
            serde_json::from_str::<Checkpoint<S>>(s)
        }).next();
        if let Some(checkpoint) = last_checkpoint {
            Ok(checkpoint)
        } else {
            Ok(Checkpoint::Clean)
        }
    }

    pub fn delete_log(&self) -> io::Result<()> {
        //drop(self.log);
        std::fs::remove_file(&self.path)
    }

    pub fn save_state<S:Serialize + std::clone::Clone>(&mut self, state: S) -> io::Result<()> {
        let checkpoint = Checkpoint::Processed {state};
        self.save_checkpoint(checkpoint)
    }

    pub fn save_sent(&mut self) -> io::Result<()> {
        self.save_checkpoint::<()>(Checkpoint::Sent)
    }

    pub fn save_confirmed(&mut self) -> io::Result<()> {
        self.save_checkpoint::<()>(Checkpoint::Confirmed)
    }

    pub fn save_clean(&mut self) -> io::Result<()> {
        self.save_checkpoint::<()>(Checkpoint::Clean)
    }

    pub fn save_end_of_stream(&mut self) -> io::Result<()> {
        self.save_checkpoint::<()>(Checkpoint::EndOfStream)
    }

    pub fn save_service_finished(&mut self) -> io::Result<()> {
        self.save_checkpoint::<()>(Checkpoint::ServiceFinished)
    }

    fn save_checkpoint<S: Serialize + std::clone::Clone>(&mut self, checkpoint: Checkpoint<S>) -> io::Result<()> {
        let line = serde_json::to_string(&checkpoint).unwrap();
        self.log.write_all(line.as_bytes())?;
        self.log.write_all(&[b'\n'])
    }

}
