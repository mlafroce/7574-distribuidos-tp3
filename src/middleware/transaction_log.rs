use std::collections::vec_deque::IntoIter;
use std::collections::VecDeque;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufRead, Seek, Write};
use serde::de::DeserializeOwned;
use serde_json;
use crate::messages::BulkBuilder;

const MAX_CHECKPOINTS: usize = 20;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum Checkpoint<S> where S: std::clone::Clone {
    Clean,
    Processed { state: S, output: BulkBuilder },
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
    ) -> io::Result<(S, BulkBuilder)> {
        let mut text = String::new();
        self.log.rewind()?;
        // TODO: load last lines, not the whole log!
        let reader = BufReader::new(&self.log);
        let last_lines = LastLines::new(reader.lines().flatten(), MAX_CHECKPOINTS);
        let last_processed = last_lines.into_iter()
            .flat_map(|s| {
                serde_json::from_str::<Checkpoint<S>>(&s)
            })
            .filter(|c| matches!(c, Checkpoint::Processed{state: _, output: _}))
            .last();
        if let Some(Checkpoint::Processed {state, output}) = last_processed {
            Ok((state, output))
        } else {
            Ok((S::default(), BulkBuilder::default()))
        }
    }

    pub fn load_checkpoint<S: DeserializeOwned + std::clone::Clone>(
        &mut self,
    ) -> io::Result<Checkpoint<S>> {
        self.log.rewind()?;
        let reader = BufReader::new(&self.log);
        let last_lines = LastLines::new(reader.lines().flatten(), MAX_CHECKPOINTS);
        let last_checkpoint = last_lines.into_iter()
            .flat_map(|s| {
                serde_json::from_str::<Checkpoint<S>>(&s)
            }).last();
        if let Some(checkpoint) = last_checkpoint {
            Ok(checkpoint)
        } else {
            Ok(Checkpoint::Clean)
        }
    }

    pub fn delete_log(&self) -> io::Result<()> {
        std::fs::remove_file(&self.path)
    }

    pub fn save_state<S:Serialize + std::clone::Clone>(&mut self, state: S, bulk: &BulkBuilder) -> io::Result<()> {

        let checkpoint = Checkpoint::Processed {state, output: bulk.clone()};
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

struct LastLines {
    buf: VecDeque<String>
}

impl LastLines {
    pub fn new<T: Iterator<Item=String>>(mut iter: T, max: usize) -> Self {
        let mut buf = VecDeque::with_capacity(max);
        while let Some(s) = iter.next() {
            buf.push_back(s);
            if buf.len() > max {
                buf.pop_front();
            }
        }
        Self {buf}
    }
}

impl IntoIterator for LastLines {
    type Item = String;
    type IntoIter = IntoIter<String>;

    fn into_iter(self) -> Self::IntoIter {
        self.buf.into_iter()
    }
}