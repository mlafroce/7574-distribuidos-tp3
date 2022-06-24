use csv::{Reader, ReaderBuilder, StringRecord};
use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Post {
    pub id: String,
    subreddit_id: String,
    nsfw: bool,
    created: u64,
    permalink: String,
    pub url: String,
    pub body: String,
    pub score: u32,
}

impl Post {
    fn from(record: StringRecord) -> Option<Self> {
        Some(Self {
            id: record.get(1)?.to_string(),
            subreddit_id: record.get(2)?.to_string(),
            nsfw: record.get(4)?.eq("true"),
            created: str::parse::<u64>(record.get(5)?).expect("Created timestamp is invalid"),
            permalink: record.get(6)?.to_string(),
            url: record.get(8)?.to_string(),
            body: record.get(9)?.to_string(),
            score: str::parse::<u32>(record.get(11)?).expect("Score is invalid"),
        })
    }
}

pub struct PostIterator {
    reader: Reader<File>,
}

impl PostIterator {
    pub fn from_file(path: &str) -> Self {
        let post_file = File::open(path).unwrap();
        let reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(post_file);
        Self { reader }
    }
}

impl Iterator for PostIterator {
    type Item = Post;

    fn next(&mut self) -> Option<Self::Item> {
        let record_res = self.reader.records().next()?;
        let record = record_res.ok()?;
        Post::from(record)
    }
}
