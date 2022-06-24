use csv::{Reader, ReaderBuilder, StringRecord};
use lazy_static::lazy_static;
use log::debug;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use std::fs::File;

const COLLEGE_WORDS: &str = "university|college|student|teacher|professor";
const URL_PATTERN: &str = "https://old.reddit.com/r/meirl/comments/([^/]+)/meirl/.*";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Comment {
    id: String,
    subreddit_id: String,
    nsfw: bool,
    created: u64,
    permalink: String,
    body: String,
    pub sentiment: String,
    score: String,
}

impl Comment {
    fn from(record: StringRecord) -> Option<Self> {
        debug!("Record: {:?}", record);
        Some(Self {
            id: record.get(1)?.to_string(),
            subreddit_id: record.get(2)?.to_string(),
            nsfw: record.get(4)?.eq("true"),
            created: str::parse::<u64>(record.get(5)?).expect("Created timestamp is invalid"),
            permalink: record.get(6)?.to_string(),
            body: record.get(7)?.to_string(),
            sentiment: record.get(8)?.to_string(),
            //score: str::parse::<u32>(record.get(9)?).expect("Score is invalid"),
            score: record.get(9)?.to_string(),
        })
    }

    pub fn parse_post_id(&self) -> Option<String> {
        lazy_static! {
            /// Avoid compiling the same regex everytime
            static ref URL_REGEX: Regex = Regex::new(URL_PATTERN).unwrap();
        }
        let matched = URL_REGEX.captures(&self.permalink)?.get(1)?;
        Some(matched.as_str().to_string())
    }

    pub fn is_college_related(&self) -> bool {
        lazy_static! {
            /// Avoid compiling the same regex everytime
            static ref COLLEGE_RELATED_REGEX: Regex = RegexBuilder::new(COLLEGE_WORDS)
                .case_insensitive(true)
                .build()
                .expect("Invalid Regex");
        }
        COLLEGE_RELATED_REGEX.is_match(&self.body)
    }
}

pub struct CommentIterator {
    reader: Reader<File>,
}

impl CommentIterator {
    pub fn from_file(path: &str) -> Self {
        let comment_file = File::open(path).unwrap();
        let reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(comment_file);
        Self { reader }
    }
}

impl Iterator for CommentIterator {
    type Item = Comment;

    fn next(&mut self) -> Option<Self::Item> {
        let record_res = self.reader.records().next()?;
        let record = record_res.ok()?;
        Comment::from(record)
    }
}
