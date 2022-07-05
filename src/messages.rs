use crate::comment::Comment;
use crate::post::Post;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Score {
    pub sum: u32,
    pub count: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostSentiment {
    pub post_id: String,
    pub sentiment: f32,
    pub count: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BestMeme {
    pub id: String,
    pub url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Message {
    EndOfStream,
    FullPost(Post),
    FullComment(Comment),
    PostScore(u32),
    PostScoreMean(f32),
    PostId(String),
    PostUrl(String, String),
    PostIdSentiment(String, f32),
    CollegePostUrl(String),
    DataToSave(String, String),
    BulkMessage(Vec<u8>, Vec<usize>),
    Confirmed
}

#[derive(Default, Debug)]
pub struct BulkBuilder {
    data_buf: Vec<u8>,
    data_sizes: Vec<usize>,
}

impl BulkBuilder {
    pub fn push<T: Serialize>(&mut self, message: &T) {
        let mut data = bincode::serialize(&message).unwrap();
        self.data_sizes.push(data.len());
        self.data_buf.append(&mut data);
    }

    pub fn build(&mut self) -> Message {
        let data = self.data_buf.drain(..).collect();
        let sizes = self.data_sizes.drain(..).collect();
        Message::BulkMessage(data, sizes)
    }

    pub fn size(&self) -> usize {
        self.data_buf.len() + self.data_sizes.len() * std::mem::size_of::<usize>()
    }
}
