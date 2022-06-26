use crate::comment::Comment;
use crate::post::Post;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct CurrentStore {
    pub sum: u32,
    pub count: u32
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PostSentiment {
    pub post_id: String,
    pub sentiment: f32,
    pub count: i32
}

#[derive(Debug, Deserialize, Serialize)]
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

    DataCurrentScore(CurrentStore),
    DataPostSentiment(PostSentiment)
}
