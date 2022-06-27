use crate::comment::Comment;
use crate::post::Post;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Score {
    pub sum: u32,
    pub count: u32
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostSentiment {
    pub post_id: String,
    pub sentiment: f32,
    pub count: i32
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BestMeme {
    pub id: String,
    pub url: String
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

    DataScore(Score),
    DataPostSentiment(PostSentiment),
    DataBestMeme(BestMeme),
    DataScoreAverage(f32),
    DataPostIdCollege(String),
    DataPostId(String),
    DataReset(String)
}
