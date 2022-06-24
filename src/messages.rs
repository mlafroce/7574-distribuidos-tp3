use crate::comment::Comment;
use crate::post::Post;
use serde::{Deserialize, Serialize};

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
}
