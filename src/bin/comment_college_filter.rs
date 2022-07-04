use amiquip::Result;
use log::warn;
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::{Config, COMMENT_COLLEGE_QUEUE_NAME, POST_ID_COLLEGE_QUEUE_NAME};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    let mut processor = CommentCollegeFilter;
    let mut service = RabbitService::new(config, &mut processor);
    service.run(
        COMMENT_COLLEGE_QUEUE_NAME,
        Some(POST_ID_COLLEGE_QUEUE_NAME.to_string()),
    )
}

struct CommentCollegeFilter;

impl MessageProcessor for CommentCollegeFilter {
    type State = ();
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::FullComment(comment) => {
                if comment.is_college_related() {
                    let post_id = comment.parse_post_id()?;
                    return Some(Message::PostId(post_id));
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }
}
