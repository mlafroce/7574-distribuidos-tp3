#[derive(Clone, Copy, PartialEq)]
pub enum HealthMsg {
    Ping = 1,
    Pong,
    Exit,
}

impl TryFrom<u8> for HealthMsg {
    type Error = ();

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(HealthMsg::Ping),
            2 => Ok(HealthMsg::Pong),
            3 => Ok(HealthMsg::Exit),
            _ => Err(())
        }
    }
}
