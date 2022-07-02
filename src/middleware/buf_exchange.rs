use crate::middleware::connection::BinaryExchange;
use crate::middleware::RabbitExchange;
use amiquip::Result;
use serde::Serialize;
use crate::messages::BulkBuilder;

const MAX_BUF_SIZE: usize = 1_000_000;

pub struct BufExchange<'a> {
    exchange: BinaryExchange<'a>,
    max_buf_size: usize,
    bulk_builder: BulkBuilder,
}

impl<'a> BufExchange<'a> {
    pub fn new(
        exchange: BinaryExchange<'a>,
        routing_key: Option<String>,
    ) -> Self {
        let max_buf_size = MAX_BUF_SIZE;
        let routing_key = routing_key.unwrap_or("".to_string());
        let bulk_builder = BulkBuilder::default();
        Self {
            exchange,
            max_buf_size,
            bulk_builder,
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.bulk_builder.size() > 0 {
            let msg = self.bulk_builder.build();
            self.exchange.send(&msg)
        } else {
            Ok(())
        }
    }
}

impl Drop for BufExchange<'_> {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

impl RabbitExchange for BufExchange<'_> {
    fn send<T>(&mut self, message: &T) -> Result<()>
    where
        T: Serialize,
    {
        self.bulk_builder.push(message);
        if self.bulk_builder.size() > self.max_buf_size {
            return self.flush();
        }
        Ok(())
    }

    fn send_with_key<T>(&mut self, _message: &T, _key: &str) -> Result<()>
    where
        T: Serialize,
    {
        todo!()
    }

    fn end_of_stream(&mut self) -> Result<bool> {
        self.flush()?;
        self.exchange.end_of_stream()
    }
}
