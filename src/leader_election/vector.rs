use std::{collections::VecDeque, sync::{Mutex, Arc, Condvar, RwLock}};

use super::condvar;

#[derive(Clone)]
pub struct Vector<T> {
    pub vector: Arc<RwLock<VecDeque<T>>>,
    pub not_empty: Arc<(Mutex<bool>, Condvar)>,
    pub closed: Arc<RwLock<bool>>
}

impl<T> Vector<T>
{
    pub fn new() -> Vector<T> {
        Vector {
            vector: Arc::new(RwLock::new(VecDeque::new())),
            not_empty: Arc::new((Mutex::new(false), Condvar::new())),
            closed: Arc::new(RwLock::new(false))
        }
    }

    pub fn clone(&self) -> Vector<T> {
        Vector {
            vector: self.vector.clone(),
            not_empty: self.not_empty.clone(),
            closed: self.closed.clone()
        }
    }

    pub fn close(&self) {
        if let Ok(mut closed_) = self.closed.write() {
            *closed_ = true
        }
        condvar::notify_all(self.not_empty.clone(), false);
    }

    pub fn pop(&self) -> Result<Option<T>, String> {
        condvar::wait_while_is_empty(self.not_empty.clone());
        if let Ok(mut messages) = self.vector.write() {
            let message = messages.pop_back();
            if message.is_none() {
                if let Ok(closed_state) = self.closed.read() {
                    if *closed_state {
                        return Err("queue closed".to_string())
                    }
                }
                condvar::notify_all(self.not_empty.clone(), true);
            }
            return Ok(message);
        } else {
            return Ok(None);
        }
    }

    pub fn push(&self, message: T) {
        if let Ok(mut messages) = self.vector.write() {
            messages.push_front(message);
        }
        condvar::notify_one(self.not_empty.clone(), false);
    }
}