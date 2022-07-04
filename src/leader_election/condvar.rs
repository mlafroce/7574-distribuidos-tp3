use std::sync::{Mutex, Condvar, Arc};

pub fn notify_all(pair: Arc<(Mutex<bool>, Condvar)>, new_state: bool) {
    let (lock, cvar) = &*pair;
    match lock.lock() {
        Ok(mut emtpy) => {
            *emtpy = new_state;
            cvar.notify_all();
        }
        Err(_) => {}
    }
}

pub fn notify_one(pair: Arc<(Mutex<bool>, Condvar)>, new_state: bool) {
    let (lock, cvar) = &*pair;
    match lock.lock() {
        Ok(mut emtpy) => {
            *emtpy = new_state;
            cvar.notify_one();
        }
        Err(_) => {}
    }
}

pub fn wait_while_is_empty(pair: Arc<(Mutex<bool>, Condvar)>) {
    let (lock, cvar) = &*pair;
    match cvar.wait_while(lock.lock().unwrap(), |empty| {
        *empty == true
    }) {
        Ok(_) => {
        }
        Err(_) => {
        }
    }
}