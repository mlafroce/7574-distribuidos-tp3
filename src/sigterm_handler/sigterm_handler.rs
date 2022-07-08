use std::sync::atomic::{AtomicBool, Ordering};
use signal_hook::{consts::SIGTERM, iterator::Signals};
use std::sync::Arc;

pub fn handle_sigterm(shutdown: Arc<AtomicBool>) {
    let mut signals = Signals::new(&[SIGTERM]).expect("Failed to register SignalsInfo");
    for sig in signals.forever() {
        println!("RECEIVED SIGNAL {}", sig);
        if sig == SIGTERM {
            println!("ENTERED IF {}", sig);
            shutdown.store(true, Ordering::Relaxed);
            break;
        }
    }
}