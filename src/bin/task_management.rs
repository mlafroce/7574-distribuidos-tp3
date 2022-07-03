use std::fs::File;
use std::io::{BufRead, BufReader};
use envconfig::Envconfig;
use std::thread;
use tp2::task_manager::task_manager::TaskManager;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::spawn;
use signal_hook::{consts::SIGTERM, iterator::Signals};

fn main() {
    let config = TaskManagementConfig::init_from_env().expect("Failed to read env configuration");
    let filename = config.service_list_file;
    let file = File::open(filename.clone()).expect(&format!("Failed to read file: {}", filename.clone()));
    let reader = BufReader::new(file);
    let services = reader.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect();
    let task_management = TaskManagement::new(services, config.service_port, config.timeout_sec, config.sec_between_requests);
    task_management.run();
    println!("Exited gracefully");
}


#[derive(Clone, Envconfig)]
pub struct TaskManagementConfig {
    /// Configuration file with the run commands for every service
    #[envconfig(from = "SERVICE_LIST_FILE", default = "services.txt")]
    pub service_list_file: String,
    #[envconfig(from = "SERVICE_PORT", default = "6789")]
    pub service_port: String,
    #[envconfig(from = "TIMEOUT_SEC", default = "80")]
    pub timeout_sec: u64,
    #[envconfig(from = "SEC_BETWEEN_REQUESTS", default = "5")]
    pub sec_between_requests: u64,
}

struct TaskManagement {
    services: Vec<String>,
    service_port: String,
    timeout_sec: u64,
    sec_between_requests: u64,
    shutdown: Arc<AtomicBool>
}

impl TaskManagement {

    pub fn new(services: Vec<String>, service_port: String, timeout_sec: u64, sec_between_requests: u64) -> TaskManagement {
        TaskManagement {
            services,
            service_port,
            timeout_sec,
            sec_between_requests,
            shutdown: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn run(&self) {
        let mut task_manager_threads = Vec::new();
        let shutdown = self.shutdown.clone();
        let signals_thread = spawn(move || {
            handle_sigterm(shutdown);
        });

        for service in self.services.clone() {
            let service_port = self.service_port.clone();
            let timeout_sec = self.timeout_sec;
            let sec_between_requests = self.sec_between_requests;
            let shutdown = self.shutdown.clone();
            task_manager_threads.push(thread::spawn(move || {
                let mut task_manager = TaskManager::new(service.clone(),
                                                        service_port,
                                                        timeout_sec,
                                                        sec_between_requests,
                                                        shutdown);
                task_manager.run();
            }));
        }
        for task_manager_thread in task_manager_threads {
            task_manager_thread.join().expect("Failed to join task manager thread");
        }
        signals_thread.join().expect("Failed to join signals_thread");
    }
}

fn handle_sigterm(shutdown: Arc<AtomicBool>) {
    println!("HANDLE SIGTERM");
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
