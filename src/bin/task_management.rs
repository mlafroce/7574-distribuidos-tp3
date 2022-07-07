use std::collections::HashMap;
use std::time::Duration;
use std::{env, thread};
use std::fs::File;
use std::io::{BufRead, BufReader};
use envconfig::Envconfig;
use tp2::leader_election::leader_election::{LeaderElection};
use tp2::leader_election::tcp::{process_output, process_input, tcp_connect, tcp_listen, is_leader_alive};
use tp2::leader_election::vector::Vector;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Mutex, Condvar};
use std::thread::spawn;
use signal_hook::{consts::SIGTERM, iterator::Signals};
use tp2::task_manager::task_manager::TaskManager;

pub const N_MEMBERS: usize = 4;

fn main() {
    let config = TaskManagementConfig::init_from_env().expect("Failed to read env configuration");
    let filename = config.service_list_file;
    let file = File::open(filename.clone()).expect(&format!("Failed to read file: {}", filename.clone()));
    let reader = BufReader::new(file);
    let services = reader.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect();
    let task_management = TaskManagement::new(services, config.service_port, config.timeout_sec, config.sec_between_requests);
    let mut task_management_clone = task_management.clone();

    // Begin Leader
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut shutdown_clone = shutdown.clone();
    let signals_handler = thread::spawn(move || handle_sigterm(shutdown_clone));
    let got_pong = Arc::new((Mutex::new(false), Condvar::new()));
    let mut got_pong_clone = got_pong.clone();
    let process_id = env::var("PROCESS_ID").unwrap().parse::<usize>().unwrap();
    let input: Vector<(usize, u8)> = Vector::new();
    let output: Vector<(usize, (usize, u8))> = Vector::new();
    let sockets_lock = Arc::new(RwLock::new(HashMap::new()));
    let sockets_lock_clone = sockets_lock.clone();
    let sockets_lock_clone_2 = sockets_lock.clone();
    let mut sockets_lock_clone_3 = sockets_lock.clone();
    let mut input_clone = input.clone();
    let mut output_clone = output.clone();
    let mut election = LeaderElection::new(process_id, output_clone, N_MEMBERS);
    output_clone = output.clone();
    let election_clone = election.clone();
    shutdown_clone = shutdown.clone();
    let tcp_listener = thread::spawn(move || tcp_listen(process_id, input_clone, sockets_lock_clone, got_pong_clone, shutdown_clone));
    input_clone = input.clone();
    got_pong_clone = got_pong.clone();
    tcp_connect(process_id, input_clone, sockets_lock_clone_2, N_MEMBERS, got_pong_clone);
    input_clone = input.clone();
    let output_processor = thread::spawn(move || process_output(output_clone, sockets_lock_clone_3));
    output_clone = output.clone();
    let input_processor = thread::spawn(move || process_input(election_clone, input));
    // End Leader

    thread::sleep(Duration::from_secs(10));

    let mut running_service = false;
    shutdown_clone = shutdown.clone();
    loop {
        if election.am_i_leader() {
            println!("leader");
            if !running_service {
                running_service = true;
                thread::spawn(move || task_management_clone.run());
                task_management_clone = task_management.clone()
            }
        } else {
            println!("not leader");
            if let Some(leader_id) = election.get_leader_id() {
                sockets_lock_clone_3 = sockets_lock.clone();
                got_pong_clone = got_pong.clone();
                if !is_leader_alive(leader_id, sockets_lock_clone_3, got_pong_clone) {
                    election.find_new();
                }
            }
        }

        let mut exit = false;
        for _ in 0..5 {
            if shutdown_clone.load(Ordering::Relaxed) {
                exit = true;
            }
            thread::sleep(Duration::from_secs(1));
        }
        if exit {
            break;
        }
    }

    input_clone.close();
    output_clone.close();

    if let Ok(_) = tcp_listener.join() {
        println!("tcp_listener joined");
    }

    if let Ok(_) = input_processor.join() {
        println!("input_processor joined")
    }
    
    if let Ok(_) = output_processor.join() {
        println!("output_processor joined")
    }

    if let Ok(_) = signals_handler.join() {
        println!("signals_handler joined")
    }
    
    println!("main exit gracefully");
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

    pub fn clone(&self) -> TaskManagement {
        TaskManagement {
            services: self.services.clone(),
            service_port: self.service_port.clone(),
            timeout_sec: self.timeout_sec.clone(),
            sec_between_requests: self.sec_between_requests.clone(),
            shutdown: self.shutdown.clone()
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
            task_manager_threads.push(spawn(move || {
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
        println!("Every task manager joined...");
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
