use std::collections::HashMap;
use std::time::Duration;
use std::{env, thread};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::UdpSocket;
use envconfig::Envconfig;
use tp2::leader_election::leader_election::{LeaderElection};
use tp2::leader_election::tcp::{process_output, process_input, tcp_connect, tcp_listen, send_pong, send_ping, id_to_dataaddr};
use tp2::leader_election::vector::Vector;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
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
    let process_id = env::var("PROCESS_ID").unwrap().parse::<usize>().unwrap();
    let socket = UdpSocket::bind(id_to_dataaddr(process_id)).unwrap();
    let input: Vector<(usize, u8)> = Vector::new();
    let output: Vector<(usize, (usize, u8))> = Vector::new();
    let sockets_lock = Arc::new(RwLock::new(HashMap::new()));
    let sockets_lock_clone = sockets_lock.clone();
    let sockets_lock_clone_2 = sockets_lock.clone();
    let mut input_clone = input.clone();
    let ouput_clone = output.clone();
    let mut election = LeaderElection::new(process_id, output, N_MEMBERS);
    let election_clone = election.clone();
    input_clone = input.clone();
    let tcp_listener = thread::spawn(move || tcp_listen(process_id, input_clone, sockets_lock_clone));
    input_clone = input.clone();
    tcp_connect(process_id, input_clone, sockets_lock, N_MEMBERS);
    thread::spawn(move || process_output(ouput_clone, sockets_lock_clone_2));
    thread::spawn(move || process_input(election_clone, input));
    // End Leader

    let mut running_service = false;
    loop {
        if election.am_i_leader() {
            println!("leader");
            if !running_service {
                running_service = true;
                thread::spawn(move || task_management_clone.run());
                task_management_clone = task_management.clone()
            }
            send_pong(socket.try_clone().unwrap(), N_MEMBERS);
        } else {
            println!("not leader");
            if let Err(_) = send_ping(&socket, election.get_leader_id()) {
                election.find_new()
            }
        }
        thread::sleep(Duration::from_secs(5));
    }

    tcp_listener.join().unwrap();
    
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
