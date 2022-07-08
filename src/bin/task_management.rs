use std::collections::HashMap;
use std::time::Duration;
use std::{env, thread};
use std::fs::File;
use std::io::{BufRead, BufReader};
use envconfig::Envconfig;
use tp2::leader_election::leader_election::{LeaderElection};
use tp2::leader_election::tcp::{process_output, process_input, tcp_connect, tcp_listen, is_leader_alive};
use tp2::leader_election::vector::Vector;
use tp2::sigterm_handler::sigterm_handler::handle_sigterm;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Mutex, Condvar};
use std::thread::spawn;
use tp2::health_checker::health_answerer::HealthAnswerer;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;
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
    let shutdown_task_management = Arc::new(AtomicBool::new(false));
    let task_management = TaskManagement::new(services, config.service_port, config.timeout_sec, config.sec_between_requests, shutdown_task_management.clone());
    let mut task_management_clone = task_management.clone();

    // begin leader election
    let shutdown = Arc::new(AtomicBool::new(false));
    let process_id = env::var("PROCESS_ID").unwrap().parse::<usize>().unwrap();
    let got_pong = Arc::new((Mutex::new(false), Condvar::new()));
    let input: Vector<(usize, u8)> = Vector::new();
    let output: Vector<(usize, (usize, u8))> = Vector::new();
    let sockets_lock = Arc::new(RwLock::new(HashMap::new()));
    let mut election = LeaderElection::new(process_id, output.clone(), N_MEMBERS);

    let mut shutdown_clone = shutdown.clone();
    let mut got_pong_clone = got_pong.clone();
    let mut sockets_lock_clone = sockets_lock.clone();
    let mut input_clone = input.clone();
    let mut output_clone = output.clone();
    let election_clone = election.clone();


    // handle sigterm
    let signals_handler = thread::spawn(move || handle_sigterm(shutdown_clone));
    shutdown_clone = shutdown.clone();

    // health_answerer
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});

    // listen members - receive msgs from them and deposit into input queue
    let tcp_listener = thread::spawn(move || tcp_listen(process_id, input_clone, sockets_lock_clone, got_pong_clone, shutdown_clone));
    input_clone = input.clone();
    got_pong_clone = got_pong.clone();
    sockets_lock_clone = sockets_lock.clone();
    shutdown_clone = shutdown.clone();

    // connect with members - receive msgs from them and deposit into input queue
    tcp_connect(process_id, input_clone, sockets_lock_clone, N_MEMBERS, got_pong_clone);
    sockets_lock_clone = sockets_lock.clone();
    input_clone = input.clone();
    got_pong_clone = got_pong.clone();

    // do socket send msgs from output queue
    let output_processor = thread::spawn(move || process_output(output_clone, sockets_lock_clone));
    output_clone = output.clone();
    sockets_lock_clone = sockets_lock.clone();

    // consume input queue msgs, and pass the messages to leader_election
    let input_processor = thread::spawn(move || process_input(election_clone, input));

    // to-do: remove this
    thread::sleep(Duration::from_secs(10));

    let mut task_manager_handler = None;
    
    let mut running_service = false;
    loop {
        if election.am_i_leader() {
            debug!("leader");
            if !running_service {
                running_service = true;
                shutdown_task_management.store(false, Ordering::Relaxed);
                task_manager_handler = Some(thread::spawn(move || task_management_clone.run()));
                task_management_clone = task_management.clone()
            }
        } else {
            println!("not leader");           

            running_service = false;
            shutdown_task_management.store(true, Ordering::Relaxed);

            if let Some(leader_id) = election.get_leader_id() {

                if !is_leader_alive(leader_id, sockets_lock_clone, got_pong_clone) {
                    election.find_new();
                }

                sockets_lock_clone = sockets_lock.clone();
                got_pong_clone = got_pong.clone();
            } else {
                election.find_new();
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
    
    if task_manager_handler.is_some() {
        if let Ok(_) = task_manager_handler.unwrap().join() {
            println!("task_manager_handler joined")
        }
    }

    if let Ok(_) = tcp_listener.join() {
        println!("tcp_listener joined");
    }

    if let Ok(_) = input_processor.join() {
        println!("input_processor joined")
    }
    
    if let Ok(_) = output_processor.join() {
        println!("output_processor joined")
    }

    if let Ok(_) = health_answerer_thread.join() {
        println!("health_answerer_thread joined")
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

    pub fn new(services: Vec<String>, service_port: String, timeout_sec: u64, sec_between_requests: u64, shutdown: Arc<AtomicBool>) -> TaskManagement {
        TaskManagement {
            services,
            service_port,
            timeout_sec,
            sec_between_requests,
            shutdown: shutdown
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
    }
}

