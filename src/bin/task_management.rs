use std::net::UdpSocket;
use std::time::Duration;
use envconfig::Envconfig;
use log::info;
use tp2::leader_election::leader_election::{LeaderElection, id_to_dataaddr, TEAM_MEMBERS, TIMEOUT};
use tp2::service::init;
use std::{thread, env};
use tp2::task_manager::task_manager::TaskManager;

fn send_pong(socket: UdpSocket) {
    let mut buf = [0; 4];
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    for _ in 0..(TEAM_MEMBERS - 1) {
        if let Ok((_, from)) = socket.recv_from(&mut buf) {
            socket.send_to("PONG".as_bytes(), from).unwrap();
        }
    }
}

fn send_ping(socket: &UdpSocket, leader_id: usize) -> Result<(), ()> {
    let mut buf = [0; 4];
    if let Err(_) = socket.send_to("PING".as_bytes(), id_to_dataaddr(leader_id)) {}
    socket.set_read_timeout(Some(TIMEOUT)).unwrap();
    if let Err(_) = socket.recv_from(&mut buf) {
        Err(())
    } else {
        Ok(())
    }
}

fn main() {
    let env_config = init();
    info!("start");
    let process_id = env::var("PROCESS_ID").unwrap().parse::<usize>().unwrap();
    let socket = UdpSocket::bind(id_to_dataaddr(process_id)).unwrap();
    let election = LeaderElection::new(process_id);

    let mut running_service = false;

    loop {  
        if election.am_i_leader() {
            info!("leader");
            if !running_service {
                running_service = true;
            }
            send_pong(socket.try_clone().unwrap());
        } else {
            info!("not leader");
            if let Err(_) = send_ping(&socket, election.get_leader_id()) {
                election.find_new()
            }
        }
        thread::sleep(Duration::from_secs(5));
    }
    /*
    let config = TaskManagementConfig::init_from_env().expect("Failed to read env configuration");
    let filename = config.service_list_file;
    let file = File::open(filename.clone()).expect(&format!("Failed to read file: {}", filename.clone()));
    let reader = BufReader::new(file);
    let services = reader.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect();
    let task_management = TaskManagement::new(services, config.service_port, config.timeout_sec, config.sec_between_requests);
    task_management.run();
    */
    info!("start");
}

#[derive(Clone, Envconfig)]
pub struct TaskManagementConfig {
    /// Configuration file with the run commands for every service
    #[envconfig(from = "SERVICE_LIST_FILE", default = "service_list")]
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
    sec_between_requests: u64
}

impl TaskManagement {

    pub fn new(services: Vec<String>, service_port: String, timeout_sec: u64, sec_between_requests: u64) -> TaskManagement {
        TaskManagement {
            services,
            service_port,
            timeout_sec,
            sec_between_requests
        }
    }

    pub fn run(&self) {
        let mut task_manager_threads = Vec::new();
        for service in self.services.clone() {
            // Ver como anda, si va chill, no hace falta usar UDP. Si no, tenerlo en cuenta (?
            let service_port = self.service_port.clone();
            let timeout_sec = self.timeout_sec;
            let sec_between_requests = self.sec_between_requests;
            task_manager_threads.push(thread::spawn(move || {
                let mut task_manager = TaskManager::new(service.clone(), service_port, timeout_sec, sec_between_requests);
                task_manager.run();
            }));
        }
        for task_manager_thread in task_manager_threads {
            task_manager_thread.join().expect("Failed to join task manager thread");
        }
    }
}



