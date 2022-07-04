use envconfig::Envconfig;
use log::info;
use tp2::leader_election::socket::Socket;
use std::collections::HashMap;
use std::mem::size_of;
use std::net::{UdpSocket, TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{env, thread};
use tp2::leader_election::leader_election::{
    id_to_dataaddr, LeaderElection, TEAM_MEMBERS, TIMEOUT,
};
use tp2::leader_election::vector::{Vector};
use tp2::service::init;
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

const PORT: &str = "1234";
const MEMBERS: [usize; 4] = [0, 1, 2, 3];

fn tcp_receive_id(socket: &mut Socket) -> usize{
    let buffer = socket.read(size_of::<usize>());
    return usize::from_le_bytes(buffer.try_into().unwrap());
}

fn send_id(id: usize, socket: &mut Socket) {
    let mut msg = vec![];
    msg.extend_from_slice(&id.to_le_bytes());
    socket.write(&msg);
}

fn build_msg(opcode: u8, id: usize) -> Vec<u8> {
    let mut msg = vec![opcode];
    msg.extend_from_slice(&id.to_le_bytes());
    msg
}

fn receive(socket: &mut Socket) -> (u8, usize) {
    let buffer = socket.read(1 + size_of::<usize>());

    let opcode = buffer[0];
    let peer_id = usize::from_le_bytes(buffer[1..].try_into().unwrap());

    (opcode, peer_id)
}

fn tcp_receive_messages(from_id: usize, socket: &mut Socket, input: Vector<(usize, u8)>) {
    info!("receiving msgs | from: {}", from_id);

    loop {
        let msg = receive(socket);
        input.push((msg.1, msg.0))
    }
}

fn tcp_listen(process_id: usize, vector: Vector<(usize, u8)>, sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>) {
    let mut threads = Vec::new();

    let listener;
    match TcpListener::bind(format!("task_management_{}:{}", process_id, PORT)) {
        Ok(tcp_listener) => {
            info!("server listening on port {}", PORT);
            listener = tcp_listener
        }
        Err(_) => panic!("could not start socket aceptor"),
    }

    let mut vector_clone = vector.clone();
    for stream_result in listener.incoming() {
        if let Ok(stream) = stream_result {
            let mut socket = Socket::new(stream);
            let socket_clone = socket.clone();
            let from_id = tcp_receive_id(&mut socket);
            if let Ok(mut sockets) = sockets_lock.write() {
                sockets.insert(from_id, socket_clone);
            }
            threads.push(thread::spawn(move || tcp_receive_messages(from_id, &mut socket, vector_clone)));
            vector_clone = vector.clone();
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

fn tcp_connect(process_id: usize, input: Vector<(usize, u8)>, sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>) {
    let mut input_clone = input.clone();

    let members = MEMBERS.to_vec();
    
    for peer_id in members[process_id..].iter() {
        let peer_id_clone = peer_id.clone();

        if *peer_id == process_id {
            continue;
        }
        loop {
            if let Ok(stream) = TcpStream::connect(&format!("task_management_{}:{}", peer_id, PORT))
            {
                info!("connected with {}", peer_id);
                let mut socket = Socket::new(stream.try_clone().unwrap());
                let socket_clone = socket.clone();
                if let Ok(mut sockets) = sockets_lock.write() {
                    sockets.insert(peer_id_clone, socket_clone);
                }
                send_id(process_id, &mut socket);
                thread::spawn(move || tcp_receive_messages(peer_id_clone, &mut socket, input_clone));
                input_clone = input.clone();
                break;
            }
        }
    }
}

fn process_input(leader_election: LeaderElection, input: Vector<(usize, u8)>) {
    loop {
        if let Ok(msg_option) = input.pop() {
            if let Some(msg) = msg_option {
                leader_election.process_msg(msg);
            }
        }
    }
}  

fn process_output(output: Vector<(usize, (usize, u8))>, sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>) {
    loop {
        if let Ok(msg_option) = output.pop() {
            if let Some(msg) = msg_option {
                if let Ok(mut sockets) = sockets_lock.write() {
                    if let Some(socket) = sockets.get_mut(&msg.0) {
                        socket.write(&build_msg(msg.1.1, msg.1.0));
                    }
                }
            }
        }
    }
}

fn main() {
    let env_config = init();
    info!("start");
    let process_id = env::var("PROCESS_ID").unwrap().parse::<usize>().unwrap();
    let socket = UdpSocket::bind(id_to_dataaddr(process_id)).unwrap();
    let input: Vector<(usize, u8)> = Vector::new();
    // (to_id, (from_id, opcode))
    let output: Vector<(usize, (usize, u8))> = Vector::new();

    let sockets_lock = Arc::new(RwLock::new(HashMap::new()));
    let sockets_lock_clone = sockets_lock.clone();
    let sockets_lock_clone_2 = sockets_lock.clone();

    let mut input_clone = input.clone();
    let ouput_clone = output.clone();

    let mut election = LeaderElection::new(process_id, output);
    let election_clone = election.clone();
    input_clone = input.clone();
    thread::spawn(move || tcp_listen(process_id, input_clone, sockets_lock_clone));
    input_clone = input.clone();
    tcp_connect(process_id, input_clone, sockets_lock);
    thread::spawn(move || process_output(ouput_clone, sockets_lock_clone_2));
    thread::spawn(move || process_input(election_clone, input));

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
    info!("shutdown");
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
    sec_between_requests: u64,
}

impl TaskManagement {
    pub fn new(
        services: Vec<String>,
        service_port: String,
        timeout_sec: u64,
        sec_between_requests: u64,
    ) -> TaskManagement {
        TaskManagement {
            services,
            service_port,
            timeout_sec,
            sec_between_requests,
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
                let mut task_manager = TaskManager::new(
                    service.clone(),
                    service_port,
                    timeout_sec,
                    sec_between_requests,
                );
                task_manager.run();
            }));
        }
        for task_manager_thread in task_manager_threads {
            task_manager_thread
                .join()
                .expect("Failed to join task manager thread");
        }
    }
}
