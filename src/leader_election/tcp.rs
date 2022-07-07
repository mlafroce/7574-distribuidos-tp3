use super::{leader_election::LeaderElection, socket::Socket, vector::Vector};
use std::{
    collections::HashMap,
    mem::size_of,
    net::{TcpListener, TcpStream},
    sync::{atomic::AtomicBool, atomic::Ordering, Arc, Condvar, Mutex, RwLock},
    thread::{self},
    time::Duration,
};

pub fn is_leader_alive(
    leader_id: usize,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
    got_pong: Arc<(Mutex<bool>, Condvar)>,
) -> bool {
    let mut leader_alive = false;

    // send PING
    if let Ok(mut sockets) = sockets_lock.write() {
        if let Some(socket) = sockets.get_mut(&leader_id) {
            tcp_send_msg_ping(socket);
        }
    }

    // wait PONG
    match got_pong.1.wait_timeout_while(
        got_pong.0.lock().unwrap(),
        Duration::from_secs(2),
        |got_it| !*got_it,
    ) {
        Ok(got_ok) => {
            if *got_ok.0 {
                leader_alive = true;
            }
        }
        Err(_) => {}
    }

    *got_pong.0.lock().unwrap() = false;

    return leader_alive;
}

const PORT: &str = "1234";

fn tcp_receive_id(socket: &mut Socket) -> Option<usize> {
    if let Ok(buffer) = socket.read(size_of::<usize>()) {
        return Some(usize::from_le_bytes(buffer.try_into().unwrap()));
    }
    None
}

fn send_id(id: usize, socket: &mut Socket) {
    let mut msg = vec![];
    msg.extend_from_slice(&id.to_le_bytes());
    socket.write(&msg);
}

/* Ping Pong */
const LAYER_PING_PONG: u8 = 0;
const PING: u8 = 0;
const PONG: u8 = 1;
/* */
 
const LAYER_LEADER_ELECTION: u8 = 1;

fn tcp_send_msg_pong(socket: &mut Socket) {
    let mut msg = vec![];
    let layer_code: u8 = LAYER_PING_PONG;
    msg.extend_from_slice(&layer_code.to_le_bytes());
    socket.write(&msg);
    msg.clear();
    let pong: u8 = PONG;
    msg.extend_from_slice(&pong.to_le_bytes());
    socket.write(&msg);
}

fn tcp_send_msg_ping(socket: &mut Socket) {
    let mut msg = vec![];
    let layer_code: u8 = LAYER_PING_PONG;
    msg.extend_from_slice(&layer_code.to_le_bytes());
    socket.write(&msg);
    msg.clear();
    let ping: u8 = PING;
    msg.extend_from_slice(&ping.to_le_bytes());
    socket.write(&msg);
}

fn tcp_send_msg(socket: &mut Socket, opcode: u8, id: usize) {
    let mut msg = vec![];
    let layer_code: u8 = LAYER_LEADER_ELECTION;
    msg.extend_from_slice(&layer_code.to_le_bytes());
    socket.write(&msg);
    msg.clear();
    msg.extend_from_slice(&opcode.to_le_bytes());
    socket.write(&msg);
    msg.clear();
    msg.extend_from_slice(&id.to_le_bytes());
    socket.write(&msg);
}

fn tcp_receive_message(socket: &mut Socket) -> Option<(u8, (u8, usize))> {
    if let Ok(layercode_buffer) = socket.read(1) {
        let layercode = layercode_buffer[0];

        if layercode == LAYER_PING_PONG {
            if let Ok(ping_or_pong_buffer) = socket.read(1) {
                let ping_or_pong = ping_or_pong_buffer[0];

                return Some((LAYER_PING_PONG, (ping_or_pong, 0)));
            }
        }

        if layercode == LAYER_LEADER_ELECTION {
            if let Ok(opcode_buffer) = socket.read(1) {
                let opcode = opcode_buffer[0];
                if let Ok(buffer) = socket.read(size_of::<usize>()) {
                    let peer_id = usize::from_le_bytes(buffer.try_into().unwrap());
                    return Some((LAYER_LEADER_ELECTION, (opcode, peer_id)));
                }
            }
        }
    }
    None
}

fn tcp_receive_messages(
    from_id: usize,
    socket: &mut Socket,
    input: Vector<(usize, u8)>,
    got_pong: Arc<(Mutex<bool>, Condvar)>,
    shutdown: Arc<AtomicBool>
) {
    loop {
        let msg_option = tcp_receive_message(socket);

        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        match msg_option {
            Some(msg) => {
                match msg.0 {
                    LAYER_PING_PONG => {
                        if msg.1.0 == PING {
                            tcp_send_msg_pong(socket);
                        }
                        if msg.1.0 == PONG {
                            *got_pong.0.lock().unwrap() = true;
                            got_pong.1.notify_all();
                        }
                    }
                    LAYER_LEADER_ELECTION => {
                        let msg_ = msg.1;
                        input.push((msg_.1, msg_.0))
                    }
                    _ => {
                    }
                }
            }
            None => {
                break
            }
        }
    }
}

pub fn tcp_listen(
    process_id: usize,
    vector: Vector<(usize, u8)>,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
    got_pong: Arc<(Mutex<bool>, Condvar)>,
    shutdown: Arc<AtomicBool>,
) {
    let mut tcp_receivers = Vec::new();

    let listener;
    match TcpListener::bind(format!("task_management_{}:{}", process_id, PORT)) {
        Ok(tcp_listener) => {
            println!("server listening on port {}", PORT);
            listener = tcp_listener
        }
        Err(_) => panic!("could not start socket aceptor"),
    }

    if let Err(_) = listener.set_nonblocking(true) {
        panic!("could not set listener as non blocking")
    }

    let mut vector_clone = vector.clone();
    let mut shutdown_clone = shutdown.clone();
    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let mut socket = Socket::new(stream);
                let socket_clone = socket.clone();
                if let Some(from_id) = tcp_receive_id(&mut socket) {
                    if let Ok(mut sockets) = sockets_lock.write() {
                        sockets.insert(from_id, socket_clone);
                    }
                    let got_pong_clone = got_pong.clone();
                    tcp_receivers.push(thread::spawn(move || {
                        tcp_receive_messages(from_id, &mut socket, vector_clone, got_pong_clone, shutdown_clone)
                    }));

                    shutdown_clone = shutdown.clone();
                    vector_clone = vector.clone();
                }
            }
            Err(_) => {
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                } else {
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            }
        }
    }

    for tcp_receiver in tcp_receivers {
        if let Ok(_) = tcp_receiver.join() {
            println!("tcp_receive_messages joined");
        }
    }

    println!("tcp_listen exit gracefully");
}

pub fn tcp_connect(
    process_id: usize,
    input: Vector<(usize, u8)>,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
    n_members: usize,
    got_pong: Arc<(Mutex<bool>, Condvar)>,
    shutdown: Arc<AtomicBool>
) {
    let mut v = Vec::new();

    let mut input_clone = input.clone();
    let mut shutdown_clone = shutdown.clone();

    let members: Vec<usize> = (0..n_members).collect();

    for peer_id in members[(process_id + 1)..].iter() {
        let peer_id_clone = peer_id.clone();

        loop {
            if let Ok(stream) = TcpStream::connect(&format!("task_management_{}:{}", peer_id, PORT))
            {
                println!("connected with {}", peer_id);
                let mut socket = Socket::new(stream.try_clone().unwrap());
                let socket_clone = socket.clone();
                if let Ok(mut sockets) = sockets_lock.write() {
                    sockets.insert(peer_id_clone, socket_clone);
                }
                send_id(process_id, &mut socket);
                let got_pong_clone = got_pong.clone();
                v.push(thread::spawn(move || {
                    tcp_receive_messages(peer_id_clone, &mut socket, input_clone, got_pong_clone, shutdown_clone)
                }));

                shutdown_clone = shutdown.clone();
                input_clone = input.clone();
                break;
            }
        }
    }

    for t in v {
        t.join().unwrap();
    }
}

pub fn process_input(mut leader_election: LeaderElection, input: Vector<(usize, u8)>) {
    loop {
        match input.pop() {
            Ok(msg_option) => {
                if let Some(msg) = msg_option {
                    leader_election.process_msg(msg);
                }
            }
            Err(_) => {
                break;
            }
        }
    }

    println!("process_input exit gracefully")
}

pub fn process_output(
    output: Vector<(usize, (usize, u8))>,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
) {
    loop {
        match output.pop() {
            Ok(msg_option) => {
                if let Some(msg) = msg_option {
                    if let Ok(mut sockets) = sockets_lock.write() {
                        if let Some(socket) = sockets.get_mut(&msg.0) {
                            tcp_send_msg(socket, msg.1 .1, msg.1 .0);
                        }
                    }
                }
            }
            Err(_) => {
                break;
            }
        }
    }

    println!("process_output exit gracefully")
}
