use super::{leader_election::LeaderElection, socket::Socket, vector::Vector};
use std::{
    collections::HashMap,
    mem::size_of,
    net::{TcpListener, TcpStream},
    sync::{atomic::AtomicBool, atomic::Ordering, Arc, Condvar, Mutex, RwLock},
    thread::{self, JoinHandle},
    time::Duration,
};

pub fn is_leader_alive(
    leader_id: usize,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
    got_pong: Arc<(Mutex<bool>, Condvar)>,
) -> bool {
    let mut leader_alive = false;

    // println!("send ping: start");
    if let Ok(mut sockets) = sockets_lock.write() {
        if let Some(socket) = sockets.get_mut(&leader_id) {
            send_msg_ping(socket);
        }
    }

    match got_pong.1.wait_timeout_while(
        got_pong.0.lock().unwrap(),
        Duration::from_secs(2),
        |got_it| !*got_it,
    ) {
        Ok(got_ok) => {
            if *got_ok.0 {
                // println!("PONG detected");
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

fn send_msg_pong(socket: &mut Socket) {
    let mut msg = vec![];
    let layer_code: u8 = 0;
    msg.extend_from_slice(&layer_code.to_le_bytes());
    socket.write(&msg);
    msg.clear();
    let pong: u8 = 1;
    msg.extend_from_slice(&pong.to_le_bytes());
    socket.write(&msg);
}

fn send_msg_ping(socket: &mut Socket) {
    let mut msg = vec![];
    let layer_code: u8 = 0;
    msg.extend_from_slice(&layer_code.to_le_bytes());
    socket.write(&msg);
    msg.clear();
    let ping: u8 = 0;
    msg.extend_from_slice(&ping.to_le_bytes());
    socket.write(&msg);
}

fn send_msg(socket: &mut Socket, opcode: u8, id: usize) {
    let mut msg = vec![];
    let layer_code: u8 = 1;
    msg.extend_from_slice(&layer_code.to_le_bytes());
    socket.write(&msg);
    msg.clear();
    msg.extend_from_slice(&opcode.to_le_bytes());
    msg.extend_from_slice(&id.to_le_bytes());
    socket.write(&msg);
}

fn tcp_receive_message(socket: &mut Socket) -> Option<(u8, (u8, usize))> {
    if let Ok(layercode_buffer) = socket.read(1) {
        let layercode = layercode_buffer[0];

        if layercode == 0 {
            if let Ok(ping_or_pong_buffer) = socket.read(1) {
                let ping_or_pong = ping_or_pong_buffer[0];

                return Some((0, (ping_or_pong, 0)));
            }
        } else {
            if let Ok(buffer) = socket.read(1 + size_of::<usize>()) {
                let opcode = buffer[0];
                let peer_id = usize::from_le_bytes(buffer[1..].try_into().unwrap());
                return Some((1, (opcode, peer_id)));
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
) {
    // println!("receiving msgs | from: {}", from_id);

    loop {
        match tcp_receive_message(socket) {
            Some(msg) => {
                // println!("receiving msgs | received: {:?}", msg);
                match msg.0 {
                    0 => {
                        if msg.1.0 == 0 {
                            // println!("received PING");
                            send_msg_pong(socket);
                        }
                        if msg.1.0 == 1 {
                            // println!("received PONG");
                            *got_pong.0.lock().unwrap() = true;
                            got_pong.1.notify_all();
                        }
                    }
                    1 => {
                        let msg_ = msg.1;
                        input.push((msg_.1, msg_.0))
                    }
                    _ => {}
                }
            }
            None => break,
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
    let shutdown_clone = shutdown.clone();
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
                        tcp_receive_messages(from_id, &mut socket, vector_clone, got_pong_clone)
                    }));
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
) {
    let mut input_clone = input.clone();

    let members: Vec<usize> = (0..n_members).collect();

    for peer_id in members[process_id..].iter() {
        let peer_id_clone = peer_id.clone();

        if *peer_id == process_id {
            continue;
        }
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
                thread::spawn(move || {
                    tcp_receive_messages(peer_id_clone, &mut socket, input_clone, got_pong_clone)
                });
                input_clone = input.clone();
                break;
            }
        }
    }
}

pub fn process_input(mut leader_election: LeaderElection, input: Vector<(usize, u8)>) {
    let mut wait_ok: Option<JoinHandle<()>> = None;

    loop {
        match input.pop() {
            Ok(msg_option) => {
                if let Some(msg) = msg_option {
                    if let Some(wait_ok_new) = leader_election.process_msg(msg) {
                        if wait_ok.is_some() {
                            wait_ok.unwrap().join().unwrap();
                        }
                        wait_ok = Some(wait_ok_new);
                    }
                }
            }
            Err(_) => {
                break;
            }
        }
    }

    if wait_ok.is_some() {
        if let Ok(_) = wait_ok.unwrap().join() {
            println!("wait_ok joined")
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
                            send_msg(socket, msg.1 .1, msg.1 .0);
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
