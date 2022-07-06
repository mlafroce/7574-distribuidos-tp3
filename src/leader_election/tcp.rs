use super::{leader_election::LeaderElection, socket::Socket, vector::Vector};
use std::{
    collections::HashMap,
    mem::size_of,
    net::{TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex, RwLock},
    thread,
    time::Duration,
};

pub fn is_leader_alive(
    leader_id: usize,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
    got_pong: Arc<(Mutex<bool>, Condvar)>,
) -> bool {
    let mut leader_alive = false;
    println!("send ping: start");
    if let Ok(mut sockets) = sockets_lock.write() {
        if let Some(socket) = sockets.get_mut(&leader_id) {
            println!("send ping: writing");
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
                println!("PONG detected");
                leader_alive = true;
            }
        }
        Err(_) => {}
    }

    *got_pong.0.lock().unwrap() = false;

    return leader_alive    
}

const PORT: &str = "1234";

fn tcp_receive_id(socket: &mut Socket) -> usize {
    let buffer = socket.read(size_of::<usize>());
    return usize::from_le_bytes(buffer.try_into().unwrap());
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

fn receive(socket: &mut Socket) -> (u8, (u8, usize)) {
    println!("receive");
    let layercode_buffer = socket.read(1);
    let layercode = layercode_buffer[0];
    println!("layercode: {}", layercode);

    if layercode == 0 {
        let ping_or_pong_buffer = socket.read(1);
        let ping_or_pong = ping_or_pong_buffer[0];

        return (0, (ping_or_pong, 0));
    } else {
        let buffer = socket.read(1 + size_of::<usize>());

        let opcode = buffer[0];
        let peer_id = usize::from_le_bytes(buffer[1..].try_into().unwrap());

        return (1, (opcode, peer_id));
    }
}

fn tcp_receive_messages(from_id: usize, socket: &mut Socket, input: Vector<(usize, u8)>, got_pong: Arc<(Mutex<bool>, Condvar)>) {
    println!("receiving msgs | from: {}", from_id);

    loop {
        println!("receiving msgs | begin");
        let msg = receive(socket);
        println!("receiving msgs | received: {:?}", msg);

        match msg.0 {
            0 => {
                if msg.1 .0 == 0 {
                    println!("received PING");
                    send_msg_pong(socket);
                }
                if msg.1 .0 == 1 {
                    println!("received PONG");
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
}

pub fn tcp_listen(
    process_id: usize,
    vector: Vector<(usize, u8)>,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
    got_pong: Arc<(Mutex<bool>, Condvar)>
) {
    let mut threads = Vec::new();

    let listener;
    match TcpListener::bind(format!("task_management_{}:{}", process_id, PORT)) {
        Ok(tcp_listener) => {
            println!("server listening on port {}", PORT);
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
            let got_pong_clone = got_pong.clone();
            threads.push(thread::spawn(move || {
                tcp_receive_messages(from_id, &mut socket, vector_clone, got_pong_clone)
            }));
            vector_clone = vector.clone();
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

pub fn tcp_connect(
    process_id: usize,
    input: Vector<(usize, u8)>,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
    n_members: usize,
    got_pong: Arc<(Mutex<bool>, Condvar)>
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

pub fn process_input(leader_election: LeaderElection, input: Vector<(usize, u8)>) {
    loop {
        if let Ok(msg_option) = input.pop() {
            if let Some(msg) = msg_option {
                leader_election.process_msg(msg);
            }
        }
    }
}

pub fn process_output(
    output: Vector<(usize, (usize, u8))>,
    sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>,
) {
    loop {
        if let Ok(msg_option) = output.pop() {
            if let Some(msg) = msg_option {
                if let Ok(mut sockets) = sockets_lock.write() {
                    if let Some(socket) = sockets.get_mut(&msg.0) {
                        send_msg(socket, msg.1 .1, msg.1 .0);
                    }
                }
            }
        }
    }
}
