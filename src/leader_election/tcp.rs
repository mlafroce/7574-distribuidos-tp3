use std::{net::{UdpSocket, TcpListener, TcpStream}, time::Duration, mem::size_of, sync::{Arc, RwLock}, collections::HashMap, thread};
use super::{leader_election::{TIMEOUT, LeaderElection}, socket::Socket, vector::Vector};

pub fn id_to_dataaddr(process_id: usize) -> String {
    format!("task_management_{}:1235", process_id)
}

pub fn send_pong(socket: UdpSocket, n_members: usize) {
    let mut buf = [0; 4];
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    for _ in 0..(n_members - 1) {
        if let Ok((_, from)) = socket.recv_from(&mut buf) {
            socket.send_to("PONG".as_bytes(), from).unwrap();
        }
    }
}

pub fn send_ping(socket: &UdpSocket, leader_id: usize) -> Result<(), ()> {
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
    println!("receiving msgs | from: {}", from_id);

    loop {
        let msg = receive(socket);
        input.push((msg.1, msg.0))
    }
}

pub fn tcp_listen(process_id: usize, vector: Vector<(usize, u8)>, sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>) {
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
            threads.push(thread::spawn(move || tcp_receive_messages(from_id, &mut socket, vector_clone)));
            vector_clone = vector.clone();
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

pub fn tcp_connect(process_id: usize, input: Vector<(usize, u8)>, sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>, n_members: usize) {
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
                thread::spawn(move || tcp_receive_messages(peer_id_clone, &mut socket, input_clone));
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

pub fn process_output(output: Vector<(usize, (usize, u8))>, sockets_lock: Arc<RwLock<HashMap<usize, Socket>>>) {
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