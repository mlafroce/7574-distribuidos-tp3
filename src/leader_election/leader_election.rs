use std::{
    collections::HashMap,
    io::Write,
    mem::size_of,
    net::{TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use log::info;

use super::socket::Socket;

pub const TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_LIDER: usize = 0;
pub const TEAM_MEMBERS: usize = 3;
pub const MEMBERS: [usize; 3] = [0, 1, 2];

pub fn id_to_dataaddr(process_id: usize) -> String {
    format!("task_management_{}:1235", process_id)
}

const PORT: &str = "1234";
pub struct LeaderElection {
    id: usize,
    members: HashMap<usize, Socket>,
    leader_id: Arc<(Mutex<Option<usize>>, Condvar)>,
    got_ok: Arc<(Mutex<bool>, Condvar)>,
}

impl LeaderElection {
    pub fn new(id: usize) -> LeaderElection {
        let mut election = LeaderElection {
            id: id,
            members: HashMap::new(),
            leader_id: Arc::new((Mutex::new(Some(FIRST_LIDER)), Condvar::new())),
            got_ok: Arc::new((Mutex::new(false), Condvar::new())),
        };

        thread::spawn(move || LeaderElection::listen_members(id));

        let members = MEMBERS.to_vec();

        for peer_id in members[id..].iter() {

            let peer_id_clone = peer_id.clone();

            if *peer_id == id {
                continue;
            }
            loop {
                if let Ok(stream) =
                    TcpStream::connect(&format!("task_management_{}:{}", peer_id, PORT))
                {
                    info!("connected with {}", peer_id);
                    let mut socket = Socket::new(stream.try_clone().unwrap());
                    let socket_clone = socket.clone();
                    LeaderElection::send_id(id, &mut socket);
                    election.members.insert(*peer_id, socket);
                    thread::spawn(move || {
                        LeaderElection::receive_messages(id, peer_id_clone, socket_clone)
                    });
                    break;
                }
            }
        }

        return election;
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

    fn send_id(id: usize, socket: &mut Socket) {
        let mut msg = vec![];
        msg.extend_from_slice(&id.to_le_bytes());
        socket.write(&msg);
    }

    fn receive_id(socket: &mut Socket) -> usize {
        let buffer = socket.read(size_of::<usize>());
        return usize::from_le_bytes(buffer.try_into().unwrap());
    }

    fn send_messages() {}

    fn receive_messages(self_id: usize, from_id: usize, mut socket: Socket) {
        info!("receiving msgs | from: {}", from_id);

        loop {
            let (opcode, peer_id) = LeaderElection::receive(&mut socket);

            match opcode {
                b'O' => {
                    info!("received msg OK from {}", peer_id);
                }
                b'E' => {
                    info!("received msg ELECTION from {}", peer_id);
                    if peer_id < self_id {
                        let msg = LeaderElection::build_msg(b'O', self_id);
                        socket.write(&msg);
                    }
                }
                b'C' => {
                    info!("C")
                }
                _ => {}
            }
        }

        /*

            b'O' => {
                *self.got_ok.0.lock().unwrap() = true;
                self.got_ok.1.notify_all();
            }
            b'E' => {
                if id_from < self.id {
                    socket.write_all(&self.id_to_msg(b'O')).unwrap();
                    //let me = self.clone();
                    //thread::spawn(move || me.find_new());
                }
            }
            b'C' => {
                println!("[{}] recibí nuevo coordinador {}", self.id, id_from);
                *self.leader_id.0.lock().unwrap() = Some(id_from);
                self.leader_id.1.notify_all();
            }
            _ => {}
        }
        */
    }

    fn listen_members(self_id: usize) {
        let listener;
        match TcpListener::bind(format!("task_management_{}:{}", self_id, PORT)) {
            Ok(tcp_listener) => {
                info!("server listening on port {}", PORT);
                listener = tcp_listener
            }
            Err(_) => panic!("could not start socket aceptor"),
        }

        for stream_result in listener.incoming() {
            if let Ok(stream) = stream_result {
                let mut socket = Socket::new(stream);
                let from_id = LeaderElection::receive_id(&mut socket);
                thread::spawn(move || LeaderElection::receive_messages(self_id, from_id, socket));
            }
        }
    }

    pub fn get_leader_id(&self) -> usize {
        let (lock, cvar) = &*self.leader_id;
        match cvar.wait_while(lock.lock().unwrap(), |leader_id| leader_id.is_none()) {
            Ok(id) => {
                return id.unwrap();
            }
            Err(_) => {
                panic!()
            }
        }
    }

    pub fn am_i_leader(&self) -> bool {
        self.get_leader_id() == self.id
    }

    // send msg ELECTION to all process with a greater id
    fn send_election(&mut self) {
        info!("send election");
        let msg = LeaderElection::build_msg(b'E', self.id);

        match self.id {
            0 => {
                if let Some(member) = self.members.get_mut(&1) {
                    member.write(&msg);
                }
                if let Some(member) = self.members.get_mut(&2) {
                    member.write(&msg);
                }
            }
            1 => {
                if let Some(member) = self.members.get_mut(&2) {
                    member.write(&msg);
                }
            }
            2 => {}
            _ => {}
        }
    }

    fn make_me_leader(&mut self) {
        println!("[{}] me anuncio como lider", self.id);

        let msg = LeaderElection::build_msg(b'C', self.id);

        if let Some(member) = self.members.get_mut(&0) {
            member.write(&msg);
        }

        if let Some(member) = self.members.get_mut(&1) {
            member.write(&msg);
        }

        if let Some(member) = self.members.get_mut(&2) {
            member.write(&msg);
        }

        *self.leader_id.0.lock().unwrap() = Some(self.id);
    }

    pub fn find_new(&mut self) {
        println!("[{}] searching lider", self.id);
        *self.got_ok.0.lock().unwrap() = false;
        *self.leader_id.0.lock().unwrap() = None;

        self.send_election();

        match self
            .got_ok
            .1
            .wait_timeout_while(self.got_ok.0.lock().unwrap(), TIMEOUT, |got_it| !*got_it)
        {
            Ok(got_ok) => {
                if !*got_ok.0 {
                    println!("[{}] no recibi ningun ok", self.id);
                    //self.make_me_leader();
                }
            }
            Err(_) => {}
        }
    }
}
