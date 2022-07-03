use std::{
    collections::HashMap,
    io::{Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use log::info;

use crate::leader_election::socket::socket_read;

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
    members: HashMap<usize, TcpStream>,
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

        let thread_listen_members = thread::spawn(move || LeaderElection::listen_members(id));

        for peer_id in MEMBERS {
            if peer_id == id {
                continue;
            }
            loop {
                if let Ok(stream) =
                    TcpStream::connect(&format!("task_management_{}:{}", peer_id, PORT))
                {
                    info!("connected with {}", peer_id);
                    election.members.insert(peer_id, stream);
                    break;
                }
            }
        }

        thread_listen_members.join().unwrap();

        return election;
    }

    fn handle_member(socket: &mut TcpStream) {

        loop {
            info!("receiving msgs...");
            let buffer = socket_read(socket);
            info!("msg received: {:?}", buffer);
        }

            /* 
            match buffer[0] {
                b'O' => {
                    println!("[{}] recibí OK de {}", self.id, id_from);
                    *self.got_ok.0.lock().unwrap() = true;
                    self.got_ok.1.notify_all();
                }
                b'E' => {
                    println!("[{}] recibí Election de {}", self.id, id_from);
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

    fn listen_members(id: usize) {
        let mut n = 0;

        let listener;
        match TcpListener::bind(format!("task_management_{}:{}", id, PORT)) {
            Ok(tcp_listener) => {
                info!("server listening on port {}", PORT);
                listener = tcp_listener
            }
            Err(_) => panic!("could not start socket aceptor"),
        }

        for stream_result in listener.incoming() {
            if let Ok(mut stream) = stream_result {
                thread::spawn(move || LeaderElection::handle_member(&mut stream));

                n += 1;

                info!("n: {}", n);

                if n == (MEMBERS.len() - 1) {
                    info!("break ok");
                    break
                }

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

    fn id_to_msg(&self, header: u8) -> Vec<u8> {
        let mut msg = vec![header];
        msg.extend_from_slice(&self.id.to_le_bytes());
        msg
    }

    fn send_election(&self) {
        // P envía el mensaje ELECTION a todos los procesos que tengan número mayor
        let msg = self.id_to_msg(b'E');
        println!("sending len: {}", msg.len());

        match self.id {
            0 => {
                if let Some(mut member) = self.members.get(&1) {
                    member.write_all(&msg).unwrap();
                }
                if let Some(mut member) = self.members.get(&2) {
                    member.write_all(&msg).unwrap();
                }
            }
            1 => {
                if let Some(mut member) = self.members.get(&2) {
                    member.write_all(&msg).unwrap();
                }
            }
            2 => {}
            _ => {}
        }
    }

    fn make_me_leader(&self) {
        println!("[{}] me anuncio como lider", self.id);

        let msg = self.id_to_msg(b'C');

        if let Some(mut member) = self.members.get(&0) {
            member.write_all(&msg).unwrap();
        }

        if let Some(mut member) = self.members.get(&1) {
            member.write_all(&msg).unwrap();
        }

        if let Some(mut member) = self.members.get(&2) {
            member.write_all(&msg).unwrap();
        }

        *self.leader_id.0.lock().unwrap() = Some(self.id);
    }

    pub fn find_new(&self) {
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
                    self.make_me_leader()
                }
            }
            Err(_) => {}
        }
    }
}
