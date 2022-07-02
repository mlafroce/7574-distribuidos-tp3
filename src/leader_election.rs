use std::{
    convert::TryInto,
    mem::size_of,
    net::UdpSocket,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

pub const TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_LIDER: usize = 0;
pub const TEAM_MEMBERS: usize = 3;

fn id_to_ctrladdr(process_id: usize) -> String {
    format!("task_management_{}:1234", process_id)
}
pub fn id_to_dataaddr(process_id: usize) -> String {
    format!("task_management_{}:1235", process_id)
}

pub struct LeaderElection {
    id: usize,
    socket: UdpSocket,
    leader_id: Arc<(Mutex<Option<usize>>, Condvar)>,
    got_ok: Arc<(Mutex<bool>, Condvar)>,
}

impl LeaderElection {
    pub fn new(id: usize) -> LeaderElection {

        let election = LeaderElection {
            id: id,
            socket: UdpSocket::bind(id_to_ctrladdr(id)).unwrap(),
            leader_id: Arc::new((Mutex::new(Some(FIRST_LIDER)), Condvar::new())),
            got_ok: Arc::new((Mutex::new(false), Condvar::new())),
        };

        let mut election_clone = election.clone();
        thread::spawn(move || election_clone.responder());

        return election;
    }

    fn clone(&self) -> LeaderElection {
        LeaderElection {
            id: self.id,
            socket: self.socket.try_clone().unwrap(),
            leader_id: self.leader_id.clone(),
            got_ok: self.got_ok.clone(),
        }
    }

    fn responder(&mut self) {
        loop {
            let mut buf = [0; size_of::<usize>() + 1];
            let (_, _) = self.socket.recv_from(&mut buf).unwrap();
            let id_from = usize::from_le_bytes(buf[1..].try_into().unwrap());
            match buf[0] {
                b'O' => {
                    println!("[{}] recibí OK de {}", self.id, id_from);
                    *self.got_ok.0.lock().unwrap() = true;
                    self.got_ok.1.notify_all();
                }
                b'E' => {
                    println!("[{}] recibí Election de {}", self.id, id_from);
                    if id_from < self.id {
                        self.socket
                            .send_to(&self.id_to_msg(b'O'), id_to_ctrladdr(id_from))
                            .unwrap();
                        let me = self.clone();
                        thread::spawn(move || me.find_new());
                    }
                }
                b'C' => {
                    println!("[{}] recibí nuevo coordinador {}", self.id, id_from);
                    *self.leader_id.0.lock().unwrap() = Some(id_from);
                    self.leader_id.1.notify_all();
                }
                _ => {}
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
        for peer_id in (self.id + 1)..TEAM_MEMBERS {
            self.socket.send_to(&msg, id_to_ctrladdr(peer_id)).unwrap();
        }
    }

    fn make_me_leader(&self) {
        println!("[{}] me anuncio como lider", self.id);
        let msg = self.id_to_msg(b'C');
        for peer_id in 0..TEAM_MEMBERS {
            if peer_id != self.id {
                if let Err(_) = self.socket.send_to(&msg, id_to_ctrladdr(peer_id)) {}
            }
        }
        *self.leader_id.0.lock().unwrap() = Some(self.id);
    }

    pub fn find_new(&self) {
        println!("[{}] buscando lider", self.id);
        *self.got_ok.0.lock().unwrap() = false;
        *self.leader_id.0.lock().unwrap() = None;
        self.send_election();

        match self.got_ok.1.wait_timeout_while(
            self.got_ok.0.lock().unwrap(),  
            TIMEOUT, 
            |got_it| !*got_it)
        {
            Ok(got_ok) => {
                if !*got_ok.0 {
                    println!("[{}] no recibi ningun ok", self.id);
                    self.make_me_leader()
                }
            },
            Err(_) => {}
        }
    }
}