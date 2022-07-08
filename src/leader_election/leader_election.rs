use std::{
    sync::{Arc, Condvar, Mutex, RwLock},
    time::Duration, thread::{self},
};
use super::vector::Vector;

pub const TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone)]
pub struct LeaderElection {
    id: usize,
    output: Vector<(usize, (usize, u8))>,
    leader_id: Arc<RwLock<Option<usize>>>,
    got_ok: Arc<(Mutex<bool>, Condvar)>,
    n_members: usize
}

impl LeaderElection {
    pub fn new(id: usize, output: Vector<(usize, (usize, u8))>, n_members: usize) -> LeaderElection {
        LeaderElection {
            id: id,
            output: output,
            leader_id: Arc::new(RwLock::new(None)),
            got_ok: Arc::new((Mutex::new(false), Condvar::new())),
            n_members: n_members
        }
    }

    pub fn clone(&self) -> LeaderElection {
        LeaderElection {
            id: self.id.clone(),
            output: self.output.clone(),
            leader_id: self.leader_id.clone(),
            got_ok: self.got_ok.clone(),
            n_members: self.n_members.clone()
        }
    }

    fn set_leader_id(&self, id: Option<usize>) {
        if let Ok(mut leader_id) = self.leader_id.write() {
            *leader_id = id;
        }
    }

    fn set_got_ok(&self, got_ok: bool) {
        *self.got_ok.0.lock().unwrap() = got_ok;
        self.got_ok.1.notify_all();
    }

    pub fn process_msg(&mut self, msg: (usize, u8)) {
        let (id_from, opcode) = msg;

        match opcode {
            b'O' => {
                // received OK
                self.set_got_ok(true);
            }
            b'E' => {
                // received ELECTION
                if id_from < self.id {
                    self.output.push((id_from, (self.id, b'O')));
                    let mut me = self.clone();
                    thread::spawn(move || me.find_new());
                }
            }
            b'C' => {
                // received new COORDINATOR
                self.set_leader_id(Some(id_from));
            }
            _ => {}
        }
    }

    pub fn get_leader_id(&self) -> Option<usize> {
        if let Ok(leader_id) = self.leader_id.read() {
            return *leader_id
        }
        None
    }

    pub fn am_i_leader(&self) -> bool {
        self.get_leader_id() == Some(self.id)
    }

    fn send_election(&mut self) {
        // send ELECTION
        for id_peer in (self.id + 1)..self.n_members {
            self.output.push((id_peer, (self.id, b'E')));
        }
    }

    pub fn find_new(&mut self) {
        // searching lider

        self.set_got_ok(false);
        self.set_leader_id(None);

        self.send_election();

        match self
            .got_ok
            .1
            .wait_timeout_while(self.got_ok.0.lock().unwrap(), TIMEOUT, |got_it| !*got_it)
        {
            Ok(got_ok) => {
                if !*got_ok.0 {
                    // any ok received
                    // i'am the new lider

                    for peer_id in 0..self.n_members {
                        self.output.push((peer_id, (self.id, b'C')));
                    }

                    self.set_leader_id(Some(self.id));
                }
            }
            Err(_) => {}
        }
    }
}