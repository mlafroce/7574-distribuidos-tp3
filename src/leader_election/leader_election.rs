use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};
use super::vector::Vector;

pub const TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_LIDER: usize = 0;

pub fn id_to_dataaddr(process_id: usize) -> String {
    format!("task_management_{}:1235", process_id)
}

#[derive(Clone)]
pub struct LeaderElection {
    id: usize,
    output: Vector<(usize, (usize, u8))>,
    leader_id: Arc<(Mutex<Option<usize>>, Condvar)>,
    got_ok: Arc<(Mutex<bool>, Condvar)>,
    n_members: usize
}

impl LeaderElection {
    pub fn new(id: usize, output: Vector<(usize, (usize, u8))>, n_members: usize) -> LeaderElection {
        LeaderElection {
            id: id,
            output: output,
            leader_id: Arc::new((Mutex::new(Some(FIRST_LIDER)), Condvar::new())),
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
        *self.leader_id.0.lock().unwrap() = id;
        self.leader_id.1.notify_all();
    }

    fn set_got_ok(&self, got_ok: bool) {
        *self.got_ok.0.lock().unwrap() = got_ok;
        self.got_ok.1.notify_all();
    }

    pub fn process_msg(&self, msg: (usize, u8)) {
        let (id_from, opcode) = msg;

        match opcode {
            b'O' => {
                println!("received OK from {}", id_from);
                self.set_got_ok(true);
            }
            b'E' => {
                println!("received ELECTION from {}", id_from);
                if id_from < self.id {
                    self.output.push((id_from, (self.id, b'O')));
                    let mut me = self.clone();
                    thread::spawn(move || me.find_new());
                }
            }
            b'C' => {
                println!("received new COORDINATOR {}", id_from);
                self.set_leader_id(Some(id_from));
            }
            _ => {}
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

    fn send_election(&mut self) {
        println!("send ELECTION");
        for id_peer in (self.id + 1)..self.n_members {
            self.output.push((id_peer, (self.id, b'E')));
        }
    }

    pub fn find_new(&mut self) {
        println!("searching lider");

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
                    println!("any ok received");
                    println!("i'am the new lider");

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