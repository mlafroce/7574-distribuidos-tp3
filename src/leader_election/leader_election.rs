use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use super::vector::Vector;

pub const TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_LIDER: usize = 0;
pub const MEMBERS: [usize; 4] = [0, 1, 2, 3];

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

    pub fn process_msg(&self, msg: (usize, u8)) {
        let (id_from, opcode) = msg;

        match opcode {
            b'O' => {
                println!("received OK from {}", id_from);
                *self.got_ok.0.lock().unwrap() = true;
                self.got_ok.1.notify_all();
            }
            b'E' => {
                println!("received ELECTION de {}", id_from);
                if id_from < self.id {
                    self.output.push((id_from, (self.id, b'O')));
                    let mut me = self.clone();
                    thread::spawn(move || me.find_new());
                }
            }
            b'C' => {
                println!("received new COORDINATOR {}", id_from);
                *self.leader_id.0.lock().unwrap() = Some(id_from);
                self.leader_id.1.notify_all();
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

    // send msg ELECTION to all process with a greater id
    fn send_election(&mut self) {
        println!("send election");
        match self.id {
            0 => {
                for id_peer in (0 + 1)..self.n_members {
                    self.output.push((id_peer, (self.id, b'E')));
                } 
            }
            1 => {
                for id_peer in (1 + 1)..self.n_members {
                    self.output.push((id_peer, (self.id, b'E')));
                }
            }
            2 => {
                for id_peer in (2 + 1)..self.n_members {
                    self.output.push((id_peer, (self.id, b'E')));
                }
            }
            _ => {}
        }
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
                    println!("[{}] any ok received", self.id);

                    // make me leader
                    println!("iam the new lider");

                    self.output.push((0, (self.id, b'C')));
                    self.output.push((1, (self.id, b'C')));
                    self.output.push((2, (self.id, b'C')));
                    self.output.push((3, (self.id, b'C')));

                    *self.leader_id.0.lock().unwrap() = Some(self.id);
                }
            }
            Err(_) => {}
        }
    }
}