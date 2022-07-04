use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use log::info;

use super::vector::Vector;

pub const TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_LIDER: usize = 0;
pub const TEAM_MEMBERS: usize = 3;
pub const MEMBERS: [usize; 3] = [0, 1, 2];

pub fn id_to_dataaddr(process_id: usize) -> String {
    format!("task_management_{}:1235", process_id)
}

#[derive(Clone)]
pub struct LeaderElection {
    id: usize,
    output: Vector<(usize, u8)>,
    leader_id: Arc<(Mutex<Option<usize>>, Condvar)>,
    got_ok: Arc<(Mutex<bool>, Condvar)>,
}

impl LeaderElection {
    pub fn new(id: usize, output: Vector<(usize, u8)>) -> LeaderElection {
        LeaderElection {
            id: id,
            output: output,
            leader_id: Arc::new((Mutex::new(Some(FIRST_LIDER)), Condvar::new())),
            got_ok: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    pub fn clone(&self) -> LeaderElection {
        LeaderElection {
            id: self.id.clone(),
            output: self.output.clone(),
            leader_id: self.leader_id.clone(),
            got_ok: self.got_ok.clone()
        }
    }

    pub fn process_msg(&self, msg: (u8, usize)) {
        let (opcode, id_from) = msg;

        match opcode {
            b'O' => {}
            b'E' => {
                println!("[{}] recibí Election de {}", self.id, id_from);
                if id_from < self.id {
                    self.output.push((id_from, b'O'));
                }
            }
            b'C' => {}
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
        info!("send election");
        match self.id {
            0 => {
                self.output.push((1, b'E'));
                self.output.push((2, b'E'));
            }
            1 => {
                self.output.push((2, b'E'));
            }
            2 => {}
            _ => {}
        }
    }

    fn make_me_leader(&mut self) {
        /*
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
        */
    }

    pub fn find_new(&mut self) {
        info!("[{}] searching lider", self.id);
        *self.got_ok.0.lock().unwrap() = false;
        *self.leader_id.0.lock().unwrap() = None;
        self.send_election();
        /*
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
        */
    }
}
