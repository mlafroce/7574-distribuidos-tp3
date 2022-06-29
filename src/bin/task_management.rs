use std::fs::File;
use std::io::{BufRead, BufReader};
use envconfig::Envconfig;
use std::thread;
use tp2::task_manager::task_manager::TaskManager;

fn main() {
    let config = TaskManagementConfig::init_from_env().expect("Failed to read env configuration");
    let filename = config.service_list_file;
    let file = File::open(filename.clone()).expect(&format!("Failed to read file: {}", filename.clone()));
    let reader = BufReader::new(file);
    let services = reader.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect();
    let task_management = TaskManagement::new(services);
    task_management.run();
}


#[derive(Clone, Envconfig)]
pub struct TaskManagementConfig {
    /// Configuration file with the run commands for every service
    #[envconfig(from = "SERVICE_LIST_FILE", default = "service_list")]
    pub service_list_file: String,
}

struct TaskManagement {
    services: Vec<String>
}

impl TaskManagement {

    pub fn new(services: Vec<String>) -> TaskManagement {
        TaskManagement {
            services
        }
    }

    pub fn run(&self) {
        let mut task_manager_threads = Vec::new();
        for service in self.services.clone() {
            // Ver como anda, si va chill, no hace falta usar UDP. Si no, tenerlo en cuenta (?
            task_manager_threads.push(thread::spawn(move || {
                let mut task_manager = TaskManager::new(&service);
                task_manager.run();
            }));
        }
        for task_manager_thread in task_manager_threads {
            task_manager_thread.join().expect("Failed to join task manager thread");
        }
    }
}



