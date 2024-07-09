use super::TransferTaskStatus;
use crate::resources::get_checkpoint;
use crate::resources::living_tasks;
use crate::resources::CF_TASK_STATUS;
use crate::resources::GLOBAL_ROCKSDB;
use crate::tasks::FilePosition;
use anyhow::anyhow;
use anyhow::Result;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::{sync::RwLock, task::JoinSet};

pub static GLOBAL_TASK_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    let rocksdb = match init_task_runtime() {
        Ok(db) => db,
        Err(err) => panic!("{}", err),
    };
    Arc::new(rocksdb)
});

pub static GLOBAL_TASK_JOINSET: Lazy<Arc<RwLock<JoinSet<()>>>> = Lazy::new(|| {
    let joinset = init_global_joinset();
    let joinset_rw = RwLock::new(joinset);
    Arc::new(joinset_rw)
});

pub static GLOBAL_TASKS_SYS_JOINSET: Lazy<DashMap<String, Arc<RwLock<JoinSet<()>>>>> =
    Lazy::new(|| {
        let map: DashMap<String, Arc<RwLock<JoinSet<()>>>> = DashMap::new();
        map
    });
pub static GLOBAL_TASKS_EXEC_JOINSET: Lazy<DashMap<String, Arc<RwLock<JoinSet<()>>>>> =
    Lazy::new(|| {
        let map: DashMap<String, Arc<RwLock<JoinSet<()>>>> = DashMap::new();
        map
    });

pub static GLOBAL_TASKS_BIGFILE_JOINSET: Lazy<DashMap<String, Arc<RwLock<JoinSet<()>>>>> =
    Lazy::new(|| {
        let map: DashMap<String, Arc<RwLock<JoinSet<()>>>> = DashMap::new();
        map
    });

pub static GLOBAL_TASK_STOP_MARK_MAP: Lazy<Arc<DashMap<String, Arc<AtomicBool>>>> =
    Lazy::new(|| {
        let map = DashMap::<String, Arc<AtomicBool>>::new();
        Arc::new(map)
    });

pub static GLOBAL_LIVING_TRANSFER_TASK_MAP: Lazy<Arc<DashMap<String, TransferTaskStatus>>> =
    Lazy::new(|| {
        let map = DashMap::<String, TransferTaskStatus>::new();
        Arc::new(map)
    });

// pub static GLOBAL_LIVING_COMPARE_TASK_MAP: Lazy<Arc<DashMap<String, TransferTaskStatus>>> =
//     Lazy::new(|| {
//         let map = DashMap::<String, TransferTaskStatus>::new();
//         Arc::new(map)
//     });

pub static GLOBAL_LIST_FILE_POSITON_MAP: Lazy<Arc<DashMap<String, FilePosition>>> =
    Lazy::new(|| {
        let map = DashMap::<String, FilePosition>::new();
        Arc::new(map)
    });

fn init_task_runtime() -> Result<Runtime> {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .max_io_events_per_tick(32)
        .build()?;
    Ok(rt)
}

fn init_global_joinset() -> JoinSet<()> {
    let set: JoinSet<()> = JoinSet::new();
    set
}

pub struct TasksStatusSaver {
    pub interval: u64,
}

impl TasksStatusSaver {
    pub async fn run(&self) {
        loop {
            //Todo 改造成函数或同步线程
            // for kv in GLOBAL_LIVING_TRANSFER_TASK_MAP.iter() {
            //     // 获取最小offset的FilePosition
            //     let taskid = kv.key();
            //     let mut checkpoint = match get_checkpoint(taskid) {
            //         Ok(c) => c,
            //         Err(e) => {
            //             log::error!("{:?}", e);
            //             continue;
            //         }
            //     };
            //     let mut file_position = FilePosition {
            //         offset: 0,
            //         line_num: 0,
            //     };

            //     GLOBAL_LIST_FILE_POSITON_MAP
            //         .iter()
            //         .filter(|item| item.key().starts_with(taskid))
            //         .map(|m| {
            //             file_position = m.clone();
            //             m.offset
            //         })
            //         .min();

            //     GLOBAL_LIST_FILE_POSITON_MAP.shrink_to_fit();
            //     checkpoint.executing_file_position = file_position.clone();

            //     if let Err(e) = checkpoint.save_to_rocksdb_cf() {
            //         log::error!("{},{}", e, taskid);
            //     } else {
            //         log::debug!("checkpoint:\n{:?}", checkpoint);
            //     };
            // }

            if let Err(e) = snapshot_living_tasks_checkpoints_to_cf().await {
                log::error!("{}", e);
            };
            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
        }
    }
}

pub async fn init_tasks_status_server() {
    let server = TasksStatusSaver { interval: 10 };
    server.run().await
}

pub fn save_task_status(task_id: &str, task_status: TransferTaskStatus) {
    GLOBAL_LIVING_TRANSFER_TASK_MAP.insert(task_id.to_string(), task_status);
}

pub fn log_out_living_task(task_id: &str) {
    GLOBAL_LIVING_TRANSFER_TASK_MAP.remove(task_id);
}

pub fn task_is_living(task_id: &str) -> bool {
    return match GLOBAL_LIVING_TRANSFER_TASK_MAP.get(task_id) {
        Some(ts) => match ts.status {
            super::TransferTaskStatusType::Stopped(_) => false,
            _ => true,
        },
        None => false,
    };
}

pub fn get_live_transfer_task_status(task_id: &str) -> Result<TransferTaskStatus> {
    match GLOBAL_LIVING_TRANSFER_TASK_MAP.get(task_id) {
        Some(kv) => Ok(kv.value().clone()),
        None => {
            return Err(anyhow!("task not living"));
        }
    }
}

pub fn get_exec_joinset(task_id: &str) -> Result<Arc<RwLock<JoinSet<()>>>> {
    let kv = match GLOBAL_TASKS_EXEC_JOINSET.get(task_id) {
        Some(s) => s,
        None => return Err(anyhow!("execute joinset not exist")),
    };
    let exec_set = kv.value().clone();
    Ok(exec_set)
}

pub fn remove_exec_joinset(task_id: &str) {
    GLOBAL_TASKS_EXEC_JOINSET.remove(task_id);
}

pub async fn snapshot_living_tasks_checkpoints_to_cf() -> Result<()> {
    for status in living_tasks()? {
        // 获取最小offset的FilePosition
        let taskid = status.task_id;
        let mut checkpoint = match get_checkpoint(&taskid) {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                continue;
            }
        };
        let mut file_position = FilePosition {
            offset: 0,
            line_num: 0,
        };

        GLOBAL_LIST_FILE_POSITON_MAP
            .iter()
            .filter(|item| item.key().starts_with(&taskid))
            .map(|m| {
                file_position = m.clone();
                m.offset
            })
            .min();

        GLOBAL_LIST_FILE_POSITON_MAP.shrink_to_fit();
        checkpoint.executing_file_position = file_position.clone();

        if let Err(e) = checkpoint.save_to_rocksdb_cf() {
            log::error!("{},{}", e, taskid);
        } else {
            log::debug!("checkpoint:\n{:?}", checkpoint);
        };
    }
    Ok(())
}
