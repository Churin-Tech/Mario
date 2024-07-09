use super::{
    CompareTask, ObjectStorage, TransferTask, TransferType, GLOBAL_TASK_JOINSET,
    GLOBAL_TASK_STOP_MARK_MAP,
};
use crate::{
    commons::{
        byte_size_str_to_usize, byte_size_usize_to_str, json_to_struct, struct_to_json_string,
        LastModifyFilter,
    },
    configure::get_config,
    resources::{CF_TASK, GLOBAL_ROCKSDB},
    s3::OSSDescription,
    tasks::{
        get_live_transfer_task_status, remove_exec_joinset, save_task_status, LogInfo,
        TransferTaskStatusType,
    },
};
use anyhow::{anyhow, Result};
use aws_sdk_s3::types::ObjectIdentifier;
use rocksdb::IteratorMode;
use serde::{
    de::{self},
    Deserialize, Deserializer, Serialize, Serializer,
};
use snowflake::SnowflakeIdGenerator;
use std::time::Duration;
use std::{
    fs::{self, File},
    io::{self, BufRead},
};
use tokio::{runtime, task::JoinSet, time::sleep};

pub const TRANSFER_OBJECT_LIST_FILE_PREFIX: &'static str = "transfer_objects_list_";
pub const COMPARE_SOURCE_OBJECT_LIST_FILE_PREFIX: &'static str = "compare_source_list_";
pub const TRANSFER_CHECK_POINT_FILE: &'static str = "checkpoint_transfer.yml";
pub const COMPARE_CHECK_POINT_FILE: &'static str = "checkpoint_compare.yml";
pub const TRANSFER_ERROR_RECORD_PREFIX: &'static str = "transfer_error_record_";
pub const COMPARE_ERROR_RECORD_PREFIX: &'static str = "compare_error_record_";
pub const COMPARE_RESULT_PREFIX: &'static str = "compare_result_";
pub const OFFSET_PREFIX: &'static str = "offset_";
pub const NOTIFY_FILE_PREFIX: &'static str = "notify_";
pub const REMOVED_PREFIX: &'static str = "removed_";
pub const MODIFIED_PREFIX: &'static str = "modified_";

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TestGlobalJoinsetTask {
    pub task_id: String,
    pub loop_size: usize,
}

impl TestGlobalJoinsetTask {
    pub async fn run(&self, loop_size: usize) {
        while GLOBAL_TASK_JOINSET.read().await.len() > 2 {
            let mut global_set = GLOBAL_TASK_JOINSET.write().await;
            global_set.join_next().await;
        }
        for i in 0..loop_size {
            println!("{}", i);
            log::info!("id:{},{}", self.task_id, i);
            sleep(Duration::from_secs(1)).await;
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct AnalyzedResult {
    pub max: i128,
    pub min: i128,
}
/// 任务阶段，包括存量曾量全量
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TransferStage {
    Stock,
    Increment,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// 任务停止原因，主动停止，或由于错误上线达成停止
pub enum TaskStopReason {
    // 正常结束或人为停止
    Finish,
    // 任务重错误容忍度达到上线
    Broken,
}

/// 任务类别，根据传输方式划分
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    Transfer,
    TruncateBucket,
    Compare,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// #[serde(untagged)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum Task {
    Transfer(TransferTask),
    Compare(CompareTask),
    // TruncateBucket(TaskTruncateBucket),
}

impl Task {
    pub fn task_type(&self) -> TaskType {
        match self {
            Task::Transfer(_) => TaskType::Transfer,
            Task::Compare(_) => TaskType::Compare,
            // Task::TruncateBucket(_) => todo!(),
        }
    }

    pub fn task_source(&self) -> ObjectStorage {
        match self {
            Task::Transfer(t) => t.source.clone(),
            Task::Compare(c) => c.target.clone(),
            // Task::TruncateBucket(_) => todo!(),
        }
    }

    pub fn task_target(&self) -> ObjectStorage {
        match self {
            Task::Transfer(t) => t.target.clone(),
            Task::Compare(c) => c.target.clone(),
            // Task::TruncateBucket(_) => todo!(),
        }
    }

    pub fn set_meta_dir(&mut self, meta_dir: &str) {
        match self {
            Task::Transfer(transfer) => {
                transfer.attributes.meta_dir = meta_dir.to_string();
            }
            Task::Compare(compare) => {
                compare.attributes.meta_dir = meta_dir.to_string();
            }
        }
    }
    pub fn set_task_id(&mut self, task_id: &str) {
        match self {
            Task::Transfer(transfer) => {
                transfer.task_id = task_id.to_string();
            }
            Task::Compare(compare) => {
                compare.task_id = task_id.to_string();
            }
        }
    }

    pub fn task_id(&self) -> String {
        return match self {
            Task::Transfer(transfer) => transfer.task_id.clone(),
            Task::Compare(compare) => compare.task_id.clone(),
        };
    }

    pub fn already_created(&self) -> Result<bool> {
        let mut created = false;
        let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
            Some(cf) => cf,
            None => return Err(anyhow!("column family not exist")),
        };
        let cf_task_iter = GLOBAL_ROCKSDB.iterator_cf(&cf, IteratorMode::Start);

        for item in cf_task_iter {
            if let Ok(kv) = item {
                let task_json_str = String::from_utf8(kv.1.to_vec())?;
                let task = json_to_struct::<Task>(task_json_str.as_str())?;
                if self.task_type().eq(&task.task_type()) {
                    if self.task_source().eq(&task.task_source())
                        && self.task_target().eq(&task.task_target())
                    {
                        created = true
                    }
                }
            }
        }
        Ok(created)
    }

    pub fn stop(&self) -> Result<()> {
        return match self {
            Task::Transfer(_) => {
                let kv = match GLOBAL_TASK_STOP_MARK_MAP.get(&self.task_id()) {
                    Some(kv) => kv,
                    None => {
                        return Err(anyhow!("task {} stop mark not exist", self.task_id()));
                    }
                };
                kv.value().store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
            _ => Err(anyhow!("task not transfer task")),
        };
    }

    pub fn create(&mut self) -> Result<i64> {
        if self.already_created()? {
            return Err(anyhow!("task created"));
        }
        let id = task_id_generator();
        let global_meta_dir = get_config()?.meta_dir;
        let meta_dir = gen_file_path(&global_meta_dir, id.to_string().as_str(), "");
        self.set_task_id(id.to_string().as_str());
        self.set_meta_dir(&meta_dir);

        let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
            Some(cf) => cf,
            None => return Err(anyhow!("column family not exist")),
        };

        let task_json = struct_to_json_string(self)?;
        GLOBAL_ROCKSDB.put_cf(&cf, id.to_string().as_bytes(), task_json.as_bytes())?;
        Ok(id)
    }

    pub async fn execute(&self) {
        match self {
            //Todo
            // 重构task status，使用cf记录
            // 主动停止任务时更新任务为停止状态，执行完成时不更新任务状态
            Task::Transfer(transfer) => {
                match transfer.execute().await {
                    Ok(_) => {
                        //Todo 增加清理逻辑，清理joinset，标志等等
                        let mut transfer_task_status =
                            match get_live_transfer_task_status(&transfer.task_id) {
                                Ok(s) => s,
                                Err(e) => {
                                    log::error!("{}", e);
                                    return;
                                }
                            };
                        transfer_task_status.status =
                            TransferTaskStatusType::Stopped(TaskStopReason::Finish);
                        save_task_status(&transfer.task_id, transfer_task_status);
                        let log_info = LogInfo::<String> {
                            task_id: transfer.task_id.clone(),
                            msg: "execute ok!".to_string(),
                            additional: None,
                        };
                        log::info!("{:?}", log_info)
                    }
                    Err(e) => {
                        let mut transfer_task_status =
                            match get_live_transfer_task_status(&transfer.task_id) {
                                Ok(s) => s,
                                Err(e) => {
                                    log::error!("{}", e);
                                    return;
                                }
                            };
                        transfer_task_status.status =
                            TransferTaskStatusType::Stopped(TaskStopReason::Broken);
                        save_task_status(&transfer.task_id, transfer_task_status);
                        log::error!("{}", e);
                    }
                }
                remove_exec_joinset(&transfer.task_id);
            }

            Task::Compare(compare) => match compare.execute() {
                Ok(_) => {
                    let log_info = LogInfo::<String> {
                        task_id: compare.task_id.clone(),
                        msg: "execute ok!".to_string(),
                        additional: None,
                    };
                    log::info!("{:?}", log_info)
                }
                Err(e) => log::error!("{:?}", e),
            },
        }
    }
}

pub struct TaskDefaultParameters {}

impl TaskDefaultParameters {
    pub fn id_default() -> String {
        task_id_generator().to_string()
    }

    pub fn name_default() -> String {
        "default_name".to_string()
    }

    pub fn objects_per_batch_default() -> i32 {
        100
    }

    pub fn task_parallelism_default() -> usize {
        num_cpus::get()
    }

    pub fn max_errors_default() -> usize {
        1
    }

    pub fn start_from_checkpoint_default() -> bool {
        false
    }

    pub fn exprirs_diff_scope_default() -> i64 {
        10
    }

    pub fn target_exists_skip_default() -> bool {
        false
    }
    pub fn large_file_size_default() -> usize {
        // 50M
        10485760 * 5
    }
    pub fn multi_part_chunk_size_default() -> usize {
        // 10M
        10485760
    }

    pub fn multi_part_chunks_per_batch_default() -> usize {
        10
    }
    pub fn multi_part_parallelism_default() -> usize {
        num_cpus::get() * 2
    }

    pub fn meta_dir_default() -> String {
        "/tmp/meta_dir".to_string()
    }

    pub fn filter_default() -> Option<Vec<String>> {
        None
    }
    pub fn continuous_default() -> bool {
        false
    }

    pub fn transfer_type_default() -> TransferType {
        TransferType::Stock
    }

    pub fn last_modify_filter_default() -> Option<LastModifyFilter> {
        None
    }
}

pub fn de_usize_from_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    byte_size_str_to_usize(&s).map_err(de::Error::custom)
}

pub fn se_usize_to_str<S>(v: &usize, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let size = byte_size_usize_to_str(*v);
    serializer.serialize_str(size.as_str())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskTruncateBucket {
    pub task_id: String,
    pub oss: OSSDescription,
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
}

impl Default for TaskTruncateBucket {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            objects_per_batch: TaskDefaultParameters::objects_per_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            oss: OSSDescription::default(),
        }
    }
}
impl TaskTruncateBucket {
    pub fn exec_multi_threads(&self) -> Result<()> {
        let object_list_file =
            gen_file_path(self.meta_dir.as_str(), TRANSFER_OBJECT_LIST_FILE_PREFIX, "");
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .build()?;

        // 预清理meta目录
        let _ = fs::remove_dir_all(self.meta_dir.as_str());
        let mut interrupted = false;

        rt.block_on(async {
            let client_source = match self.oss.gen_oss_client() {
                Result::Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    interrupted = true;
                    return;
                }
            };
            if let Err(e) = client_source
                .append_object_list_to_file(
                    self.oss.bucket.clone(),
                    self.oss.prefix.clone(),
                    self.objects_per_batch,
                    &object_list_file,
                    None,
                )
                .await
            {
                log::error!("{}", e);
                interrupted = true;
                return;
            };
        });

        if interrupted {
            return Err(anyhow!("get object list error"));
        }

        let mut set: JoinSet<()> = JoinSet::new();
        let file = File::open(object_list_file.as_str())?;

        rt.block_on(async {
            let mut vec_keys: Vec<ObjectIdentifier> = vec![];

            // 按列表传输object from source to target
            let lines = io::BufReader::new(file).lines();
            for line in lines {
                if let Result::Ok(key) = line {
                    if !key.ends_with("/") {
                        if let Ok(obj_id) = ObjectIdentifier::builder().set_key(Some(key)).build() {
                            vec_keys.push(obj_id);
                        }
                    }
                };
                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.objects_per_batch.to_string())
                {
                    while set.len() >= self.task_parallelism {
                        set.join_next().await;
                    }
                    let c = match self.oss.gen_oss_client() {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("{}", e);
                            continue;
                        }
                    };
                    let keys = vec_keys.clone();
                    let bucket = self.oss.bucket.clone();
                    set.spawn(async move {
                        if let Err(e) = c.remove_objects(bucket.as_str(), keys).await {
                            log::error!("{}", e);
                        };
                    });

                    vec_keys.clear();
                }
            }

            if vec_keys.len() > 0 {
                while set.len() >= self.task_parallelism {
                    set.join_next().await;
                }
                let c = match self.oss.gen_oss_client() {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };
                let keys = vec_keys.clone();
                let bucket = self.oss.bucket.clone();
                set.spawn(async move {
                    if let Err(e) = c.remove_objects(bucket.as_str(), keys).await {
                        log::error!("{}", e);
                    };
                });
            }

            if set.len() > 0 {
                set.join_next().await;
            }
        });

        Ok(())
    }
}

pub fn task_id_generator() -> i64 {
    let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
    let id = id_generator_generator.real_time_generate();
    id
}

pub fn gen_file_path(dir: &str, file_prefix: &str, file_subffix: &str) -> String {
    let mut file_name = dir.to_string();
    if dir.ends_with("/") {
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    } else {
        file_name.push_str("/");
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    }
    file_name
}
