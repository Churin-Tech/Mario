use super::FileDescription;
use super::LogInfo;
use super::RecordDescription;
use super::TaskStopReason;
use super::{
    de_usize_from_str, gen_file_path, se_usize_to_str, CheckPoint, FilePosition, ListedRecord,
    TaskDefaultParameters, TransferStage, OFFSET_PREFIX, TRANSFER_OBJECT_LIST_FILE_PREFIX,
};
use super::{
    task_actions::TransferTaskActions, IncrementAssistant, TransferLocal2Local, TransferLocal2Oss,
    TransferOss2Local, TransferOss2Oss,
};
use crate::commons::quantify_processbar;
use crate::commons::{json_to_struct, LastModifyFilter};
use crate::resources::get_checkpoint;
use crate::tasks::log_out_living_task;
use crate::tasks::save_task_status;
use crate::tasks::task_is_living;
use crate::tasks::GLOBAL_TASKS_EXEC_JOINSET;
use crate::tasks::GLOBAL_TASKS_SYS_JOINSET;
use crate::tasks::GLOBAL_TASK_STOP_MARK_MAP;
use crate::{commons::RegexFilter, s3::OSSDescription, tasks::NOTIFY_FILE_PREFIX};
use anyhow::anyhow;
use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::{
    fs::{self, File},
    io::{self, BufRead},
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
};
use tokio::sync::RwLock;
use tokio::{
    sync::Mutex,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransferTaskStatusType {
    Starting,
    Running(TransferStage),
    Stopped(TaskStopReason),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferTaskStatus {
    pub task_id: String,
    pub start_time: u64,
    pub status: TransferTaskStatusType,
}

impl TransferTaskStatusType {
    pub fn is_starting(&self) -> bool {
        match self {
            TransferTaskStatusType::Starting => true,
            _ => false,
        }
    }

    pub fn is_stock_running(&self) -> bool {
        match self {
            TransferTaskStatusType::Running(ts) => match ts {
                TransferStage::Stock => true,
                TransferStage::Increment => false,
            },
            _ => false,
        }
    }

    pub fn is_running(&self) -> bool {
        match self {
            TransferTaskStatusType::Running(_) => true,
            _ => false,
        }
    }

    pub fn is_increment_running(&self) -> bool {
        match self {
            TransferTaskStatusType::Running(ts) => match ts {
                TransferStage::Stock => false,
                TransferStage::Increment => true,
            },
            _ => false,
        }
    }

    pub fn is_stopped(&self) -> bool {
        match self {
            TransferTaskStatusType::Stopped(_) => true,
            _ => false,
        }
    }

    pub fn is_stopped_finish(&self) -> bool {
        match self {
            TransferTaskStatusType::Stopped(s) => match s {
                TaskStopReason::Finish => true,
                TaskStopReason::Broken => false,
            },
            _ => false,
        }
    }

    pub fn is_stopped_broken(&self) -> bool {
        match self {
            TransferTaskStatusType::Stopped(s) => match s {
                TaskStopReason::Finish => false,
                TaskStopReason::Broken => true,
            },
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TransferType {
    Full,
    Stock,
    Increment,
}

impl TransferType {
    pub fn is_full(&self) -> bool {
        match self {
            TransferType::Full => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_stock(&self) -> bool {
        match self {
            TransferType::Stock => true,
            _ => false,
        }
    }

    pub fn is_increment(&self) -> bool {
        match self {
            TransferType::Increment => true,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
#[serde(rename_all = "lowercase")]
// #[serde(tag = "type")]
pub enum ObjectStorage {
    Local(String),
    OSS(OSSDescription),
}

impl Default for ObjectStorage {
    fn default() -> Self {
        ObjectStorage::OSS(OSSDescription::default())
    }
}

// ToDo 规范属性名称
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferTaskAttributes {
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub target_exists_skip: bool,
    #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    pub start_from_checkpoint: bool,
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub large_file_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub multi_part_chunk_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunks_per_batch_default")]
    pub multi_part_chunks_per_batch: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_parallelism_default")]
    pub multi_part_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::transfer_type_default")]
    pub transfer_type: TransferType,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for TransferTaskAttributes {
    fn default() -> Self {
        Self {
            objects_per_batch: TaskDefaultParameters::objects_per_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            target_exists_skip: TaskDefaultParameters::target_exists_skip_default(),
            start_from_checkpoint: TaskDefaultParameters::target_exists_skip_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk_size: TaskDefaultParameters::multi_part_chunk_size_default(),
            multi_part_chunks_per_batch: TaskDefaultParameters::multi_part_chunks_per_batch_default(
            ),
            multi_part_parallelism: TaskDefaultParameters::multi_part_parallelism_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            transfer_type: TaskDefaultParameters::transfer_type_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferTask {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferTask {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: ObjectStorage::OSS(OSSDescription::default()),
            target: ObjectStorage::OSS(OSSDescription::default()),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

impl TransferTask {
    pub fn gen_transfer_actions(&self) -> Box<dyn TransferTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferLocal2Local {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferLocal2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferOss2Local {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferOss2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
        }
    }

    pub async fn analyze(&self) -> Result<BTreeMap<String, i128>> {
        let task = self.gen_transfer_actions();
        task.analyze_source().await
    }

    //Todo
    // 使用全局joinset，任务启动注册执行joinset和大文件joinset，任务启动时查看承载任务数量是否达到上线
    pub async fn execute(&self) -> Result<()> {
        let task = self.gen_transfer_actions();
        // 从checkpoint 执行，且taskstage 处于增量模式时该标识为true，从上次任务起始时间戳开始抓取变化数据并同步
        let mut exec_modified = false;
        // 执行过程中错误数统计
        let err_counter = Arc::new(AtomicUsize::new(0));
        // 任务停止标准，用于通知所有协程任务结束
        let stop_mark = Arc::new(AtomicBool::new(false));
        GLOBAL_TASK_STOP_MARK_MAP.insert(self.task_id.clone(), stop_mark.clone());

        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        //注册活动任务
        let task_status = TransferTaskStatus {
            task_id: self.task_id.clone(),
            start_time: now.as_secs(),
            status: TransferTaskStatusType::Starting,
        };
        save_task_status(&self.task_id, task_status);

        let mut executed_file = FileDescription {
            path: gen_file_path(
                self.attributes.meta_dir.as_str(),
                TRANSFER_OBJECT_LIST_FILE_PREFIX,
                now.as_secs().to_string().as_str(),
            ),
            size: 0,
            total_lines: 0,
        };

        let assistant = IncrementAssistant::default();
        let increment_assistant = Arc::new(Mutex::new(assistant));
        let regex_filter =
            RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;

        let mut list_file = None;
        let mut list_file_position = FilePosition::default();

        let log_info: LogInfo<String> = LogInfo {
            task_id: self.task_id.clone(),
            msg: "Generating object list ...".to_string(),
            additional: None,
        };
        log::info!("{:?}", log_info);
        // let pd = promote_processbar("Generating object list ...");

        // 生成执行文件
        if self.attributes.start_from_checkpoint {
            // 正在执行的任务数量，用于控制分片上传并行度
            let executing_transfers = Arc::new(RwLock::new(0));
            // 变更object_list_file_name文件名
            let checkpoint = get_checkpoint(&self.task_id)?;

            // 执行error retry
            task.error_record_retry(executing_transfers)?;

            executed_file = checkpoint.executing_file.clone();

            // 清理notify file
            for entry in WalkDir::new(&self.attributes.meta_dir)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|e| !e.file_type().is_dir())
            {
                if let Some(p) = entry.path().to_str() {
                    if p.contains(NOTIFY_FILE_PREFIX) {
                        let _ = fs::remove_file(p);
                    }
                };
            }

            match checkpoint.task_stage {
                TransferStage::Stock => {
                    let f = checkpoint.seeked_execute_file()?;
                    list_file_position = checkpoint.executing_file_position.clone();
                    list_file = Some(f);
                }

                TransferStage::Increment => {
                    // 清理文件重新生成object list 文件需大于指定时间戳,并根据原始object list 删除位于目标端但源端不存在的文件
                    // 流程逻辑
                    // 扫描target 文件list-> 抓取自扫描时间开始，源端的变动数据 -> 生成objlist，action 新增target change capture
                    let modified: FileDescription = task
                        .changed_object_capture_based_target(checkpoint.task_begin_timestamp)
                        .await?;
                    list_file = Some(File::open(&modified.path)?);
                    exec_modified = true;
                }
            }
        } else {
            // 清理 meta 目录
            // 重新生成object list file
            let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());
            executed_file = task
                .gen_source_object_list_file(None, &executed_file.path)
                .await?;
        }

        let log_info = LogInfo::<String> {
            task_id: self.task_id.clone(),
            msg: "object list generated".to_string(),
            additional: None,
        };
        log::info!("{:?}", log_info);

        //注册任务状态为stock
        let task_status = TransferTaskStatus {
            task_id: self.task_id.clone(),
            start_time: now.as_secs(),
            status: TransferTaskStatusType::Running(TransferStage::Stock),
        };
        save_task_status(&self.task_id, task_status);

        let sys_set = Arc::new(RwLock::new(JoinSet::<()>::new()));
        GLOBAL_TASKS_SYS_JOINSET.insert(self.task_id.clone(), sys_set.clone());

        // sys_set 用于执行checkpoint、notify等辅助任务
        // let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        // let mut execut_set = JoinSet::new();
        let task_exec_set = Arc::new(RwLock::new(JoinSet::<()>::new()));
        GLOBAL_TASKS_EXEC_JOINSET.insert(self.task_id.clone(), task_exec_set.clone());
        // 正在执行的任务数量，用于控制分片上传并行度
        let executing_transfers = Arc::new(RwLock::new(0));

        let object_list_file = match list_file {
            Some(f) => f,
            None => File::open(&executed_file.path)?,
        };

        let mut file_for_notify = None;
        // 持续同步逻辑: 执行增量助理
        let task_increment_prelude = self.gen_transfer_actions();

        if self.attributes.transfer_type.is_full() || self.attributes.transfer_type.is_increment() {
            let assistant = Arc::clone(&increment_assistant);
            task::spawn(async move {
                if let Err(e) = task_increment_prelude.increment_prelude(assistant).await {
                    log::error!("{}", e);
                }
            });

            // 当源存储为本地时，获取notify文件
            if let ObjectStorage::Local(_) = self.source {
                while file_for_notify.is_none() {
                    let lock = increment_assistant.lock().await;
                    file_for_notify = match lock.get_notify_file_path() {
                        Some(s) => Some(s),
                        None => None,
                    };
                    drop(lock);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

        if exec_modified {
            let task_modify: Box<dyn TransferTaskActions + Send + Sync> =
                self.gen_transfer_actions();
            let mut vec_keys: Vec<RecordDescription> = vec![];
            // 按列表传输object from source to target
            let lines: io::Lines<io::BufReader<File>> =
                io::BufReader::new(object_list_file).lines();

            for line in lines {
                // 若错误达到上限，则停止任务
                if err_counter.load(std::sync::atomic::Ordering::SeqCst)
                    >= self.attributes.max_errors
                {
                    break;
                }
                if let Result::Ok(l) = line {
                    let record = match json_to_struct::<RecordDescription>(&l) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{}", e);
                            err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            continue;
                        }
                    };
                    vec_keys.push(record);
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_per_batch.to_string())
                {
                    while task_exec_set.read().await.len() >= self.attributes.task_parallelism {
                        task_exec_set.write().await.join_next().await;
                    }

                    let vk: Vec<RecordDescription> = vec_keys.clone();
                    task_modify
                        .record_descriptions_transfor(
                            // &mut execut_set,
                            task_exec_set.clone(),
                            Arc::clone(&executing_transfers),
                            vk,
                            Arc::clone(&stop_mark),
                            Arc::clone(&err_counter),
                            Arc::clone(&offset_map),
                            executed_file.path.clone(),
                        )
                        .await;

                    // 清理临时key vec
                    vec_keys.clear();
                }
            }

            // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
            if vec_keys.len() > 0
                && err_counter.load(std::sync::atomic::Ordering::SeqCst)
                    < self.attributes.max_errors
            {
                while task_exec_set.read().await.len() >= self.attributes.task_parallelism {
                    task_exec_set.write().await.join_next().await;
                }
                let vk = vec_keys.clone();
                task_modify
                    .record_descriptions_transfor(
                        // &mut execut_set,
                        task_exec_set.clone(),
                        Arc::clone(&executing_transfers),
                        vk,
                        Arc::clone(&stop_mark),
                        Arc::clone(&err_counter),
                        Arc::clone(&offset_map),
                        executed_file.path.clone(),
                    )
                    .await;
            }
        } else {
            // 若transfer_type 不为increment，既为 Stock 或 Full则开始执行存量任务
            if !self.attributes.transfer_type.is_increment() {
                // 记录checkpoint
                let lock = increment_assistant.lock().await;
                let notify = lock.get_notify_file_path();
                drop(lock);
                let mut checkpoint: CheckPoint = CheckPoint {
                    task_id: self.task_id.clone(),
                    executing_file: executed_file.clone(),
                    executing_file_position: list_file_position.clone(),
                    file_for_notify: notify,
                    task_stage: TransferStage::Stock,
                    modify_checkpoint_timestamp: i128::from(now.as_secs()),
                    task_begin_timestamp: i128::from(now.as_secs()),
                };
                checkpoint.save_to_rocksdb_cf()?;

                // 启动进度条线程
                let map = Arc::clone(&offset_map);
                let s_m = Arc::clone(&stop_mark);
                let total = executed_file.total_lines;
                let id = self.task_id.clone();
                sys_set.write().await.spawn(async move {
                    // Todo 调整进度条
                    quantify_processbar(id, total, s_m, map, OFFSET_PREFIX).await;
                });

                let task_stock = self.gen_transfer_actions();
                let mut vec_keys: Vec<ListedRecord> = vec![];
                // 按列表传输object from source to target

                let lines: io::Lines<io::BufReader<File>> =
                    io::BufReader::new(object_list_file).lines();

                for (num, line) in lines.enumerate() {
                    // 若错误达到上限，则停止任务
                    if err_counter.load(std::sync::atomic::Ordering::SeqCst)
                        >= self.attributes.max_errors
                    {
                        break;
                    }
                    if let Result::Ok(key) = line {
                        let len = key.bytes().len() + "\n".bytes().len();
                        list_file_position.offset += len;
                        list_file_position.line_num += 1;

                        if !key.ends_with("/") {
                            let record = ListedRecord {
                                key,
                                offset: list_file_position.offset,
                                line_num: list_file_position.line_num,
                            };

                            if regex_filter.filter(&record.key) {
                                vec_keys.push(record);
                            }
                        }
                    };

                    if vec_keys
                        .len()
                        .to_string()
                        .eq(&self.attributes.objects_per_batch.to_string())
                        || (num + 1).eq(&TryInto::<usize>::try_into(total)?)
                            && err_counter.load(std::sync::atomic::Ordering::SeqCst)
                                < self.attributes.max_errors
                    {
                        if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                            match err_counter
                                .load(std::sync::atomic::Ordering::SeqCst)
                                .ge(&self.attributes.max_errors)
                            {
                                true => return Err(anyhow!("task stopped")),
                                false => return Ok(()),
                            }
                        }

                        while task_exec_set.read().await.len() >= self.attributes.task_parallelism {
                            task_exec_set.write().await.join_next().await;
                        }
                        let vk = vec_keys.clone();

                        task_stock
                            .listed_records_transfor(
                                // &mut execut_set,
                                task_exec_set.clone(),
                                Arc::clone(&executing_transfers),
                                vk,
                                Arc::clone(&stop_mark),
                                Arc::clone(&err_counter),
                                Arc::clone(&offset_map),
                                executed_file.path.clone(),
                            )
                            .await;

                        // 清理临时key vec
                        vec_keys.clear();
                    }
                }
            }
        }

        while task_exec_set.read().await.len() > 0 {
            task_exec_set.write().await.join_next().await;
        }

        // 配置停止 offset save 标识为 true
        // snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        if stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
            match err_counter
                .load(std::sync::atomic::Ordering::Relaxed)
                .ge(&self.attributes.max_errors)
            {
                true => return Err(anyhow!("too many errors")),
                false => return Ok(()),
            }
        }

        let modify_checkpoint_timestamp =
            i128::from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs());
        // 记录checkpoint
        let mut checkpoint: CheckPoint = CheckPoint {
            task_id: self.task_id.clone(),
            executing_file: executed_file.clone(),
            executing_file_position: list_file_position.clone(),
            file_for_notify: None,
            task_stage: TransferStage::Stock,
            modify_checkpoint_timestamp,
            task_begin_timestamp: i128::from(now.as_secs()),
        };
        if self.attributes.transfer_type.is_stock() {
            checkpoint.save_to_rocksdb_cf()?;
            log_out_living_task(&self.task_id);
            return Ok(());
        } else {
            checkpoint.task_stage = TransferStage::Increment;
            checkpoint.save_to_rocksdb_cf()?;
        }

        while sys_set.read().await.len() > 0 {
            task::yield_now().await;
            sys_set.write().await.join_next().await;
        }

        let lock = increment_assistant.lock().await;
        let notify = lock.get_notify_file_path();
        drop(lock);

        // 增量逻辑
        if task_is_living(&self.task_id) && !stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
            let executing_transfers = Arc::new(RwLock::new(0));
            let task_increment = self.gen_transfer_actions();

            let _ = task_increment
                .execute_increment(
                    // &mut execut_set,
                    task_exec_set.clone(),
                    executing_transfers,
                    Arc::clone(&increment_assistant),
                    Arc::clone(&err_counter),
                    Arc::clone(&offset_map),
                    stop_mark.clone(),
                )
                .await;
            // 配置停止 offset save 标识为 true
            stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(())
    }
}
