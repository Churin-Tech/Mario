use super::FilePosition;
use crate::{
    commons::{read_yaml_file, struct_to_yaml_string},
    resources::{CF_TASK_CHECKPOINTS, GLOBAL_ROCKSDB},
    tasks::{TaskDefaultParameters, TransferStage},
};
use anyhow::{anyhow, Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileDescription {
    pub path: String,
    pub size: u64,
    pub total_lines: u64,
}

impl Default for FileDescription {
    fn default() -> Self {
        Self {
            path: "".to_string(),
            size: 0,
            total_lines: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckPoint {
    pub task_id: String,
    //当前全量对象列表
    // 对象列表命名规则：OBJECT_LIST_FILE_PREFIX+秒级unix 时间戳 'objeclt_list_unixtimestampe'
    pub executing_file: FileDescription,
    // 文件执行位置，既执行到的offset，用于断点续传
    pub executing_file_position: FilePosition,
    pub file_for_notify: Option<String>,
    pub task_stage: TransferStage,
    // 记录 checkpoint 时点的时间戳
    pub modify_checkpoint_timestamp: i128,
    // 任务起始时间戳，用于后续增量任务
    pub task_begin_timestamp: i128,
}

impl Default for CheckPoint {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            executing_file: Default::default(),
            executing_file_position: FilePosition {
                offset: 0,
                line_num: 0,
            },
            file_for_notify: Default::default(),
            task_stage: TransferStage::Stock,
            modify_checkpoint_timestamp: 0,
            task_begin_timestamp: 0,
        }
    }
}

impl FromStr for CheckPoint {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_yaml::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl CheckPoint {
    pub fn seeked_execute_file(&self) -> Result<File> {
        let mut file = File::open(&self.executing_file.path)?;
        let seek_offset = TryInto::<u64>::try_into(self.executing_file_position.offset)?;
        file.seek(SeekFrom::Start(seek_offset))?;
        Ok(file)
    }
    pub fn save_to(&mut self, path: &str) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        self.modify_checkpoint_timestamp = i128::from(now.as_secs());
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let constent = struct_to_yaml_string(self)?;
        file.write_all(constent.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn save_to_file(&mut self, file: &mut File) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        self.modify_checkpoint_timestamp = i128::from(now.as_secs());
        let constent = struct_to_yaml_string(self)?;
        file.write_all(constent.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn save_to_rocksdb_cf(&mut self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        self.modify_checkpoint_timestamp = i128::from(now.as_secs());
        let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_CHECKPOINTS) {
            Some(cf) => cf,
            None => return Err(anyhow!("content length is None")),
        };
        let encoded: Vec<u8> = bincode::serialize(self)?;
        GLOBAL_ROCKSDB.put_cf(&cf, self.task_id.as_bytes(), encoded)?;

        Ok(())
    }
}

pub fn get_task_checkpoint(checkpoint_file: &str) -> Result<CheckPoint> {
    let checkpoint = read_yaml_file::<CheckPoint>(checkpoint_file)?;
    Ok(checkpoint)
}

#[cfg(test)]
mod test {
    use crate::tasks::modules::get_task_checkpoint;

    //cargo test checkpoint::checkpoint::test::test_get_task_checkpoint -- --nocapture
    #[test]
    fn test_get_task_checkpoint() {
        println!("get_task_checkpoint");
        let c = get_task_checkpoint("/tmp/meta_dir/checkpoint.yml");
        println!("{:?}", c);
    }
}
