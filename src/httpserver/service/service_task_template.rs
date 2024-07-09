use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    commons::{LastModifyFilter, LastModifyFilterType},
    s3::{OSSDescription, OssProvider},
    tasks::{ObjectStorage, Task, TransferTask},
};

pub fn service_task_template_transfer_oss2oss() -> Result<Task> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let mut transfer_oss2oss = TransferTask::default();
    transfer_oss2oss.name = "transfer_oss2oss".to_string();
    transfer_oss2oss.attributes.task_parallelism = num_cpus::get();

    let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
    let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
    transfer_oss2oss.attributes.exclude = Some(exclude_vec);
    transfer_oss2oss.attributes.include = Some(include_vec);
    let mut oss_desc = OSSDescription::default();
    oss_desc.provider = OssProvider::ALI;
    oss_desc.endpoint = "http://oss-cn-beijing.aliyuncs.com".to_string();
    transfer_oss2oss.source = ObjectStorage::OSS(oss_desc);
    transfer_oss2oss.attributes.last_modify_filter = Some(LastModifyFilter {
        filter_type: LastModifyFilterType::Greater,
        timestamp: i128::from(now.as_secs()),
    });

    let task = Task::Transfer(transfer_oss2oss);
    Ok(task)
}

pub fn service_task_template_transfer_oss2local() -> Result<Task> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let mut transfer_oss2local = TransferTask::default();
    transfer_oss2local.name = "oss2local".to_string();
    transfer_oss2local.target = ObjectStorage::Local("/tmp".to_string());
    transfer_oss2local.attributes.task_parallelism = num_cpus::get();

    let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
    let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
    transfer_oss2local.attributes.exclude = Some(exclude_vec);
    transfer_oss2local.attributes.include = Some(include_vec);
    transfer_oss2local.attributes.last_modify_filter = Some(LastModifyFilter {
        filter_type: LastModifyFilterType::Greater,
        timestamp: i128::from(now.as_secs()),
    });

    let task = Task::Transfer(transfer_oss2local);
    Ok(task)
}

pub fn service_task_template_transfer_local2oss() -> Result<Task> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let mut transfer_local2oss = TransferTask::default();
    transfer_local2oss.name = "transfer_local2oss".to_string();
    transfer_local2oss.source = ObjectStorage::Local("/tmp".to_string());
    transfer_local2oss.attributes.task_parallelism = num_cpus::get();

    let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
    let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
    transfer_local2oss.attributes.exclude = Some(exclude_vec);
    transfer_local2oss.attributes.include = Some(include_vec);
    transfer_local2oss.attributes.last_modify_filter = Some(LastModifyFilter {
        filter_type: LastModifyFilterType::Greater,
        timestamp: i128::from(now.as_secs()),
    });

    let task = Task::Transfer(transfer_local2oss);
    Ok(task)
}

pub fn service_task_template_transfer_local2local() -> Result<Task> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let mut transfer_local2local = TransferTask::default();
    transfer_local2local.name = "transfer_local2local".to_string();
    transfer_local2local.source = ObjectStorage::Local("/tmp/source".to_string());
    transfer_local2local.target = ObjectStorage::Local("/tmp/target".to_string());
    transfer_local2local.attributes.task_parallelism = num_cpus::get();

    let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
    let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
    transfer_local2local.attributes.exclude = Some(exclude_vec);
    transfer_local2local.attributes.include = Some(include_vec);
    transfer_local2local.attributes.last_modify_filter = Some(LastModifyFilter {
        filter_type: LastModifyFilterType::Greater,
        timestamp: i128::from(now.as_secs()),
    });

    let task = Task::Transfer(transfer_local2local);
    Ok(task)
}
