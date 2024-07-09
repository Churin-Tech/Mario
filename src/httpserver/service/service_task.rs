use crate::{
    commons::{json_to_struct, struct_to_json_string},
    configure::get_config,
    httpserver::module::RespListTask,
    resources::{get_checkpoint, get_task, CF_TASK, GLOBAL_ROCKSDB},
    tasks::{gen_file_path, task_is_living, CheckPoint, Task, GLOBAL_TASK_RUNTIME},
};
use anyhow::anyhow;
use anyhow::Result;
use rocksdb::IteratorMode;
use std::{collections::BTreeMap, fs};

pub fn service_task_create(task: &mut Task) -> Result<i64> {
    task.create()
}

pub fn service_remove_task(task_ids: Vec<String>) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };

    for id in task_ids {
        let global_meta_dir = get_config()?.meta_dir;
        let meta_dir = gen_file_path(&global_meta_dir, id.as_str(), "");
        GLOBAL_ROCKSDB.delete_cf(&cf, id)?;
        fs::remove_dir(meta_dir)?
    }

    Ok(())
}

pub fn service_update_task(task_id: &str, task: &mut Task) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let global_meta_dir = get_config()?.meta_dir;
    let meta_dir = gen_file_path(&global_meta_dir, task_id, "");
    task.set_task_id(task_id);
    task.set_meta_dir(&meta_dir);
    let task_json = struct_to_json_string(task)?;
    GLOBAL_ROCKSDB.put_cf(&cf, task_id.to_string().as_bytes(), task_json.as_bytes())?;
    Ok(())
}

pub fn service_start_task(task_id: &str) -> Result<()> {
    let task = get_task(task_id)?;
    if task_is_living(task_id) {
        return Err(anyhow!("task {} is living", task_id));
    }
    GLOBAL_TASK_RUNTIME.spawn(async move { task.execute().await });
    // 检查任务生存状态
    Ok(())
}

pub fn service_stop_task(task_id: &str) -> Result<()> {
    if !task_is_living(task_id) {
        return Err(anyhow!("task not living"));
    }
    let task = get_task(task_id)?;
    task.stop()
    // return match task_is_living(task_id) {
    //     true => match GLOBAL_TASK_STOP_MARK_MAP.get_mut(task_id) {
    //         Some(mask) => {
    //             mask.value()
    //                 .store(true, std::sync::atomic::Ordering::SeqCst);
    //             log_out_living_task(task_id);
    //             Ok(())
    //         }
    //         None => Err(anyhow!("task stop mask not exist")),
    //     },
    //     false => Ok(()),
    // };
}

pub async fn service_analyze_task(task_id: &str) -> Result<BTreeMap<String, i128>> {
    let task = service_show_task(task_id)?;
    match task {
        Task::Transfer(t) => {
            let r = t.gen_transfer_actions().analyze_source().await?;
            Ok(r)
        }
        _ => Err(anyhow!("task not transfer task")),
    }
}

pub fn service_show_task(task_id: &str) -> Result<Task> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };

    let value = GLOBAL_ROCKSDB.get_cf(&cf, task_id)?;
    return match value {
        Some(v) => {
            let task_json_str = String::from_utf8(v)?;
            let task = json_to_struct::<Task>(task_json_str.as_str())?;
            Ok(task)
        }
        None => Err(anyhow!("task {} not exist", task_id)),
    };
}

pub fn service_task_checkpoint(task_id: &str) -> Result<CheckPoint> {
    get_checkpoint(task_id)
}

pub fn service_list_all_tasks() -> Result<Vec<RespListTask>> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let cf_task_iter = GLOBAL_ROCKSDB.iterator_cf(&cf, IteratorMode::Start);
    let mut vec_task = vec![];
    for item in cf_task_iter {
        if let Ok(kv) = item {
            let cf_id = String::from_utf8(kv.0.to_vec())?;
            let task_json_str = String::from_utf8(kv.1.to_vec())?;
            let task = json_to_struct::<Task>(task_json_str.as_str())?;
            let resp = RespListTask { cf_id, task };
            vec_task.push(resp);
        }
    }
    Ok(vec_task)
}
