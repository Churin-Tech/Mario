use crate::tasks::{get_live_transfer_task_status, FilePosition};
use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::{
    cmp::min,
    fmt::Write,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tokio::task::yield_now;

/// 进度条，使用时在主线程之外的线程使用
pub async fn quantify_processbar(
    task_id: String,
    total: u64,
    stop_mark: Arc<AtomicBool>,
    status_map: Arc<DashMap<String, FilePosition>>,
    key_prefix: &str,
) {
    let pb = ProgressBar::new(total);
    let progress_style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
    )
    .unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("#>-");
    pb.set_style(progress_style);

    while !stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
        let task_status = match get_live_transfer_task_status(&task_id) {
            Ok(s) => s,
            Err(_) => return,
        };

        if !task_status.status.is_stock_running() {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        let line_num = status_map
            .iter()
            .filter(|f| f.key().starts_with(&task_id))
            .map(|m| m.line_num)
            .min();
        match line_num {
            Some(current) => {
                let new = min(current, total);
                pb.set_position(new);
                log::info!("total:{},executed:{}", total, new)
            }
            None => {}
        }
        yield_now().await;
    }
    log::info!("total:{},executed:{}", total, total);
    pb.set_position(total);
    pb.finish_with_message("Finish");
}

pub fn promote_processbar(message: &str) -> ProgressBar {
    let pd = ProgressBar::new_spinner();
    pd.enable_steady_tick(Duration::from_millis(200));
    pd.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&[
                "▰▱▱▱▱▱▱",
                "▰▰▱▱▱▱▱",
                "▰▰▰▱▱▱▱",
                "▰▰▰▰▱▱▱",
                "▰▰▰▰▰▱▱",
                "▰▰▰▰▰▰▱",
                "▰▰▰▰▰▰▰",
            ]),
    );
    pd.set_message(message.to_string());
    pd
}
