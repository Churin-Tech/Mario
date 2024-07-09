use super::{TaskStopReason, TaskType, TransferStage};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Status {
    Transfer(TransferStatus),
    Compare(CompareStatus),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskStatus {
    pub task_id: String,
    pub start_time: u64,
    pub status: Status,
}

impl TaskStatus {
    pub fn status_type(&self) -> TaskType {
        match self.status {
            Status::Transfer(_) => TaskType::Transfer,
            Status::Compare(_) => TaskType::Compare,
        }
    }

    pub fn is_starting(&self) -> bool {
        match &self.status {
            Status::Transfer(t) => match t {
                TransferStatus::Starting => true,
                _ => false,
            },
            Status::Compare(c) => match c {
                CompareStatus::Starting => true,
                _ => false,
            },
        }
    }

    pub fn is_running(&self) -> bool {
        match &self.status {
            Status::Transfer(t) => match t {
                TransferStatus::Running(_) => true,
                _ => false,
            },
            Status::Compare(c) => match c {
                CompareStatus::Running => true,
                _ => false,
            },
        }
    }

    pub fn is_stopped(&self) -> bool {
        match &self.status {
            Status::Transfer(t) => match t {
                TransferStatus::Stopped(_) => true,
                _ => false,
            },
            Status::Compare(c) => match c {
                CompareStatus::Stopped => true,
                _ => false,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransferStatus {
    Starting,
    Running(TransferStage),
    Stopped(TaskStopReason),
}

impl TransferStatus {
    pub fn is_running_stock(&self) -> bool {
        match self {
            TransferStatus::Running(r) => match r {
                TransferStage::Stock => true,
                TransferStage::Increment => false,
            },
            _ => false,
        }
    }

    pub fn is_running_increment(&self) -> bool {
        match self {
            TransferStatus::Running(r) => match r {
                TransferStage::Stock => false,
                TransferStage::Increment => true,
            },
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CompareStatus {
    Starting,
    Running,
    Stopped,
}
