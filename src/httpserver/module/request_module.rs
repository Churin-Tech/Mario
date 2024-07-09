use serde::Deserialize;
use strum_macros::{Display, EnumString};

#[derive(EnumString, Display, Debug, PartialEq, Deserialize)]
pub enum Option {
    Put,
    Del,
    Get,
}

#[derive(Debug, Deserialize)]
pub struct ReqScan {
    pub begin: String,
    pub end: String,
    pub limited: u32,
}
