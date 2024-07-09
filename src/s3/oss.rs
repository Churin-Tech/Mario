use super::oss_client::OssClient;
use anyhow::{Ok, Result};
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_s3::config::Region;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum OssProvider {
    JD,
    JRSS,
    ALI,
    AWS,
    HUAWEI,
    COS,
    MINIO,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OssObjectsList {
    pub object_list: Option<Vec<String>>,
    pub next_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OSSDescription {
    pub provider: OssProvider,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    #[serde(default = "OSSDescription::prefix_default")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

impl Default for OSSDescription {
    fn default() -> Self {
        Self {
            provider: OssProvider::JD,
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            endpoint: "http://s3.cn-north-1.jdcloud-oss.com".to_string(),
            region: "cn-north-1".to_string(),
            bucket: "bucket_name".to_string(),
            prefix: Some("test/samples/".to_string()),
        }
    }
}

impl OSSDescription {
    fn prefix_default() -> Option<String> {
        None
    }
}

impl OSSDescription {
    pub fn gen_oss_client(&self) -> Result<OssClient> {
        match self.provider {
            OssProvider::JD => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .behavior_version(BehaviorVersion::latest())
                    .build();

                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);

                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

                let oss_client = OssClient { client };
                Ok(oss_client)
            }
            OssProvider::ALI => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .behavior_version(BehaviorVersion::latest())
                    .build();

                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let oss_client = OssClient { client };
                Ok(oss_client)
            }

            OssProvider::JRSS => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .behavior_version(BehaviorVersion::latest())
                    .build();

                let s3_config_builder =
                    aws_sdk_s3::config::Builder::from(&shared_config).force_path_style(true);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let oss_client = OssClient { client };
                Ok(oss_client)
            }

            OssProvider::AWS => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .behavior_version(BehaviorVersion::latest())
                    .build();
                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let oss_client = OssClient { client };
                Ok(oss_client)
            }
            OssProvider::HUAWEI => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .behavior_version(BehaviorVersion::latest())
                    .build();
                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let oss_client = OssClient { client };
                Ok(oss_client)
            }

            OssProvider::COS => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .behavior_version(BehaviorVersion::latest())
                    .build();
                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let oss_client = OssClient { client };
                Ok(oss_client)
            }

            OssProvider::MINIO => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .behavior_version(BehaviorVersion::latest())
                    .build();
                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let oss_client = OssClient { client };
                Ok(oss_client)
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use super::{OSSDescription, OssProvider};
    use crate::commons::read_yaml_file;
    use tokio::{runtime, task::JoinSet, time::sleep};

    fn get_jd_oss_description() -> OSSDescription {
        let vec_oss = read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let mut oss_jd = OSSDescription::default();
        for item in vec_oss.iter() {
            if item.provider == OssProvider::JD {
                oss_jd = item.clone();
            }
        }
        oss_jd
    }

    //cargo test s3::oss::test::test_tokio_multi_thread -- --nocapture
    #[test]
    fn test_tokio_multi_thread() {
        let max_task = 2;
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(max_task)
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut set = JoinSet::new();
            for i in 0..100 {
                println!("run {}", i);
                while set.len() >= max_task {
                    set.join_next().await;
                }
                set.spawn(async move {
                    sleep(Duration::from_secs(2)).await;
                    println!("spawn {}", i);
                });
            }
            while set.len() >= max_task {
                set.join_next().await;
            }
        });
    }
}
