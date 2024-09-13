// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(not(windows))]
pub mod cache;

use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry,
};
use datafusion::execution::runtime_env::RuntimeConfig;
#[cfg(any(feature = "hdfs", feature = "hdfs3"))]
use datafusion_objectstore_hdfs::object_store::hdfs::HadoopFileSystem;
#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "azure")]
use object_store::azure::MicrosoftAzureBuilder;
#[cfg(feature = "gcs")]
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;
use std::sync::Arc;
use object_store::local::LocalFileSystem;
use url::Url;
use aws_sdk_s3::{Client as S3Client};
use aws_sdk_s3::config::Config;
use std::env;
use std::fs;
use std::path::PathBuf;
use tokio;

fn get_aws_credentials() -> Result<Option<(String, String)>, String> {
    dotenv::dotenv().ok(); // Load environment variables from .env file if present

    // Check if running on EC2
    fn is_ec2() -> bool {
        let path = "/sys/devices/virtual/dmi/id/product_uuid";
        match fs::read_to_string(path) {
            Ok(content) => content.starts_with("ec2"),
            Err(_) => false,
        }
    }

    // Check if running on ECS
    fn is_ecs() -> bool {
        env::var("ECS_CONTAINER_METADATA_URI").is_ok()
    }

    // If credentials are passed explicitly
    if let (Ok(access_key), Ok(secret_key)) = (env::var("AWS_ACCESS_KEY_ID"), env::var("AWS_SECRET_ACCESS_KEY")) {
        return Ok(Some((access_key, secret_key)));
    }

    // If running on EC2 or ECS, credentials should be handled by environment variables or IAM roles
    if is_ec2() || is_ecs() {
        println!("Using instance IAM role or environment variables for credentials.");
        return Ok(None); // No credentials to return, SDK will use default method
    }

    // Otherwise, read from the config file (local setup)
    let config_file_path = dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")).join(".aws/credentials");
    println!("{:?}", config_file_path);
    if config_file_path.exists() {
        let contents = fs::read_to_string(config_file_path).expect("Unable to read config file");
        let mut in_default_profile = false;
        let mut access_key = None;
        let mut secret_key = None;

        for line in contents.lines() {
            if line.starts_with('[') && line.ends_with(']') {
                if line == "[default]" {
                    in_default_profile = true;
                } else {
                    in_default_profile = false;
                }
            } else if in_default_profile {
                if line.starts_with("aws_access_key_id") {
                    access_key = Some(line.split('=').nth(1).unwrap().trim().to_string());
                } else if line.starts_with("aws_secret_access_key") {
                    secret_key = Some(line.split('=').nth(1).unwrap().trim().to_string());
                }
            }
        }

        if let (Some(key), Some(secret)) = (access_key, secret_key) {
            return Ok(Some((key, secret)));
        }
        println!("AWS credentials not found in the default profile.");
    }

    Err("No valid AWS credentials found in environment or config file.".to_string())
}

/// Get a RuntimeConfig with specific ObjectStoreRegistry
pub fn with_object_store_registry(config: RuntimeConfig) -> RuntimeConfig {
    let registry = Arc::new(BallistaObjectStoreRegistry::default());
    config.with_object_store_registry(registry)
}

/// An object store detector based on which features are enable for different kinds of object stores
#[derive(Debug, Default)]
pub struct BallistaObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
}

impl BallistaObjectStoreRegistry {
    pub fn new() -> Self {
        Default::default()
    }

    /// Find a suitable object store based on its url and enabled features if possible
    fn get_feature_store(
        &self,
        url: &Url,
    ) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        #[cfg(any(feature = "hdfs", feature = "hdfs3"))]
        {
            if let Some(store) = HadoopFileSystem::new(url.as_str()) {
                return Ok(Arc::new(store));
            }
        }

        #[cfg(feature = "s3")]
        {
            if url.as_str().starts_with("s3://") {
                if let Some(bucket_name) = url.host_str() {
                    let parts: Vec<&str> = bucket_name.split('~').collect();

                    let object_store: Arc<dyn ObjectStore> = match get_aws_credentials() {
                        Ok(Some((access_key, secret_key))) => {

                            println!("access key {:?}", access_key);
                            Arc::new(
                                AmazonS3Builder::new()
                                    .with_region("us-east-1")
                                    .with_bucket_name(parts[0].to_string())
                                    .with_access_key_id(access_key)
                                    .with_secret_access_key(secret_key)
                                    .build()
                                    .unwrap(),
                            )
                        }
                        Ok(None) => {
                            // If no explicit credentials, use environment variables or default method
                            Arc::new(
                                AmazonS3Builder::from_env()
                                    .with_bucket_name(parts[0].to_string())
                                    .with_region("us-east-1")
                                    .build()
                                    .unwrap(),
                            )
                        }
                        Err(e) => return Err(DataFusionError::Execution(format!("Failed to load credentials: {:?}", e))),
                    };


                    /*
                    let store = Arc::new(
                        AmazonS3Builder::from_env()
                            .with_bucket_name(parts[0].to_string())
                            .with_region(parts[1].to_string())
                            .build()?,
                    );

                     */
                    return Ok(object_store);
                }
                // Support Alibaba Cloud OSS
                // Use S3 compatibility mode to access Alibaba Cloud OSS
                // The `AWS_ENDPOINT` should have bucket name included
            } else if url.as_str().starts_with("oss://") {
                if let Some(bucket_name) = url.host_str() {
                    let store = Arc::new(
                        AmazonS3Builder::from_env()
                            .with_virtual_hosted_style_request(true)
                            .with_bucket_name(bucket_name)
                            .build()?,
                    );
                    return Ok(store);
                }
            }
        }

        #[cfg(feature = "azure")]
        {
            if url.to_string().starts_with("azure://") {
                if let Some(bucket_name) = url.host_str() {
                    let store = Arc::new(
                        MicrosoftAzureBuilder::from_env()
                            .with_container_name(bucket_name)
                            .build()?,
                    );
                    return Ok(store);
                }
            }
        }

        #[cfg(feature = "gcs")]
        {
            if url.to_string().starts_with("gs://")
                || url.to_string().starts_with("gcs://")
            {
                if let Some(bucket_name) = url.host_str() {
                    let store = Arc::new(
                        GoogleCloudStorageBuilder::from_env()
                            .with_bucket_name(bucket_name)
                            .build()?,
                    );
                    return Ok(store);
                }
            }
        }

        Err(DataFusionError::Execution(format!(
            "No object store available for: {url}"
        )))
    }
}

impl ObjectStoreRegistry for BallistaObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {

        println!("in the get_store {:?}", url);

        self.inner.get_store(url).or_else(|_| {
            let store = self.get_feature_store(url)?;
            self.inner.register_store(url, store.clone());

            Ok(store)
        })
    }
}
