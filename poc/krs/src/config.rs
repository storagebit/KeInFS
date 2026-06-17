// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) kms_endpoint: String,
    pub(crate) kas_endpoint: String,
    pub(crate) lease_owner: String,
    pub(crate) max_tasks: u32,
    pub(crate) lease_ttl: Duration,
    pub(crate) poll_interval: Duration,
    pub(crate) stats_root: PathBuf,
    pub(crate) stats_publish_interval: Duration,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    kms_endpoint: Option<String>,
    kas_endpoint: Option<String>,
    lease_owner: Option<String>,
    max_tasks: Option<u32>,
    lease_ttl_ms: Option<u64>,
    poll_ms: Option<u64>,
    stats_root: Option<String>,
    stats_publish_ms: Option<u64>,
}

impl Config {
    fn defaults() -> Self {
        Self {
            kms_endpoint: "http://127.0.0.1:50060".to_string(),
            kas_endpoint: "http://127.0.0.1:50061".to_string(),
            lease_owner: format!("krs-{}", std::process::id()),
            max_tasks: 64,
            lease_ttl: Duration::from_millis(30_000),
            poll_interval: Duration::from_millis(1_000),
            stats_root: PathBuf::from("/run/keinfs/krs"),
            stats_publish_interval: Duration::from_millis(250),
        }
    }

    fn apply_file(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let raw = fs::read_to_string(path).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("failed to read KRS config `{}`: {err}", path.display()),
            )
        })?;
        let file: FileConfig = toml::from_str(&raw).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse KRS config `{}`: {err}", path.display()),
            )
        })?;

        if let Some(value) = file.kms_endpoint {
            self.kms_endpoint = value;
        }
        if let Some(value) = file.kas_endpoint {
            self.kas_endpoint = value;
        }
        if let Some(value) = file.lease_owner {
            self.lease_owner = value;
        }
        if let Some(value) = file.max_tasks {
            self.max_tasks = value.max(1);
        }
        if let Some(value) = file.lease_ttl_ms {
            self.lease_ttl = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.poll_ms {
            self.poll_interval = Duration::from_millis(value.max(100));
        }
        if let Some(value) = file.stats_root {
            self.stats_root = PathBuf::from(value);
        }
        if let Some(value) = file.stats_publish_ms {
            self.stats_publish_interval = Duration::from_millis(value.max(50));
        }

        Ok(())
    }

    pub(crate) fn fingerprint_source(&self) -> String {
        format!(
            concat!(
                "kms_endpoint={}\n",
                "kas_endpoint={}\n",
                "lease_owner={}\n",
                "max_tasks={}\n",
                "lease_ttl_ms={}\n",
                "poll_ms={}\n",
                "stats_root={}\n",
                "stats_publish_ms={}\n"
            ),
            self.kms_endpoint,
            self.kas_endpoint,
            self.lease_owner,
            self.max_tasks,
            self.lease_ttl.as_millis(),
            self.poll_interval.as_millis(),
            self.stats_root.display(),
            self.stats_publish_interval.as_millis(),
        )
    }

    pub(crate) fn kas_endpoints(&self) -> Vec<String> {
        self.kas_endpoint
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }
}

pub(crate) fn parse_args(args: Vec<String>) -> Result<Config, Box<dyn Error>> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Err(arg_error(usage()));
    }

    let mut config = Config::defaults();
    if let Some(path) = scan_config_path(&args)? {
        config.apply_file(&path)?;
    }

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                let _ = args.get(i).ok_or_else(|| missing_value("--config"))?;
            }
            "--kms-endpoint" => {
                i += 1;
                config.kms_endpoint = args
                    .get(i)
                    .ok_or_else(|| missing_value("--kms-endpoint"))?
                    .clone();
            }
            "--kas-endpoint" => {
                i += 1;
                config.kas_endpoint = args
                    .get(i)
                    .ok_or_else(|| missing_value("--kas-endpoint"))?
                    .clone();
            }
            "--lease-owner" => {
                i += 1;
                config.lease_owner = args
                    .get(i)
                    .ok_or_else(|| missing_value("--lease-owner"))?
                    .clone();
            }
            "--max-tasks" => {
                i += 1;
                config.max_tasks = args
                    .get(i)
                    .ok_or_else(|| missing_value("--max-tasks"))?
                    .parse::<u32>()?
                    .max(1);
            }
            "--lease-ttl-ms" => {
                i += 1;
                let value: u64 = args
                    .get(i)
                    .ok_or_else(|| missing_value("--lease-ttl-ms"))?
                    .parse()?;
                config.lease_ttl = Duration::from_millis(value.max(1_000));
            }
            "--poll-ms" => {
                i += 1;
                let value: u64 = args
                    .get(i)
                    .ok_or_else(|| missing_value("--poll-ms"))?
                    .parse()?;
                config.poll_interval = Duration::from_millis(value.max(100));
            }
            "--stats-root" => {
                i += 1;
                config.stats_root =
                    PathBuf::from(args.get(i).ok_or_else(|| missing_value("--stats-root"))?);
            }
            "--stats-publish-ms" => {
                i += 1;
                let value: u64 = args
                    .get(i)
                    .ok_or_else(|| missing_value("--stats-publish-ms"))?
                    .parse()?;
                config.stats_publish_interval = Duration::from_millis(value.max(50));
            }
            other => return Err(arg_error(format!("unknown KRS argument `{other}`"))),
        }
        i += 1;
    }

    Ok(config)
}

fn scan_config_path(args: &[String]) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let mut config_path = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--config" {
            i += 1;
            let value = args.get(i).ok_or_else(|| missing_value("--config"))?;
            config_path = Some(PathBuf::from(value));
        }
        i += 1;
    }
    Ok(config_path)
}

fn missing_value(flag: &str) -> Box<dyn Error> {
    arg_error(format!("missing value for {flag}"))
}

fn arg_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn usage() -> &'static str {
    "krs [--config /etc/keinfs/krs.toml] [--kms-endpoint http://127.0.0.1:50060] [--kas-endpoint http://127.0.0.1:50061,http://127.0.0.1:50062] [--lease-owner krs-host] [--max-tasks 64] [--lease-ttl-ms 30000] [--poll-ms 1000] [--stats-root /run/keinfs/krs] [--stats-publish-ms 250]"
}

#[cfg(test)]
mod tests {
    use super::{parse_args, Config};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn loads_toml_config() {
        let config_path = temp_config_path("krs");
        fs::write(
            &config_path,
            r#"
kms_endpoint = "http://10.0.0.1:50060"
kas_endpoint = "http://10.0.0.2:50061"
lease_owner = "krs-node-a"
max_tasks = 9
stats_root = "/tmp/krs-stats"
"#,
        )
        .unwrap();

        let parsed = parse_args(vec![
            "--config".to_string(),
            config_path.display().to_string(),
        ])
        .unwrap();

        assert_eq!(parsed.kms_endpoint, "http://10.0.0.1:50060");
        assert_eq!(parsed.kas_endpoint, "http://10.0.0.2:50061");
        assert_eq!(parsed.lease_owner, "krs-node-a");
        assert_eq!(parsed.max_tasks, 9);
        assert_eq!(parsed.stats_root, PathBuf::from("/tmp/krs-stats"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn splits_multiple_kas_endpoints() {
        let config = Config {
            kas_endpoint: " http://10.0.0.2:50061, http://10.0.0.3:50061 ,, ".to_string(),
            ..Config::defaults()
        };

        assert_eq!(
            config.kas_endpoints(),
            vec![
                "http://10.0.0.2:50061".to_string(),
                "http://10.0.0.3:50061".to_string()
            ]
        );
    }

    fn temp_config_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{nanos}.toml"))
    }
}
