use std::{
    collections::HashSet,
    fs::{File, create_dir_all},
    io::Write,
    sync::Arc,
};

use anyhow::Context;
use clap::Parser;
use futures_util::{AsyncBufReadExt, TryStreamExt};
use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::{
    Api, Client, ResourceExt,
    api::LogParams,
    runtime::{WatchStreamExt, watcher},
};
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Namespace to read from (default: current)
    #[arg(short, long)]
    namespace: Option<String>,

    /// Tail lines
    #[arg(short, long, default_value_t = 500, conflicts_with = "no_tail_lines")]
    tail_lines: i64,

    /// No tail lines
    #[arg(long, default_value_t = false, conflicts_with = "tail_lines")]
    no_tail_lines: bool
}

async fn stream_container_logs(
    pods: Api<Pod>,
    pod_name: String,
    container: String,
    tail_lines: Option<i64>
) -> anyhow::Result<()> {
    let log_path = format!("logs/{pod_name}/{container}.log");
    let mut file = File::create(&log_path).with_context(|| format!("opening {log_path}"))?;

    info!("starting logs for {pod_name}/{container}");

    let mut stream = pods
        .log_stream(
            &pod_name,
            &LogParams {
                container: Some(container.clone()),
                follow: true,
                tail_lines,
                ..LogParams::default()
            },
        )
        .await?
        .lines();

    while let Some(line) = stream.try_next().await? {
        println!("{pod_name}/{container}: {line}");
        writeln!(file, "{line}")?;
    }

    Ok(())
}

async fn start_pod_logging(pods: Api<Pod>, pod: Pod, tail_lines: Option<i64>) {
    async fn inner(pods: Api<Pod>, pod: &Pod, tail_lines: &Option<i64>) -> anyhow::Result<()> {
        let pod_name = pod.name_any();

        let dir = format!("logs/{pod_name}");
        create_dir_all(&dir).with_context(|| format!("creating {dir}"))?;

        let containers = pod
            .spec
            .as_ref()
            .map(|s| {
                s.containers
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        for container in containers {
            let pods = pods.clone();
            let pod_name: String = pod_name.clone();
            let local_tail_lines = tail_lines.clone();

            tokio::spawn(async move {
                if let Err(e) = stream_container_logs(pods, pod_name, container, local_tail_lines).await {
                    warn!("log stream error: {e:?}");
                }
            });
        }

        Ok(())
    }
    match inner(pods, &pod, &tail_lines).await {
        Ok(_) => {}
        Err(error) => {
            println!("{}: error {error:?}", pod.name_any());
        }
    };
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let client = Client::try_default().await?;
    let ns = args.namespace.unwrap_or_else(|| client.default_namespace().to_string());

    let namespaces: Api<Namespace> = Api::all(client.clone());
    let namespace = namespaces.get(&ns).await;
    if let Err(e) = namespace {
        return Err(e.into());
    }

    let tail_lines = if args.no_tail_lines {
        None 
    } else {
        Some(args.tail_lines)
    };

    let pods: Api<Pod> = Api::namespaced(client, &ns);

    let seen = Arc::new(Mutex::new(HashSet::<String>::new()));

    watcher(pods.clone(), watcher::Config::default())
        .applied_objects()
        .default_backoff()
        .try_for_each(|pod| {
            let pods = pods.clone();
            let seen = seen.clone();

            async move {
                let pod_name = pod.name_any();
                let uid = pod.uid().unwrap_or_default();
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());

                info!("pod {} phase {:?}", pod_name, phase);

                if let Some(status) = &pod.status {
                    if let Some(conditions) = &status.conditions {
                        for c in conditions {
                            info!("pod {} condition {} = {}", pod_name, c.type_, c.status);
                        }
                    }

                    if let Some(cs) = &status.container_statuses {
                        for c in cs {
                            if c.restart_count > 0 {
                                warn!(
                                    "container {} in pod {} restarted {} times",
                                    c.name, pod_name, c.restart_count
                                );
                            }
                        }
                    }
                }

                if phase == Some(&"Running".to_string()) {
                    let mut seen = seen.lock().await;
                    if seen.insert(uid) {
                        start_pod_logging(pods, pod, tail_lines).await;
                    }
                }

                Ok(())
            }
        })
        .await?;

    Ok(())
}
