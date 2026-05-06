use std::{
    collections::HashSet,
    fs::{File, create_dir_all},
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use futures_util::{AsyncBufReadExt, TryStreamExt};
use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::{
    Api, Client, ResourceExt,
    api::LogParams,
    config::{Config, KubeConfigOptions, Kubeconfig},
    runtime::{WatchStreamExt, watcher},
};
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// Namespace to read from (default: current). Ignored when --all-namespaces is set.
    #[arg(short, long, conflicts_with = "all_namespaces")]
    namespace: Option<String>,

    /// Watch pods across all namespaces
    #[arg(long, default_value_t = false)]
    all_namespaces: bool,

    /// Kubeconfig context to use (default: current context)
    #[arg(long)]
    context: Option<String>,

    /// Path to a kubeconfig file (default: $KUBECONFIG or ~/.kube/config)
    #[arg(long)]
    kubeconfig: Option<PathBuf>,

    /// Name for this run; used as the log subdirectory (default: UTC timestamp)
    #[arg(long)]
    run_name: Option<String>,

    /// Root directory under which run folders are created
    #[arg(long, default_value = "logs")]
    logs_dir: PathBuf,

    /// Tail lines
    #[arg(short, long, default_value_t = 500, conflicts_with = "no_tail_lines")]
    tail_lines: i64,

    /// No tail lines
    #[arg(long, default_value_t = false, conflicts_with = "tail_lines")]
    no_tail_lines: bool,

    // Dump config at the start of the log
    #[arg(short, long, default_value_t = false)]
    dump_config: bool,
}

#[derive(Serialize)]
struct RunManifest {
    run_name: String,
    run_dir: String,
    start_time: String,
    end_time: Option<String>,
    context: Option<String>,
    cluster_url: Option<String>,
    namespace: Option<String>,
    all_namespaces: bool,
    argv: Vec<String>,
    captured: Vec<CapturedPod>,
}

#[derive(Serialize)]
struct CapturedPod {
    pod: String,
    namespace: String,
    uid: String,
    started_at: String,
    containers: Vec<CapturedContainer>,
}

#[derive(Serialize)]
struct CapturedContainer {
    name: String,
    log_path: String,
}

struct RunState {
    dir: PathBuf,
    manifest: Mutex<RunManifest>,
}

impl RunState {
    fn manifest_path(&self) -> PathBuf {
        self.dir.join("run.yaml")
    }

    async fn write_manifest(&self) -> anyhow::Result<()> {
        let yaml = {
            let manifest = self.manifest.lock().await;
            serde_norway::to_string(&*manifest)?
        };
        let path = self.manifest_path();
        let mut file =
            File::create(&path).with_context(|| format!("opening {}", path.display()))?;
        file.write_all(yaml.as_bytes())?;
        Ok(())
    }

    async fn record_pod(&self, captured: CapturedPod) -> anyhow::Result<()> {
        {
            let mut manifest = self.manifest.lock().await;
            manifest.captured.push(captured);
        }
        self.write_manifest().await
    }

    async fn finalize(&self) -> anyhow::Result<()> {
        {
            let mut manifest = self.manifest.lock().await;
            manifest.end_time = Some(Utc::now().to_rfc3339());
        }
        self.write_manifest().await
    }
}

async fn build_config(args: &Args) -> anyhow::Result<Config> {
    if args.kubeconfig.is_some() || args.context.is_some() {
        let kubeconfig = match &args.kubeconfig {
            Some(path) => Kubeconfig::read_from(path)
                .with_context(|| format!("reading kubeconfig {}", path.display()))?,
            None => Kubeconfig::read().context("reading default kubeconfig")?,
        };
        let opts = KubeConfigOptions {
            context: args.context.clone(),
            ..Default::default()
        };
        Config::from_custom_kubeconfig(kubeconfig, &opts)
            .await
            .context("building config from kubeconfig")
    } else {
        Config::infer().await.context("inferring kube config")
    }
}

async fn stream_container_logs(
    pods: Api<Pod>,
    namespace: String,
    pod_name: String,
    container: String,
    log_path: PathBuf,
    args: Args,
) -> anyhow::Result<()> {
    let mut file =
        File::create(&log_path).with_context(|| format!("opening {}", log_path.display()))?;

    if args.dump_config {
        let mut pod = pods.get(&pod_name).await?;
        pod.managed_fields_mut().clear(); // hide managed fields (as suggested by https://github.com/kube-rs/kube/blob/ab9040edbfeffb2584d77fa0edfd02eff8f60d2b/examples/kubectl.rs#L99)
        writeln!(file, "{}", serde_norway::to_string(&pod)?)?;
    }

    let tail_lines = if args.no_tail_lines {
        None
    } else {
        Some(args.tail_lines)
    };

    info!("starting logs for {namespace}/{pod_name}/{container}");

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
        println!("{namespace}/{pod_name}/{container}: {line}");
        writeln!(file, "{line}")?;
    }

    Ok(())
}

async fn start_pod_logging(client: Client, pod: Pod, args: &Args, run: Arc<RunState>) {
    async fn inner(
        client: Client,
        pod: &Pod,
        args: &Args,
        run: Arc<RunState>,
    ) -> anyhow::Result<()> {
        let pod_name = pod.name_any();
        let namespace = pod.namespace().unwrap_or_else(|| "default".to_string());
        let uid = pod.uid().unwrap_or_default();

        let pod_dir = run.dir.join(&namespace).join(&pod_name);
        create_dir_all(&pod_dir).with_context(|| format!("creating {}", pod_dir.display()))?;

        let pods: Api<Pod> = Api::namespaced(client, &namespace);

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

        let captured_containers = containers
            .iter()
            .map(|c| CapturedContainer {
                name: c.clone(),
                log_path: pod_dir.join(format!("{c}.log")).to_string_lossy().into_owned(),
            })
            .collect();

        run.record_pod(CapturedPod {
            pod: pod_name.clone(),
            namespace: namespace.clone(),
            uid,
            started_at: Utc::now().to_rfc3339(),
            containers: captured_containers,
        })
        .await?;

        for container in containers {
            let pods = pods.clone();
            let pod_name = pod_name.clone();
            let namespace = namespace.clone();
            let local_args = args.to_owned();
            let log_path = pod_dir.join(format!("{container}.log"));

            tokio::spawn(async move {
                if let Err(e) = stream_container_logs(
                    pods, namespace, pod_name, container, log_path, local_args,
                )
                .await
                {
                    warn!("log stream error: {e:?}");
                }
            });
        }

        Ok(())
    }

    if let Err(e) = inner(client, &pod, args, run).await {
        warn!("{}: error {e:?}", pod.name_any());
    }
}

fn sanitize_run_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let config = build_config(&args).await?;
    let cluster_url = config.cluster_url.to_string();
    let client = Client::try_from(config)?;

    let namespace = if args.all_namespaces {
        None
    } else {
        Some(
            args.namespace
                .clone()
                .unwrap_or_else(|| client.default_namespace().to_string()),
        )
    };

    let run_name = match &args.run_name {
        Some(name) => sanitize_run_name(name),
        None => Utc::now().format("%Y%m%dT%H%M%SZ").to_string(),
    };
    let run_dir = args.logs_dir.join(&run_name);
    create_dir_all(&run_dir).with_context(|| format!("creating {}", run_dir.display()))?;

    let manifest = RunManifest {
        run_name: run_name.clone(),
        run_dir: run_dir.to_string_lossy().into_owned(),
        start_time: Utc::now().to_rfc3339(),
        end_time: None,
        context: args.context.clone(),
        cluster_url: Some(cluster_url),
        namespace: namespace.clone(),
        all_namespaces: args.all_namespaces,
        argv: std::env::args().collect(),
        captured: Vec::new(),
    };

    let run = Arc::new(RunState {
        dir: run_dir,
        manifest: Mutex::new(manifest),
    });
    run.write_manifest().await?;

    info!("run dir: {}", run.dir.display());

    let pods: Api<Pod> = if let Some(ns) = &namespace {
        let nss: Api<Namespace> = Api::all(client.clone());
        nss.get(ns)
            .await
            .with_context(|| format!("namespace {ns}"))?;
        Api::namespaced(client.clone(), ns)
    } else {
        Api::all(client.clone())
    };

    let seen = Arc::new(Mutex::new(HashSet::<String>::new()));

    let watch = watcher(pods.clone(), watcher::Config::default())
        .applied_objects()
        .default_backoff()
        .try_for_each(|pod| {
            let client = client.clone();
            let seen = seen.clone();
            let local_args = args.clone();
            let run = run.clone();

            async move {
                let pod_name = pod.name_any();
                let uid = pod.uid().unwrap_or_default();
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());

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

                if phase == Some("Running") {
                    let mut seen = seen.lock().await;
                    if seen.insert(uid) {
                        start_pod_logging(client, pod, &local_args, run).await;
                    }
                }

                Ok(())
            }
        });

    let result = tokio::select! {
        r = watch => r.map_err(anyhow::Error::from),
        _ = tokio::signal::ctrl_c() => {
            info!("received ctrl-c, finalizing run manifest");
            Ok(())
        }
    };

    if let Err(e) = run.finalize().await {
        warn!("failed to finalize manifest: {e:?}");
    }

    result
}
