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
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::Mutex,
};
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

    /// Filter pods by label selector (e.g. "app=foo,role=worker")
    #[arg(long)]
    label_selector: Option<String>,

    /// Filter pods by field selector
    #[arg(long)]
    field_selector: Option<String>,

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

    /// Include kube-side timestamps on every log line
    #[arg(long, default_value_t = false)]
    timestamps: bool,

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
    label_selector: Option<String>,
    field_selector: Option<String>,
    argv: Vec<String>,
    captured: Vec<CapturedPod>,
}

#[derive(Serialize)]
struct CapturedPod {
    pod: String,
    namespace: String,
    uid: String,
    started_at: String,
    pod_yaml_path: Option<String>,
    containers: Vec<CapturedContainer>,
}

#[derive(Serialize)]
struct CapturedContainer {
    name: String,
    log_path: String,
    previous_log_path: Option<String>,
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
                timestamps: args.timestamps,
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

async fn fetch_previous_log(
    pods: Api<Pod>,
    pod_name: String,
    container: String,
    out_path: PathBuf,
) {
    let params = LogParams {
        container: Some(container.clone()),
        previous: true,
        ..LogParams::default()
    };
    match pods.logs(&pod_name, &params).await {
        Ok(content) => {
            if let Err(e) = std::fs::write(&out_path, content) {
                warn!("write previous log {}: {e:?}", out_path.display());
            } else {
                info!("captured previous log: {}", out_path.display());
            }
        }
        Err(e) => warn!("fetch previous log for {pod_name}/{container}: {e:?}"),
    }
}

async fn start_pod_logging(client: Client, pod: Pod, args: &Args, run: Arc<RunState>) {
    async fn inner(
        client: Client,
        pod: Pod,
        args: &Args,
        run: Arc<RunState>,
    ) -> anyhow::Result<()> {
        let pod_name = pod.name_any();
        let namespace = pod.namespace().unwrap_or_else(|| "default".to_string());
        let uid = pod.uid().unwrap_or_default();

        let pod_dir = run.dir.join(&namespace).join(&pod_name);
        create_dir_all(&pod_dir).with_context(|| format!("creating {}", pod_dir.display()))?;

        let pods: Api<Pod> = Api::namespaced(client, &namespace);

        let pod_yaml_path = if args.dump_config {
            let path = pod_dir.join("pod.yaml");
            let mut sanitized = pod.clone();
            sanitized.managed_fields_mut().clear(); // hide managed fields (https://github.com/kube-rs/kube/blob/ab9040edbfeffb2584d77fa0edfd02eff8f60d2b/examples/kubectl.rs#L99)
            let yaml = serde_norway::to_string(&sanitized)?;
            std::fs::write(&path, yaml)
                .with_context(|| format!("writing {}", path.display()))?;
            Some(path.to_string_lossy().into_owned())
        } else {
            None
        };

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

        let restarted: HashSet<String> = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .map(|cs| {
                cs.iter()
                    .filter(|c| c.restart_count > 0)
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        let captured_containers = containers
            .iter()
            .map(|c| {
                let log_path = pod_dir.join(format!("{c}.log"));
                let previous_log_path = if restarted.contains(c) {
                    Some(
                        pod_dir
                            .join(format!("{c}.previous.log"))
                            .to_string_lossy()
                            .into_owned(),
                    )
                } else {
                    None
                };
                CapturedContainer {
                    name: c.clone(),
                    log_path: log_path.to_string_lossy().into_owned(),
                    previous_log_path,
                }
            })
            .collect();

        run.record_pod(CapturedPod {
            pod: pod_name.clone(),
            namespace: namespace.clone(),
            uid,
            started_at: Utc::now().to_rfc3339(),
            pod_yaml_path,
            containers: captured_containers,
        })
        .await?;

        for container in &containers {
            if restarted.contains(container) {
                let pods = pods.clone();
                let pod_name = pod_name.clone();
                let container = container.clone();
                let prev_path = pod_dir.join(format!("{container}.previous.log"));
                tokio::spawn(fetch_previous_log(pods, pod_name, container, prev_path));
            }
        }

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

    let pod_name = pod.name_any();
    if let Err(e) = inner(client, pod, args, run).await {
        warn!("{pod_name}: error {e:?}");
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
        label_selector: args.label_selector.clone(),
        field_selector: args.field_selector.clone(),
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

    let mut watcher_config = watcher::Config::default();
    if let Some(s) = &args.label_selector {
        watcher_config = watcher_config.labels(s);
    }
    if let Some(s) = &args.field_selector {
        watcher_config = watcher_config.fields(s);
    }

    let seen = Arc::new(Mutex::new(HashSet::<String>::new()));

    let watch = watcher(pods.clone(), watcher_config)
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

    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    let result = tokio::select! {
        r = watch => r.map_err(anyhow::Error::from),
        _ = sigint.recv() => {
            info!("received SIGINT, finalizing run manifest");
            Ok(())
        }
        _ = sigterm.recv() => {
            info!("received SIGTERM, finalizing run manifest");
            Ok(())
        }
    };

    if let Err(e) = run.finalize().await {
        warn!("failed to finalize manifest: {e:?}");
    }

    result
}
