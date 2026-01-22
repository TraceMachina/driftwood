use std::{fs::File, io::Write};

use anyhow::Context;
use futures_util::{AsyncBufReadExt, StreamExt, TryStreamExt, stream};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client, ResourceExt,
    api::LogParams,
    runtime::{WatchStreamExt, watcher},
};
use tracing::info;

async fn logger(pods: Api<Pod>, pod_name: String) {
    async fn inner(pods: Api<Pod>, pod_name: String) -> anyhow::Result<()> {
        info!("Fetching logs for {}", pod_name);
        let log_path = format!("logs/{pod_name}.log");
        let mut output_log =
            File::create(&log_path).with_context(|| format!("Opening {log_path}"))?;
        let mut logs = pods
            .log_stream(
                &pod_name,
                &LogParams {
                    follow: true,
                    // container: app.container,
                    //timestamps: true,
                    ..LogParams::default()
                },
            )
            .await?
            .lines();

        while let Some(line) = logs.try_next().await? {
            println!("{}: {}", pod_name, line);
            output_log.write_all(line.as_bytes())?;
            output_log.write_all("\n".as_bytes())?;
        }
        Ok(())
    }
    match inner(pods, pod_name.clone()).await {
        Ok(_) => {}
        Err(error) => {
            println!("{pod_name}: error {error:?}");
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let client = Client::try_default().await?;
    let api = Api::<Pod>::default_namespaced(client);
    let use_watchlist = std::env::var("WATCHLIST")
        .map(|s| s == "1")
        .unwrap_or(false);
    let wc = if use_watchlist {
        // requires WatchList feature gate on 1.27 or later
        watcher::Config::default().streaming_lists()
    } else {
        watcher::Config::default()
    };

    watcher(api.clone(), wc)
        .applied_objects()
        .default_backoff()
        .zip(stream::repeat(api.clone()))
        .map(|(p, local_api)| p.and_then(|ok_p| std::result::Result::Ok((ok_p, local_api))))
        .try_for_each(|(p, local_api)| async move {
            let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
            let pod_name = p.name_any();
            info!("saw {} in {:?}", pod_name, phase);
            if Some(&"Running".to_string()) == phase {
                println!("Running!");
                tokio::spawn(logger(local_api, pod_name));
            }
            Ok(())
        })
        .await?;
    Ok(())
}
