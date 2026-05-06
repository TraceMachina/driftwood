# driftwood

A small Kubernetes pod-log collector. Watches a namespace (or a whole
cluster), tails logs from every container in every running pod, and writes
them to disk in a per-run folder along with a manifest describing what was
captured.

## Build

```sh
cargo build --release
```

The binary lands at `target/release/driftwood`.

## Quick start

```sh
# Tail every container in the current kube context's default namespace.
driftwood

# Scope to a namespace and a label selector.
driftwood -n nativelink --label-selector app=nativelink

# Cross-namespace, on a non-default kube context.
driftwood --context staging --all-namespaces

# Use a specific kubeconfig file and pin a run name (useful for retries).
driftwood --kubeconfig ~/.kube/foo.yaml --run-name repro-issue-42
```

Stream output to stdout while logs accumulate on disk. `Ctrl-C` (or
`SIGTERM`) finalizes the run manifest before exit.

## Output layout

Each invocation creates a run folder:

```
logs/<run-id>/
├── run.yaml                                # manifest for this run
└── <namespace>/
    └── <pod>/
        ├── pod.yaml                        # if --dump-config
        ├── <container>.log                 # live tail
        └── <container>.previous.log        # if the container had restarted
```

`<run-id>` defaults to a UTC timestamp (`20260506T142301Z`) and can be
overridden with `--run-name`. The root (`logs/`) can be moved with
`--logs-dir`.

### `run.yaml`

Written at start, updated as pods are picked up, finalized on shutdown.
Captures everything you need to reproduce or interpret a run:

```yaml
run_name: repro-issue-42
run_dir: logs/repro-issue-42
start_time: 2026-05-06T14:23:01Z
end_time: 2026-05-06T14:51:18Z
context: staging
cluster_url: https://10.0.0.1
namespace: nativelink
all_namespaces: false
label_selector: app=nativelink
field_selector: null
argv: [driftwood, --context, staging, -n, nativelink, --label-selector, app=nativelink]
captured:
  - pod: scheduler-0
    namespace: nativelink
    uid: 8c1f...
    started_at: 2026-05-06T14:23:04Z
    pod_yaml_path: null
    containers:
      - name: scheduler
        log_path: logs/repro-issue-42/nativelink/scheduler-0/scheduler.log
        previous_log_path: null
```

## Flags

| Flag | Description |
| ---- | ----------- |
| `-n, --namespace <NS>` | Namespace to watch. Defaults to the kube context's namespace. |
| `--all-namespaces` | Watch every namespace. Conflicts with `-n`. |
| `--context <NAME>` | Kubeconfig context. Defaults to the current context. |
| `--kubeconfig <PATH>` | Kubeconfig file. Defaults to `$KUBECONFIG` or `~/.kube/config`. |
| `--label-selector <SEL>` | Standard label selector, e.g. `app=foo,role=worker`. |
| `--field-selector <SEL>` | Standard field selector, e.g. `status.phase=Running`. |
| `--run-name <NAME>` | Name for this run's folder. Defaults to a UTC timestamp. |
| `--logs-dir <DIR>` | Root directory for run folders. Default: `logs`. |
| `-t, --tail-lines <N>` | How many recent lines to fetch when starting each stream. Default: 500. |
| `--no-tail-lines` | Stream from the beginning of the buffer instead of tailing. |
| `--timestamps` | Prefix every log line with the kube-side timestamp. |
| `-d, --dump-config` | Write each pod's spec to `<pod>/pod.yaml` (managed fields stripped). |

## Behavior notes

- Pods are picked up when their phase first becomes `Running`. A pod's UID
  is remembered so the same pod isn't logged twice in one run.
- If a container has a non-zero `restart_count` at the moment driftwood
  attaches, driftwood fetches that container's previous-instance logs
  (`previous: true`) into `<container>.previous.log`. Live restarts that
  happen *during* a run are not handled yet.
- Watching is in-memory only; if driftwood is restarted, it starts a fresh
  run folder and reattaches to whatever pods are running at that moment.

## Cleanup

```sh
make cleanup-logs
```

Removes everything tracked under `logs/` via `git clean -dfX`.
