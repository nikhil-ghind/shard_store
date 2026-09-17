# Shard Store

## Project Overview

A distributed object storage system modeled after S3/Ceph that provides reliable, scalable storage across a cluster of nodes. Data placement uses consistent hashing to minimize rebalancing on topology changes. Every object is replicated across 3 nodes (replication factor = 3). When a node fails, the cluster automatically detects the failure, identifies under-replicated shards, and rebalances data onto healthy nodes. A Python CLI provides operators with bucket management, object operations, and lifecycle policy automation (e.g., expiry, tiering).

Key goals:
- Sub-100ms PUT/GET latency for objects under 1 MB at steady state
- Automatic shard rebalancing within 60 seconds of node failure detection
- Zero data loss during single-node failure
- Operator-facing CLI for all lifecycle operations
- Full observability via Prometheus metrics and Grafana dashboards

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Storage node daemon | Go | 1.22 |
| Coordinator / metadata service | Go | 1.22 |
| Consistent hashing library | `stathat/consistent` (Go) | latest |
| Python CLI | Python | 3.12 |
| CLI HTTP client | `httpx` | 0.27 |
| CLI arg parsing | `typer` | 0.12 |
| Container orchestration | Kubernetes | 1.29 |
| Service mesh (optional) | Istio | 1.21 |
| Metrics collection | Prometheus | 2.51 |
| Dashboards | Grafana | 10.4 |
| Object serialization (metadata) | Protocol Buffers | 3 |
| Storage format on disk | Raw files + WAL journal | — |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Python CLI (client)                  │
│  bucket create/delete/list, object put/get/delete,      │
│  lifecycle policy set/get                               │
└───────────────────┬─────────────────────────────────────┘
                    │ HTTP/REST
┌───────────────────▼─────────────────────────────────────┐
│              Coordinator Service (Go)                   │
│  - Consistent hash ring management                      │
│  - Node membership / heartbeat tracking                 │
│  - Shard assignment & rebalance orchestration           │
│  - Bucket/object metadata (etcd-backed)                 │
│  - Lifecycle policy engine                              │
└──────┬──────────────────────────────────┬───────────────┘
       │ gRPC                             │ gRPC
┌──────▼──────┐  ┌────────────┐  ┌───────▼──────┐
│  Storage    │  │  Storage   │  │  Storage     │
│  Node A     │  │  Node B    │  │  Node C      │
│  (Go daemon)│  │  (Go daemon│  │  (Go daemon) │
│  port 7001  │  │  port 7002 │  │  port 7003   │
└─────────────┘  └────────────┘  └──────────────┘
       │                │                │
   local disk       local disk       local disk
   /data/shards    /data/shards    /data/shards
```

**Data flow — PUT:**
1. CLI sends `PUT /buckets/{bucket}/objects/{key}` to Coordinator.
2. Coordinator computes hash ring position for `bucket/key`, identifies 3 replica nodes.
3. Coordinator streams object to primary node; primary fan-outs to 2 replicas using gRPC `ReplicateObject` RPC.
4. Primary returns ACK only after all 3 replicas confirm write.
5. Coordinator records metadata (object key, size, checksum, replica nodes, version) in etcd.

**Data flow — GET:**
1. CLI sends `GET /buckets/{bucket}/objects/{key}`.
2. Coordinator looks up metadata, returns presigned redirect URL to nearest healthy replica.
3. CLI streams object bytes directly from storage node.

**Failure detection & rebalancing:**
- Each storage node sends a heartbeat gRPC call to Coordinator every 5 s.
- After 3 missed heartbeats (15 s), node is marked `SUSPECT`; after 45 s total, marked `DEAD`.
- Coordinator queries etcd for all shards with replicas on dead node, enqueues rebalance jobs.
- Rebalance worker picks a new healthy node from hash ring, copies shard from a surviving replica, updates metadata.

---

## Phase 1 — Repository Structure & Scaffolding

**Goal:** Establish the monorepo layout, Go module setup, Python package skeleton, and local development tooling.

### Tasks

1. Create top-level directory layout:
   ```
   /
   ├── coordinator/          # Go: coordinator service
   ├── storagenode/          # Go: storage node daemon
   ├── proto/                # Protobuf definitions
   ├── cli/                  # Python CLI
   ├── deploy/
   │   ├── k8s/              # Kubernetes manifests
   │   └── monitoring/       # Prometheus & Grafana configs
   ├── scripts/              # Build/dev helper scripts
   └── Makefile
   ```

2. Initialize Go modules:
   - Run `go mod init github.com/yourorg/dist-storage` inside `coordinator/`.
   - Run `go mod init github.com/yourorg/dist-storage/storagenode` inside `storagenode/`.
   - Add dependencies: `google.golang.org/grpc`, `google.golang.org/protobuf`, `go.etcd.io/etcd/client/v3`, `github.com/stathat/consistent`, `github.com/prometheus/client_golang`.

3. Initialize Python package:
   - Create `cli/pyproject.toml` using `[project]` table with `name = "doss-cli"`, `version = "0.1.0"`, dependencies: `typer[all]==0.12.*`, `httpx==0.27.*`, `rich==13.*`.
   - Create `cli/doss_cli/__init__.py` (empty).
   - Create `cli/doss_cli/main.py` with `app = typer.Typer()` and `if __name__ == "__main__": app()`.

4. Write `proto/storage.proto`:
   - Define `package storage;` with `option go_package = "github.com/yourorg/dist-storage/proto"`.
   - Define messages: `ObjectChunk { bytes data = 1; string checksum = 2; }`, `ReplicateRequest { string bucket = 1; string key = 2; stream ObjectChunk chunks = 3; }`, `ReplicateResponse { bool success = 1; }`, `HeartbeatRequest { string node_id = 1; int64 timestamp_unix = 2; }`, `HeartbeatResponse { bool ack = 1; }`.
   - Define service `StorageNodeService { rpc ReplicateObject(stream ReplicateRequest) returns (ReplicateResponse); rpc GetObject(GetRequest) returns (stream ObjectChunk); rpc HeartbeatToCoordinator(HeartbeatRequest) returns (HeartbeatResponse); }`.

5. Write `Makefile` targets:
   - `proto`: run `protoc --go_out=. --go-grpc_out=. proto/storage.proto`.
   - `build-coordinator`: `cd coordinator && go build -o ../bin/coordinator ./cmd/coordinator`.
   - `build-storagenode`: `cd storagenode && go build -o ../bin/storagenode ./cmd/storagenode`.
   - `install-cli`: `pip install -e cli/`.
   - `lint`: `golangci-lint run ./...` and `ruff check cli/`.
   - `test`: `go test ./... && pytest cli/tests/`.

6. Create `scripts/dev-cluster.sh`: starts 1 coordinator and 3 storage node processes locally using different ports and temp data directories, for local integration testing without Kubernetes.

---

## Phase 2 — Storage Node Daemon (Go)

**Goal:** Implement the storage node daemon that accepts gRPC calls to store, retrieve, and replicate objects on local disk.

### Tasks

1. Create `storagenode/cmd/storagenode/main.go`:
   - Parse flags: `--node-id string`, `--data-dir string`, `--listen-addr string` (default `:7001`), `--coordinator-addr string`.
   - Call `storagenode.New(cfg).Start()`.

2. Create `storagenode/internal/server/server.go`:
   - Struct `Server` with fields: `dataDir string`, `nodeID string`, `grpcServer *grpc.Server`, `metrics *Metrics`.
   - Method `Start() error`: registers gRPC service, starts heartbeat goroutine, starts gRPC listener.
   - Method `Stop()`: graceful shutdown with 10 s timeout.

3. Create `storagenode/internal/store/store.go`:
   - Struct `DiskStore` with field `baseDir string`.
   - Method `Put(bucket, key string, reader io.Reader) (checksum string, err error)`:
     - Compute object path: `{baseDir}/{bucket}/{key}`.
     - Create directories if absent.
     - Write to temp file `{path}.tmp`, compute SHA-256 checksum while writing.
     - Rename temp file to final path (atomic swap).
     - Append WAL entry `{timestamp} PUT {bucket}/{key} {checksum}\n` to `{baseDir}/wal.log`.
   - Method `Get(bucket, key string) (io.ReadCloser, error)`: open file at computed path, return reader.
   - Method `Delete(bucket, key string) error`: remove file, append WAL entry.
   - Method `ListShards() ([]string, error)`: walk `baseDir`, return list of `bucket/key` paths.

4. Create `storagenode/internal/grpchandler/handler.go`:
   - Implement `StorageNodeServiceServer` interface generated from proto.
   - `ReplicateObject(stream StorageNodeService_ReplicateObjectServer) error`: read chunks from stream, call `store.Put`, return `ReplicateResponse`.
   - `GetObject(req *GetRequest, stream StorageNodeService_GetObjectServer) error`: call `store.Get`, stream file in 64 KB chunks, compute checksum, send `ObjectChunk` messages.
   - `HeartbeatToCoordinator(ctx, req) (*HeartbeatResponse, error)`: simply ack (heartbeat direction is actually node→coordinator, so this RPC is on coordinator side; add stub here for bidirectional ping health check).

5. Create `storagenode/internal/heartbeat/heartbeat.go`:
   - Function `StartHeartbeat(coordinatorAddr, nodeID string, interval time.Duration)`: dials coordinator gRPC endpoint, sends `HeartbeatRequest` every `interval`, reconnects on error with exponential backoff capped at 30 s.

6. Create `storagenode/internal/metrics/metrics.go`:
   - Register Prometheus counters/gauges:
     - `storage_put_total counter`
     - `storage_get_total counter`
     - `storage_put_bytes_total counter`
     - `storage_disk_used_bytes gauge` (updated every 30 s by a background goroutine using `syscall.Statfs`).
   - Expose `/metrics` HTTP endpoint on a separate port (default `:9101`).

7. Write unit tests in `storagenode/internal/store/store_test.go`:
   - Test `Put` then `Get` round-trips for a 1 MB random byte slice.
   - Test `Put` is atomic (no partial file visible if process killed mid-write — simulate by checking temp file cleanup).
   - Test `Delete` removes file and subsequent `Get` returns `os.ErrNotExist`.

---

## Phase 3 — Coordinator Service (Go)

**Goal:** Implement the coordinator: consistent hash ring, node membership, metadata in etcd, PUT/GET routing, lifecycle policy engine.

### Tasks

1. Create `coordinator/cmd/coordinator/main.go`:
   - Parse flags: `--listen-addr string` (default `:8080` for HTTP REST), `--grpc-addr string` (default `:9090`), `--etcd-endpoints string` (comma-separated), `--replication-factor int` (default `3`), `--rebalance-workers int` (default `4`).
   - Initialize etcd client, hash ring, HTTP router, gRPC server; call `coordinator.New(cfg).Start()`.

2. Create `coordinator/internal/ring/ring.go`:
   - Wrap `github.com/stathat/consistent` in struct `HashRing`.
   - Method `AddNode(nodeID string)`: add to ring.
   - Method `RemoveNode(nodeID string)`: remove from ring.
   - Method `GetReplicas(key string, n int) ([]string, error)`: return `n` distinct node IDs for placement.
   - Method `Nodes() []string`: list all current nodes.
   - Use `sync.RWMutex` for all mutations.

3. Create `coordinator/internal/membership/membership.go`:
   - Struct `NodeState { ID string; Addr string; Status string; LastHeartbeat time.Time }`.
   - Struct `MembershipManager { nodes map[string]*NodeState; ring *ring.HashRing; mu sync.RWMutex }`.
   - Method `RegisterNode(id, addr string)`: add to map and ring, persist to etcd key `/nodes/{id}`.
   - Method `RecordHeartbeat(id string)`: update `LastHeartbeat`.
   - Method `StartFailureDetector(interval time.Duration, onDead func(id string))`: background goroutine that every `interval` checks all nodes; marks `SUSPECT` after 3 missed intervals, calls `onDead` after 9 missed intervals; removes node from ring.

4. Create `coordinator/internal/metadata/metadata.go`:
   - Use `go.etcd.io/etcd/client/v3` for all KV operations.
   - Key schema:
     - Buckets: `/buckets/{bucket}` → JSON `{ "created_at": ..., "lifecycle": {...} }`.
     - Objects: `/objects/{bucket}/{key}` → JSON `{ "size": int, "checksum": string, "replicas": ["nodeA","nodeB","nodeC"], "version": int, "created_at": ..., "expires_at": ... }`.
   - Functions: `PutBucket`, `DeleteBucket`, `ListBuckets`, `PutObjectMeta`, `GetObjectMeta`, `DeleteObjectMeta`, `ListObjectsInBucket`.

5. Create `coordinator/internal/rebalance/rebalance.go`:
   - Struct `RebalanceWorker { queue chan RebalanceJob; store *metadata.Store; ring *ring.HashRing; nodeClients map[string]StorageNodeClient }`.
   - Struct `RebalanceJob { Bucket, Key string; SourceNode, TargetNode string }`.
   - Method `Enqueue(job RebalanceJob)`: non-blocking send to buffered channel (size 10000).
   - Method `Start(numWorkers int)`: spawn `numWorkers` goroutines each calling `processJob`.
   - Method `processJob(job RebalanceJob)`: dial source node gRPC, stream object bytes, dial target node gRPC, write object, update etcd metadata replica list; emit Prometheus counter `rebalance_jobs_completed_total`.
   - Function `FindUnderReplicatedObjects(deadNodeID string, meta *metadata.Store) ([]RebalanceJob, error)`: etcd prefix scan `/objects/`, filter where `replicas` contains `deadNodeID`, return jobs.

6. Create `coordinator/internal/httpapi/router.go`:
   - Use `net/http` standard library with `gorilla/mux` v1.8 for routing.
   - Register routes:
     - `PUT /buckets/{bucket}` → `CreateBucketHandler`
     - `DELETE /buckets/{bucket}` → `DeleteBucketHandler`
     - `GET /buckets` → `ListBucketsHandler`
     - `PUT /buckets/{bucket}/objects/{key}` → `PutObjectHandler`
     - `GET /buckets/{bucket}/objects/{key}` → `GetObjectHandler`
     - `DELETE /buckets/{bucket}/objects/{key}` → `DeleteObjectHandler`
     - `GET /buckets/{bucket}/objects` → `ListObjectsHandler`
     - `PUT /buckets/{bucket}/lifecycle` → `SetLifecyclePolicyHandler`
     - `GET /buckets/{bucket}/lifecycle` → `GetLifecyclePolicyHandler`

7. Implement `coordinator/internal/httpapi/object_handlers.go`:
   - `PutObjectHandler`: read request body, call `ring.GetReplicas(bucket+"/"+key, 3)`, dial primary storage node gRPC, stream body using `ReplicateObject` RPC. Primary is responsible for fan-out replication to other 2 replicas (implement fan-out in storage node's `ReplicateObject` handler). After primary ACK, write metadata to etcd.
   - `GetObjectHandler`: look up metadata, check replica nodes for liveness via membership manager, pick nearest healthy replica, return HTTP 307 redirect to `http://{nodeAddr}/objects/{bucket}/{key}` (storage nodes also expose a simple HTTP GET endpoint).
   - `DeleteObjectHandler`: delete from all replica nodes via gRPC `DeleteObject` RPC, then delete etcd metadata.

8. Create `coordinator/internal/lifecycle/engine.go`:
   - Struct `LifecycleEngine { meta *metadata.Store; ticker *time.Ticker }`.
   - Method `Start()`: every 1 hour, call `RunPass`.
   - Method `RunPass()`: scan all object metadata; for each object where `expires_at` is non-zero and before `time.Now()`, enqueue a delete operation via coordinator's delete pipeline.

9. Add gRPC server in coordinator for heartbeat reception:
   - File `coordinator/internal/grpchandler/heartbeat_handler.go`.
   - Implement `CoordinatorService { rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse); rpc RegisterNode(RegisterRequest) returns (RegisterResponse); }` (add these to `proto/storage.proto`).

10. Write unit tests in `coordinator/internal/ring/ring_test.go`:
    - Test that `GetReplicas` returns exactly `n` distinct nodes.
    - Test that after `RemoveNode`, removed node never appears in `GetReplicas`.
    - Test distribution uniformity: 10000 keys across 10 nodes; each node should receive between 800–1200 keys.

---

## Phase 4 — Python CLI

**Goal:** Implement the `doss` CLI with commands for bucket management, object operations, and lifecycle policies.

### Tasks

1. Create `cli/doss_cli/config.py`:
   - Function `get_coordinator_url() -> str`: reads env var `DOSS_COORDINATOR_URL`, falls back to `http://localhost:8080`.
   - Function `load_config() -> dict`: reads `~/.doss/config.json` if it exists; merges with env vars.

2. Create `cli/doss_cli/client.py`:
   - Class `DossClient`:
     - `__init__(self, base_url: str)`: store base URL, create `httpx.Client(base_url=base_url, timeout=30)`.
     - `create_bucket(self, bucket: str) -> dict`
     - `delete_bucket(self, bucket: str) -> None`
     - `list_buckets(self) -> list[dict]`
     - `put_object(self, bucket: str, key: str, file_path: str) -> dict`: stream file using `httpx` streaming PUT.
     - `get_object(self, bucket: str, key: str, output_path: str) -> None`: follow redirect, stream to file.
     - `delete_object(self, bucket: str, key: str) -> None`
     - `list_objects(self, bucket: str, prefix: str = "") -> list[dict]`
     - `set_lifecycle(self, bucket: str, policy: dict) -> dict`
     - `get_lifecycle(self, bucket: str) -> dict`
   - All methods raise `typer.Exit(code=1)` after printing error with `rich.console.Console().print_exception()` on non-2xx responses.

3. Create `cli/doss_cli/commands/bucket.py`:
   - `app = typer.Typer()`.
   - `@app.command("create") def create(bucket: str)`: call `client.create_bucket`, print success with `rich`.
   - `@app.command("delete") def delete(bucket: str, force: bool = typer.Option(False, "--force"))`: confirm prompt unless `--force`.
   - `@app.command("list") def list_buckets()`: print table using `rich.table.Table`.

4. Create `cli/doss_cli/commands/object.py`:
   - `@app.command("put") def put(bucket: str, key: str, file: Path = typer.Argument(..., exists=True))`: show progress bar using `rich.progress.Progress` during upload.
   - `@app.command("get") def get(bucket: str, key: str, output: Path = typer.Option(..., "--output", "-o"))`.
   - `@app.command("delete") def delete(bucket: str, key: str)`.
   - `@app.command("list") def list_objects(bucket: str, prefix: str = "")`.

5. Create `cli/doss_cli/commands/lifecycle.py`:
   - `@app.command("set") def set_policy(bucket: str, policy_file: Path = typer.Argument(..., exists=True))`: load JSON from `policy_file`, call `client.set_lifecycle`.
   - `@app.command("get") def get_policy(bucket: str)`: pretty-print policy JSON using `rich`.

6. Update `cli/doss_cli/main.py`:
   - Add subcommand groups: `app.add_typer(bucket.app, name="bucket")`, `app.add_typer(object.app, name="object")`, `app.add_typer(lifecycle.app, name="lifecycle")`.
   - Add top-level `--coordinator-url` option that overrides config.

7. Create `cli/tests/test_client.py`:
   - Use `pytest` with `respx` library to mock `httpx` calls.
   - Test `create_bucket` sends `PUT /buckets/{name}`.
   - Test `put_object` streams file body correctly.
   - Test error handling: 404 and 500 responses raise `typer.Exit`.

8. Build CLI entry point in `cli/pyproject.toml`:
   ```toml
   [project.scripts]
   doss = "doss_cli.main:app"
   ```

---

## Phase 5 — Kubernetes Deployment

**Goal:** Deploy coordinator, storage nodes, etcd, and Prometheus/Grafana to a Kubernetes cluster.

### Tasks

1. Create `deploy/k8s/namespace.yaml`:
   - Namespace `doss`.

2. Create `deploy/k8s/etcd/statefulset.yaml`:
   - `StatefulSet` named `etcd` with 3 replicas, image `quay.io/coreos/etcd:v3.5.13`.
   - Environment variables for cluster peer URLs.
   - `volumeClaimTemplates` requesting 10 Gi `ReadWriteOnce` storage.
   - Liveness probe: `etcdctl endpoint health`.

3. Create `deploy/k8s/coordinator/deployment.yaml`:
   - `Deployment` with 2 replicas, image `yourorg/doss-coordinator:latest`.
   - Environment variables: `ETCD_ENDPOINTS`, `LISTEN_ADDR`, `GRPC_ADDR`, `REPLICATION_FACTOR=3`.
   - `readinessProbe` HTTP GET `/healthz` on port 8080.
   - `resources.requests`: `cpu: 500m`, `memory: 512Mi`.

4. Create `deploy/k8s/coordinator/service.yaml`:
   - `ClusterIP` service exposing port 8080 (HTTP) and 9090 (gRPC).

5. Create `deploy/k8s/storagenode/statefulset.yaml`:
   - `StatefulSet` named `storage-node` with 6 replicas (initial cluster size).
   - Image `yourorg/doss-storagenode:latest`.
   - `volumeClaimTemplates` requesting 100 Gi `ReadWriteOnce` per pod.
   - Environment variables: `NODE_ID` set from pod name via downward API, `DATA_DIR=/data`, `COORDINATOR_ADDR=coordinator:9090`.
   - Init container that registers node with coordinator via `curl -X PUT http://coordinator:8080/nodes/${NODE_ID}`.
   - `preStop` lifecycle hook: sends deregister call to coordinator.

6. Create `deploy/k8s/storagenode/service.yaml`:
   - Headless `ClusterIP` service (`.spec.clusterIP: None`) for stable DNS.

7. Create `deploy/k8s/monitoring/prometheus-configmap.yaml`:
   - `ConfigMap` with `prometheus.yml` content:
     - `scrape_configs` for coordinator (`/metrics` on port 9090), storage nodes (`/metrics` on port 9101), and etcd.
     - `scrape_interval: 15s`.

8. Create `deploy/k8s/monitoring/prometheus-deployment.yaml`:
   - `Deployment` with 1 replica, image `prom/prometheus:v2.51.0`.
   - Mount config map at `/etc/prometheus/prometheus.yml`.
   - `PersistentVolumeClaim` for 20 Gi retention storage.

9. Create `deploy/k8s/monitoring/grafana-deployment.yaml`:
   - `Deployment` image `grafana/grafana:10.4.0`.
   - `ConfigMap` for datasource auto-provisioning pointing at Prometheus service.
   - `ConfigMap` for dashboard JSON (see Phase 6).

10. Create `deploy/k8s/kustomization.yaml`:
    - References all resource files; allows `kubectl apply -k deploy/k8s/` to deploy everything.

---

## Phase 6 — Observability (Prometheus & Grafana)

**Goal:** Define all metrics, alerts, and Grafana dashboard panels.

### Tasks

1. In coordinator (`coordinator/internal/metrics/metrics.go`), register:
   - `coordinator_put_requests_total` (counter, labels: `status`)
   - `coordinator_get_requests_total` (counter, labels: `status`)
   - `coordinator_active_nodes` (gauge)
   - `coordinator_rebalance_jobs_total` (counter)
   - `coordinator_rebalance_jobs_pending` (gauge)
   - `coordinator_put_latency_seconds` (histogram, buckets: 0.01, 0.05, 0.1, 0.5, 1.0)

2. In storage node, add metrics already listed in Phase 2; additionally add:
   - `storagenode_replicate_latency_seconds` (histogram)
   - `storagenode_errors_total` (counter, labels: `op`)

3. Create `deploy/monitoring/alerts.yml`:
   - Alert `NodeDown`: fires when `coordinator_active_nodes` drops below expected minimum for more than 1 minute.
   - Alert `RebalanceBacklog`: fires when `coordinator_rebalance_jobs_pending > 500` for 5 minutes.
   - Alert `HighPutLatency`: fires when `histogram_quantile(0.99, rate(coordinator_put_latency_seconds_bucket[5m])) > 0.5`.
   - Alert `DiskUsageHigh`: fires when `storagenode_disk_used_bytes / node_filesystem_size_bytes > 0.85`.

4. Create `deploy/monitoring/grafana-dashboard.json`:
   - Panel 1 — "Request Rate": `rate(coordinator_put_requests_total[1m])` + `rate(coordinator_get_requests_total[1m])`.
   - Panel 2 — "PUT p99 Latency": `histogram_quantile(0.99, rate(coordinator_put_latency_seconds_bucket[5m]))`.
   - Panel 3 — "Active Nodes": `coordinator_active_nodes`.
   - Panel 4 — "Rebalance Queue Depth": `coordinator_rebalance_jobs_pending`.
   - Panel 5 — "Disk Used Per Node": `storagenode_disk_used_bytes` by `instance`.
   - Panel 6 — "Error Rate": `rate(storagenode_errors_total[1m])` by `op`.

---

## Phase 7 — Integration Testing & Hardening

**Goal:** End-to-end tests that validate replication, failure recovery, and CLI correctness against a live local cluster.

### Tasks

1. Create `tests/integration/test_replication.py`:
   - Start local cluster using `scripts/dev-cluster.sh` as a pytest fixture (subprocess, await port readiness).
   - Test: PUT 1 MB object; verify object files exist on exactly 3 storage node data dirs.
   - Test: kill one storage node process; wait 60 s; verify `coordinator_active_nodes` decrements; verify rebalance completes (object files again present on 3 nodes).
   - Test: GET after node kill returns correct bytes with matching checksum.

2. Create `tests/integration/test_cli.py`:
   - Run `doss bucket create test-bucket` via `subprocess.run`; assert exit code 0.
   - Run `doss object put test-bucket mykey /tmp/testfile`; assert exit code 0.
   - Run `doss object get test-bucket mykey --output /tmp/retrieved`; assert file contents match.
   - Run `doss lifecycle set test-bucket tests/fixtures/lifecycle.json`; assert policy round-trips correctly.

3. Create `tests/fixtures/lifecycle.json`:
   ```json
   { "expiry_days": 30, "transition_after_days": 7, "transition_to": "cold" }
   ```

4. Add GitHub Actions workflow `scripts/ci.yml`:
   - Jobs: `lint` (golangci-lint + ruff), `unit-test` (go test + pytest), `integration-test` (start local cluster, run integration suite).
   - Use `actions/setup-go@v5` and `actions/setup-python@v5`.
   - Cache Go modules and pip dependencies.

5. Performance benchmark in `tests/bench/bench_put.py`:
   - Use `concurrent.futures.ThreadPoolExecutor` with 16 workers.
   - PUT 1000 objects of 100 KB each; record total time, calculate throughput (MB/s) and p99 latency.
   - Assert throughput > 50 MB/s on local cluster.
