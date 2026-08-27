#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="skiffdb-benchmark"
MANAGED_BY="skiffdb-microk8s"
IMAGE="${SKIFFDB_MICROK8S_IMAGE:-localhost:32000/skiffdb:microk8s}"
RESULTS_ROOT="${SKIFFDB_MICROK8S_RESULTS:-benchmarks/results/microk8s}"
FORWARD_BASE_PORT="${SKIFFDB_FORWARD_BASE_PORT:-16379}"
TARGETS=""
BENCH_BIN=""
WORK_DIR=""
FORWARD_PIDS=()
SAMPLER_PID=""

usage() {
  cat <<'USAGE'
Usage: scripts/microk8s.sh <command> [benchmark flags]

Commands:
  image              Build and push the local MicroK8s image
  deploy             Build the image and deploy/wait for the three-node cluster
  status             Show StatefulSet, Pod, PVC, Service, and PDB status
  benchmark [flags]  Run the remote smoke benchmark from the host
  benchmark-formal   Run five 60-second mixed-workload repetitions and sample
                     Kubernetes CPU/memory usage (override with
                     SKIFFDB_BENCHMARK_RUNS, _DURATION, _WARMUP, _CONCURRENCY,
                     _KEYS, and _SEED)
  restart-follower   Delete a follower, write while it is down, and verify catch-up
  failover           Delete the leader and measure time to a new writable leader
  logs               Print logs from all SkiffDB pods
  clean              Delete only the managed skiffdb-benchmark namespace

Single-host MicroK8s results validate deployment and regression behavior. They
must not be presented as multi-host or production performance measurements.
USAGE
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

kube() {
  microk8s kubectl "$@"
}

namespace_managed() {
  local value
  value="$(kube get namespace "${NAMESPACE}" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}' 2>/dev/null || true)"
  [[ "${value}" == "${MANAGED_BY}" ]]
}

build_image() {
  require_command docker
  microk8s status --wait-ready >/dev/null
  echo "Building ${IMAGE}"
  docker build --tag "${IMAGE}" "${ROOT_DIR}"
  echo "Pushing ${IMAGE} to the MicroK8s registry"
  docker push "${IMAGE}"
}

deploy() {
  build_image
  local redeploy=false
  if kube -n "${NAMESPACE}" get statefulset skiffdb >/dev/null 2>&1; then
    redeploy=true
  fi
  kube apply -k "${ROOT_DIR}/deploy/microk8s"
  kube -n "${NAMESPACE}" set image statefulset/skiffdb skiffdb="${IMAGE}"
  if [[ "${redeploy}" == "true" ]]; then
    kube -n "${NAMESPACE}" rollout restart statefulset/skiffdb
  fi
  kube -n "${NAMESPACE}" rollout status statefulset/skiffdb --timeout=300s
  status
}

status() {
  if ! kube get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "namespace ${NAMESPACE} is not deployed"
    return 0
  fi
  kube -n "${NAMESPACE}" get statefulset,pods,pvc,service,poddisruptionbudget -o wide
}

init_work_dir() {
  if [[ -n "${WORK_DIR}" ]]; then
    return
  fi
  WORK_DIR="$(mktemp -d /tmp/skiffdb-microk8s.XXXXXX)"
}

stop_forwards() {
  local pid
  for pid in "${FORWARD_PIDS[@]:-}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" >/dev/null 2>&1 || true
    fi
  done
  FORWARD_PIDS=()
}

stop_sampler() {
  if [[ -n "${SAMPLER_PID}" ]]; then
    kill "${SAMPLER_PID}" >/dev/null 2>&1 || true
    wait "${SAMPLER_PID}" >/dev/null 2>&1 || true
    SAMPLER_PID=""
  fi
}

cleanup() {
  stop_sampler
  stop_forwards
  if [[ -n "${WORK_DIR}" && "${WORK_DIR}" == /tmp/skiffdb-microk8s.* ]]; then
    rm -rf -- "${WORK_DIR}"
  fi
}
trap cleanup EXIT INT TERM

wait_for_port() {
  local port="$1"
  local attempt
  for attempt in $(seq 1 150); do
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
      exec 3>&-
      return 0
    fi
    sleep 0.2
  done
  echo "timed out waiting for port-forward on 127.0.0.1:${port}" >&2
  return 1
}

start_forwards() {
  init_work_dir
  stop_forwards
  TARGETS=""
  local index port
  for index in 0 1 2; do
    port=$((FORWARD_BASE_PORT + index))
    kube -n "${NAMESPACE}" port-forward --address 127.0.0.1 "pod/skiffdb-${index}" "${port}:6379" \
      >"${WORK_DIR}/port-forward-${index}.log" 2>&1 &
    FORWARD_PIDS+=("$!")
    if [[ -n "${TARGETS}" ]]; then
      TARGETS+=","
    fi
    TARGETS+="127.0.0.1:${port}"
  done
  for index in 0 1 2; do
    wait_for_port "$((FORWARD_BASE_PORT + index))"
  done
}

build_bench() {
  init_work_dir
  if ! compgen -G "${ROOT_DIR}/proto/cluster/*.pb.go" >/dev/null; then
    make -C "${ROOT_DIR}" proto
  fi
  BENCH_BIN="${WORK_DIR}/skiffdb-bench"
  (cd "${ROOT_DIR}" && go build -o "${BENCH_BIN}" ./benchmarks/cmd/skiffdb-bench)
}

leader_index() {
  "${BENCH_BIN}" leader --targets "${TARGETS}" --deployment durable-three-voter | awk '{print $1}'
}

wait_for_recreated_pod() {
  local pod="$1"
  local old_uid="$2"
  local attempt uid ready
  for attempt in $(seq 1 180); do
    uid="$(kube -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    ready="$(kube -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
    if [[ -n "${uid}" && "${uid}" != "${old_uid}" && "${ready}" == "True" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for ${pod} to be recreated and Ready" >&2
  return 1
}

benchmark() {
  build_bench
  start_forwards
  mkdir -p "${ROOT_DIR}/${RESULTS_ROOT}"
  (cd "${ROOT_DIR}" && "${BENCH_BIN}" remote \
    --targets "${TARGETS}" \
    --deployment durable-three-voter \
    --profile smoke \
    --results "${RESULTS_ROOT}" \
    "$@")
}

sample_pod_resources() {
  local output="$1"
  printf 'timestamp_utc\tpod\tcpu\tmemory\n' >"${output}"
  while true; do
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    while read -r pod cpu memory; do
      if [[ -n "${pod}" ]]; then
        printf '%s\t%s\t%s\t%s\n' "${timestamp}" "${pod}" "${cpu}" "${memory}"
      fi
    done < <(kube -n "${NAMESPACE}" top pods --no-headers 2>/dev/null || true)
    sleep 1
  done >>"${output}"
}

benchmark_formal() {
  local runs="${SKIFFDB_BENCHMARK_RUNS:-5}"
  local duration="${SKIFFDB_BENCHMARK_DURATION:-60s}"
  local warmup="${SKIFFDB_BENCHMARK_WARMUP:-10s}"
  local concurrency="${SKIFFDB_BENCHMARK_CONCURRENCY:-8}"
  local keys="${SKIFFDB_BENCHMARK_KEYS:-4096}"
  local base_seed="${SKIFFDB_BENCHMARK_SEED:-1}"
  if [[ ! "${runs}" =~ ^[1-9][0-9]*$ ]]; then
    echo "SKIFFDB_BENCHMARK_RUNS must be a positive integer" >&2
    exit 2
  fi

  build_bench
  start_forwards
  mkdir -p "${ROOT_DIR}/${RESULTS_ROOT}"

  local run seed resource_log output result_dir result_path
  for run in $(seq 1 "${runs}"); do
    seed=$((base_seed + run - 1))
    resource_log="${WORK_DIR}/kubernetes-top-${run}.tsv"
    echo "Formal benchmark ${run}/${runs}: duration=${duration}, warmup=${warmup}, concurrency=${concurrency}, keys=${keys}, seed=${seed}"
    sample_pod_resources "${resource_log}" &
    SAMPLER_PID="$!"
    if ! output="$(cd "${ROOT_DIR}" && "${BENCH_BIN}" remote \
      --targets "${TARGETS}" \
      --deployment durable-three-voter \
      --profile smoke \
      --duration "${duration}" \
      --warmup "${warmup}" \
      --concurrency "${concurrency}" \
      --pipeline 1 \
      --keys "${keys}" \
      --seed "${seed}" \
      --results "${RESULTS_ROOT}")"; then
      stop_sampler
      printf '%s\n' "${output}"
      return 1
    fi
    stop_sampler
    printf '%s\n' "${output}"
    result_dir="$(printf '%s\n' "${output}" | awk '$1 == "results:" {print $2}')"
    result_path="${ROOT_DIR}/${result_dir}"
    if [[ "${result_dir}" != "${RESULTS_ROOT}/"* || ! -d "${result_path}" ]]; then
      echo "could not identify benchmark result directory from: ${output}" >&2
      return 1
    fi
    cp "${resource_log}" "${result_path}/kubernetes-top.tsv"
    kube -n "${NAMESPACE}" get pods -o wide >"${result_path}/kubernetes-pods.txt"
    kube get node -o wide >"${result_path}/kubernetes-nodes.txt"
  done
}

restart_follower() {
  build_bench

  start_forwards
  local leader follower pod old_uid marker marker_value follower_port
  leader="$(leader_index)"
  if [[ "${leader}" == "0" ]]; then
    follower=1
  else
    follower=0
  fi
  pod="skiffdb-${follower}"
  old_uid="$(kube -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')"
  echo "Deleting follower ${pod}; current leader is skiffdb-${leader}"
  kube -n "${NAMESPACE}" delete pod "${pod}" --wait=true

  marker="bench:microk8s-catchup"
  marker_value="$(date +%s%N)"
  "${BENCH_BIN}" write --targets "${TARGETS}" --deployment durable-three-voter \
    --key "${marker}" --value "${marker_value}" >/dev/null
  wait_for_recreated_pod "${pod}" "${old_uid}"

  start_forwards
  follower_port=$((FORWARD_BASE_PORT + follower))
  "${BENCH_BIN}" wait-value --target "127.0.0.1:${follower_port}" \
    --key "${marker}" --value "${marker_value}" --timeout 30s
  echo "Follower ${pod} restarted with its PVC and caught up successfully."
}

failover() {
  build_bench
  start_forwards
  local leader pod old_uid started finished new_leader
  leader="$(leader_index)"
  pod="skiffdb-${leader}"
  old_uid="$(kube -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')"
  echo "Deleting leader ${pod}"
  started="$(date +%s%3N)"
  kube -n "${NAMESPACE}" delete pod "${pod}" --wait=true
  while ! new_leader="$(leader_index 2>/dev/null)"; do
    sleep 0.05
  done
  finished="$(date +%s%3N)"
  echo "New writable leader: skiffdb-${new_leader}"
  echo "Pod deletion to writable leader: $((finished - started)) ms"
  wait_for_recreated_pod "${pod}" "${old_uid}"
}

clean_namespace() {
  if ! kube get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "namespace ${NAMESPACE} is already absent"
    return
  fi
  if ! namespace_managed; then
    echo "refusing to delete namespace ${NAMESPACE}: managed-by label is not ${MANAGED_BY}" >&2
    exit 1
  fi
  kube delete namespace "${NAMESPACE}" --wait=true
}

main() {
  require_command microk8s
  local command="${1:-}"
  if [[ -z "${command}" ]]; then
    usage
    exit 2
  fi
  shift
  case "${command}" in
    image) build_image ;;
    deploy) deploy ;;
    status) status ;;
    benchmark) benchmark "$@" ;;
    benchmark-formal) benchmark_formal ;;
    restart-follower) restart_follower ;;
    failover) failover ;;
    logs) kube -n "${NAMESPACE}" logs statefulset/skiffdb --all-pods=true ;;
    clean) clean_namespace ;;
    help|-h|--help) usage ;;
    *)
      echo "unknown command: ${command}" >&2
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
