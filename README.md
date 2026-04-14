# Multi-Container Runtime with Kernel Memory Monitor

## 1. Team Information

- Shishir Hegde, SRN: PES1UG24CS438
- Sudeeksh Nayak, SRN: PES1UG24CS474

## 2. Project Summary

This project implements a lightweight container runtime in C with:

- A long-running supervisor process (`engine supervisor`) that manages multiple containers.
- A CLI client (`engine start/run/ps/logs/stop`) that communicates with the supervisor.
- A bounded-buffer logging pipeline for concurrent container output capture.
- A Linux kernel module (`monitor.ko`) that enforces soft/hard memory limits.
- Scheduler experiments using CPU-bound and I/O-bound workloads.

## 3. Build, Load, and Run Instructions (Ubuntu VM)

### 3.1 Environment Assumptions

- Ubuntu 22.04 or Ubuntu 24.04 VM
- Secure Boot disabled (required for unsigned kernel modules)
- WSL is not supported for this assignment

### 3.2 Install Dependencies

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

`build-essential` provides user-space compilation tools and `linux-headers-$(uname -r)` is required to build `monitor.ko` for the running kernel.

### 3.3 Prepare Base Root Filesystem

Run from repository root:

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

This creates a minimal Linux userspace used as the container root filesystem template.

### 3.4 Create Per-Container Writable Rootfs Copies

```bash
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
cp -a ./rootfs-base ./rootfs-gamma
```

Each running container must use a separate writable rootfs copy.

### 3.5 Build Runtime, Kernel Module, and Workloads

```bash
cd boilerplate
make
```

Build output includes:

- `engine` (supervisor + CLI)
- `monitor.ko` (kernel monitor module)
- `cpu_hog`, `memory_hog`, `io_pulse` (workloads)

### 3.6 Copy Workloads into Rootfs

From repository root:

```bash
cp ./boilerplate/memory_hog ./rootfs-alpha/
cp ./boilerplate/cpu_hog    ./rootfs-alpha/
cp ./boilerplate/io_pulse   ./rootfs-beta/
cp ./boilerplate/cpu_hog    ./rootfs-beta/
cp ./boilerplate/cpu_hog    ./rootfs-gamma/
```

These binaries are executed inside the container after `chroot`.

### 3.7 Load Kernel Module and Verify Device

```bash
cd boilerplate
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

Expected: `/dev/container_monitor` exists and is readable/writable by root.

### 3.8 Start Supervisor (Terminal A)

```bash
cd boilerplate
sudo ./engine supervisor ../rootfs-base
```

The supervisor remains running and serves control requests over `/tmp/mini_runtime.sock`.

### 3.9 Use CLI Commands (Terminal B)

```bash
cd boilerplate
sudo ./engine start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
sudo ./engine run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
sudo ./engine ps
sudo ./engine logs <id>
sudo ./engine stop <id>
```

Behavior:

- `start`: background launch and immediate response.
- `run`: foreground launch; waits for final exit state.
- `ps`: prints container metadata and final reason/state.
- `logs`: prints per-container log file content.
- `stop`: graceful request, then escalation if needed.

### 3.10 End-to-End Demo Command Sequence

```bash
cd boilerplate

# Start two containers
sudo ./engine start alpha ../rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta  ../rootfs-beta  /bin/sh --soft-mib 64 --hard-mib 96

# Inspect tracked metadata
sudo ./engine ps

# Logs
sudo ./engine logs alpha

# Foreground run example
sudo ./engine run probe ../rootfs-gamma /bin/ps

# Stop containers
sudo ./engine stop alpha
sudo ./engine stop beta
```

### 3.11 Memory Limit Tests

Soft/hard threshold test command:

```bash
cd boilerplate
sudo ./engine start memsoft ../rootfs-alpha /memory_hog --soft-mib 20 --hard-mib 80
sleep 3
sudo ./engine ps
sudo dmesg | tail -n 100
```

Expected behavior:

- Soft threshold crossing should emit a one-time warning from the kernel module.
- Hard threshold crossing should kill the container and reflect `reason=hard_limit_killed` in `engine ps`.

### 3.12 Scheduler Experiment Commands

CPU vs CPU with different priorities:

```bash
cd boilerplate
time sudo ./engine run gamma1 ../rootfs-gamma /cpu_hog --nice 0
time sudo ./engine run gamma2 ../rootfs-beta  /cpu_hog --nice 10
```

Observed in our run:

- `gamma1` (`nice 0`): `real 0m9.238s`
- `gamma2` (`nice 10`): `real 0m10.560s`

This shows the lower-priority workload (`nice 10`) taking longer.

### 3.13 Shutdown and Cleanup

```bash
# In supervisor terminal: Ctrl+C to stop supervisor
cd boilerplate
ps aux | grep engine
sudo rmmod monitor
ls -l /dev/container_monitor
```

Expected after unload: `/dev/container_monitor` no longer exists.

### 3.14 CI-Safe Build Path

```bash
make -C boilerplate ci
```

This target is intended for quick compile checks in CI without running privileged runtime/module steps.

## 4. Demo with Screenshots (Required Evidence)

### 1) Multi-container supervision

Two containers (`alpha`, `beta`) running concurrently under one supervisor.

![Multi-container supervision](Task1/Screenshot%202026-04-15%20001533.png)

### 2) Metadata tracking (`engine ps`)

`engine ps` output showing container IDs, host PIDs, states, limits, start/finish time, and log paths.

![Metadata tracking](Task1/Screenshot%202026-04-15%20002748.png)

### 3) Bounded-buffer logging pipeline

`engine run` followed by `engine logs` and on-disk log file confirmation (`logs/logdemo.log`).

![Logging evidence](Task1/Screenshot%202026-04-15%20003148.png)

### 4) CLI request/response over control IPC

CLI `start` commands return success responses (`Started container ...`) from the long-running supervisor.

![CLI control flow](Task1/Screenshot%202026-04-15%20001533.png)

### 5) Soft-limit test evidence

Soft-limit scenario command execution and `dmesg` capture attempt for warning verification.

![Soft-limit scenario](Task1/Screenshot%202026-04-15%20003329.png)
![Kernel log capture attempt](Task1/Screenshot%202026-04-15%20004511.png)

### 6) Hard-limit enforcement evidence

Container is marked as `state=hard_limit_killed` with `reason=hard_limit_killed` and `exit_signal=9`.

![Hard-limit enforcement](Task1/Screenshot%202026-04-15%20003501.png)

### 7) Scheduler experiment evidence

Priority difference (`nice 0` vs `nice 10`) produced different completion times.

![Scheduler experiment](Task1/Screenshot%202026-04-15%20004735.png)

### 8) Clean teardown evidence

No monitored device after module unload and process cleanup checks performed.

![Teardown process state](Task1/Screenshot%202026-04-15%20004939.png)
![Monitor device removed](Task1/Screenshot%202026-04-15%20005200.png)

## 5. Full Screenshot-to-Task Mapping (All Files in `Task1/`)

| Screenshot File | Task / Checkpoint Mapping | What It Shows |
| --- | --- | --- |
| `Screenshot 2026-04-15 000828.png` | Setup | Base rootfs download/extract and rootfs copy creation |
| `Screenshot 2026-04-15 001037.png` | Setup | Workload binaries copied into rootfs directories |
| `Screenshot 2026-04-15 001124.png` | Task 4 setup | `monitor.ko` loaded and `/dev/container_monitor` present |
| `Screenshot 2026-04-15 001314.png` | Task 1 | Supervisor process started |
| `Screenshot 2026-04-15 001443.png` | Task 2 | `engine ps` baseline: no containers tracked |
| `Screenshot 2026-04-15 001533.png` | Task 1 + Task 2 | `start` for two containers and metadata listing |
| `Screenshot 2026-04-15 002122.png` | Task 2 | `stop` flow with escalation behavior |
| `Screenshot 2026-04-15 002306.png` | Task 2 + Task 6 | Final stopped state with reason attribution |
| `Screenshot 2026-04-15 002748.png` | Task 1 + Task 2 | Short-lived probe container and mixed-state `ps` |
| `Screenshot 2026-04-15 003031.png` | Task 6 | Reaping check (`ps -p <pid>` empty result) |
| `Screenshot 2026-04-15 003148.png` | Task 3 | Foreground run, log retrieval, and persisted log file |
| `Screenshot 2026-04-15 003329.png` | Task 4 (soft-limit run) | Soft/hard config test launched (`memory_hog`) |
| `Screenshot 2026-04-15 003501.png` | Task 4 (hard-limit enforcement) | `hard_limit_killed` reflected in metadata |
| `Screenshot 2026-04-15 004511.png` | Task 4 support | Kernel log output capture command run |
| `Screenshot 2026-04-15 004545.png` | Task 5 | Scheduling experiment start (`nice 0` vs `nice 10`) |
| `Screenshot 2026-04-15 004735.png` | Task 5 | Scheduling timings showing priority impact |
| `Screenshot 2026-04-15 004939.png` | Task 2 + Task 6 | Final container states and engine process inspection |
| `Screenshot 2026-04-15 005200.png` | Task 6 | Module unload verified (`/dev/container_monitor` removed) |

## 6. Engineering Analysis

### 6.1 Isolation Mechanisms

Container processes are created with namespace isolation (`PID`, `UTS`, `mount`) and run inside their own rootfs copy via `chroot`. Mounting `/proc` inside each container root ensures process tools operate in that container view. This gives strong process-tree and filesystem view isolation while still sharing the same host kernel, CPU scheduler, and physical memory subsystem.

### 6.2 Supervisor and Process Lifecycle

The supervisor is a long-running authority for lifecycle state:

- Accepts requests (`start`, `run`, `ps`, `logs`, `stop`) from short-lived CLI clients.
- Tracks per-container metadata including PID, limits, state, exit code/signal, timestamps, and reason.
- Reaps children to avoid zombies and preserve final reason attribution.
- Distinguishes stop-requested termination from hard-limit kill using internal `stop_requested` tracking.

This architecture keeps state consistent even with concurrent commands.

### 6.3 IPC, Threads, and Synchronization

Two different IPC paths are used:

- Control IPC: UNIX domain socket (`/tmp/mini_runtime.sock`) between CLI and supervisor.
- Logging IPC: per-container `stdout/stderr` pipes into the supervisor.

Logging is implemented as a bounded producer-consumer queue:

- Producer thread(s): read pipe output and enqueue log chunks.
- Consumer thread(s): dequeue chunks and write to per-container log files.
- Synchronization: mutex + condition variables for full/empty queue conditions, plus a separate metadata lock.

Without synchronization, races would corrupt queue indices, lose lines, or deadlock under backpressure. The bounded queue design also decouples container write rate from disk write latency.

### 6.4 Memory Management and Enforcement

The kernel module tracks container host PIDs and periodically samples RSS. Policy is split into:

- Soft limit: one warning event on first crossing.
- Hard limit: immediate `SIGKILL` and monitor entry cleanup.

Kernel-space enforcement is important because it observes and controls processes with reliable authority, while a pure user-space checker can miss fast spikes or lose races under load. In our run, hard-limit enforcement is visible via `state=hard_limit_killed` and `exit_signal=9`.

### 6.5 Scheduling Behavior

Scheduler experiment results:

- `nice 0` CPU workload: `real 9.238s`
- `nice 10` CPU workload: `real 10.560s`

The lower-priority process (`nice 10`) took about `1.322s` longer (roughly 14.3 percent slower in this run). This matches CFS expectations: both tasks make progress, but lower niceness value receives comparatively better CPU share.

## 7. Design Decisions and Tradeoffs

- Namespace + `chroot` isolation:
  - Choice: `PID/UTS/mount` namespaces and `chroot` with per-container rootfs.
  - Tradeoff: simpler than `pivot_root`, but less strict in root-switch semantics.
  - Why chosen: fast, clear implementation aligned with assignment requirements.
- Central supervisor daemon:
  - Choice: single long-running process owns all metadata, control, and logging.
  - Tradeoff: requires careful locking and shutdown ordering.
  - Why chosen: consistent source of truth for all container states.
- Split IPC design:
  - Choice: UNIX socket for control plane and pipes for output plane.
  - Tradeoff: more components than a single channel.
  - Why chosen: clean separation of command/response from stream logging.
- Kernel memory monitor policy:
  - Choice: periodic RSS checks with soft warning and hard kill.
  - Tradeoff: sampling may miss transient spikes between ticks.
  - Why chosen: predictable, easy to reason about, and assignment-compliant.
- Reason attribution (`stopped_forced` vs `hard_limit_killed`):
  - Choice: maintain stop intent (`stop_requested`) in runtime metadata.
  - Tradeoff: ordering between signal send, monitor kill, and reap must be correct.
  - Why chosen: clear grading-visible distinction in `engine ps`.

## 8. Task Coverage Map

- Task 1: Multi-container runtime and supervisor implemented in `boilerplate/engine.c`.
- Task 2: CLI contract, control IPC, and stop/run handling in `boilerplate/engine.c`.
- Task 3: Bounded-buffer producer-consumer logging pipeline in `boilerplate/engine.c`.
- Task 4: Kernel monitor register/unregister + soft/hard enforcement in `boilerplate/monitor.c`.
- Task 5: Scheduler experiment support (`--nice`) and workload binaries.
- Task 6: End-to-end cleanup in runtime shutdown, child reaping, and module unload.

## 9. Submission Checklist

Include these in the final submitted repository:

- `boilerplate/engine.c`
- `boilerplate/monitor.c`
- `boilerplate/monitor_ioctl.h`
- Workloads (`boilerplate/cpu_hog.c`, `boilerplate/memory_hog.c`, `boilerplate/io_pulse.c`)
- `boilerplate/Makefile` with `make` and `make -C boilerplate ci` working
- `README.md` (this file)
- `Task1/` screenshot evidence folder

Optional but recommended for grading convenience:

- Keep `project-guide.md` in repo root
- Keep `.github/workflows/submission-smoke.yml` for CI compile checks
