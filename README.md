# Multi-Container Runtime with Kernel Memory Monitor

## 1. Team Information

- Member 1: `<name>`, SRN: `<srn>`
- Member 2: `<name>`, SRN: `<srn>`

## 2. Build, Load, and Run Instructions (Ubuntu 22.04/24.04 VM)

### Environment

- Ubuntu 22.04 or 24.04 VM
- Secure Boot disabled
- WSL not supported

### Install Dependencies

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

### Prepare Root Filesystem

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

### Build

```bash
cd boilerplate
make
```

### Load Kernel Module

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

### Start Supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

### Create Per-Container Writable Rootfs Copies

```bash
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
cp -a ./rootfs-base ./rootfs-gamma
```

### Copy Workloads into Rootfs

```bash
cp ./memory_hog ./rootfs-alpha/
cp ./cpu_hog ./rootfs-alpha/
cp ./io_pulse ./rootfs-beta/
cp ./cpu_hog ./rootfs-beta/
cp ./cpu_hog ./rootfs-gamma/
```

### CLI Commands

```bash
sudo ./engine start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
sudo ./engine run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
sudo ./engine ps
sudo ./engine logs <id>
sudo ./engine stop <id>
```

### Example Session

```bash
# background start
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta  ./rootfs-beta  /bin/sh --soft-mib 64 --hard-mib 96

# inspect metadata
sudo ./engine ps

# inspect logs
sudo ./engine logs alpha

# run a foreground workload and return its final status
sudo ./engine run gamma ./rootfs-gamma /cpu_hog --nice 10
echo $?

# stop containers
sudo ./engine stop alpha
sudo ./engine stop beta
```

### Shutdown and Cleanup

```bash
# if supervisor is running in another terminal, stop it with Ctrl+C
dmesg | tail -n 100
sudo rmmod monitor
```

### CI-Safe Build Path

```bash
make -C boilerplate ci
```

## 3. Demo with Screenshots

Capture and annotate screenshots for:

1. Two or more containers under one supervisor
2. `engine ps` output showing metadata
3. Per-container logs written via bounded-buffer pipeline
4. CLI request/response flow over control IPC
5. Soft-limit warning in `dmesg`
6. Hard-limit kill in `dmesg` plus `ps` reason state
7. Scheduler experiment outputs with visible differences
8. Clean teardown evidence (no zombies, supervisor exits cleanly)

## 4. Engineering Analysis

### Isolation Mechanisms

The runtime uses `clone()` with `CLONE_NEWPID`, `CLONE_NEWUTS`, and `CLONE_NEWNS` to create isolated process ID, hostname, and mount views. Each container process then executes `chroot(container-rootfs)` and mounts `/proc` inside that root, so process tools run against container PID namespace state. All containers still share the same host kernel, scheduler, and physical memory manager.

### Supervisor and Process Lifecycle

The supervisor is a long-running parent that owns container metadata, logging, and control IPC. It starts container children, tracks their host PIDs and lifecycle state, and continuously reaps exits through a dedicated reaper thread using `waitpid`. This avoids zombie accumulation and preserves final reason/state attribution for `ps`.

### IPC, Threads, and Synchronization

Two IPC paths are used:

- Control path: CLI client to supervisor via UNIX domain socket at `/tmp/mini_runtime.sock`
- Logging path: container stdout/stderr redirected through pipe to supervisor

Logging uses a bounded producer-consumer queue in user space. Producer threads read from container pipes and enqueue chunks. A consumer thread dequeues and appends to per-container log files. Mutex and condition variables protect queue operations and prevent deadlock when full/empty. Container metadata is protected separately by a dedicated mutex.

### Memory Management and Enforcement

The kernel module tracks registered container host PIDs and periodically samples RSS. Soft-limit policy emits one warning event on first threshold crossing. Hard-limit policy sends `SIGKILL` and removes the monitor entry. Kernel-space enforcement is required for reliable observation and termination because user-space polling cannot guarantee the same authority or timing under load.

### Scheduling Behavior

Scheduler behavior is explored by running workloads with different execution profiles and priorities (`--nice`). CPU-bound workloads compete for CPU share and are influenced by relative priority; I/O-heavy workloads naturally yield and regain responsiveness around sleep and wake cycles. These experiments show Linux balancing fairness, throughput, and interactivity under CFS.

## 5. Design Decisions and Tradeoffs

### Namespace + chroot Isolation

- Choice: PID/UTS/mount namespaces with `chroot` + `/proc` mount
- Tradeoff: simpler than `pivot_root`, but `pivot_root` provides stricter root switching semantics
- Justification: faster implementation path while preserving required isolation behavior for this project

### Long-Running Supervisor

- Choice: one daemon process handling start/stop/ps/logs/run control
- Tradeoff: central process increases responsibility for synchronization and shutdown ordering
- Justification: gives one authoritative lifecycle state machine and consistent metadata/log ownership

### IPC and Logging Pipeline

- Choice: UNIX socket for control plane; pipe + bounded queue for logs
- Tradeoff: more moving parts than direct file writes
- Justification: avoids blocking container writes on disk latency and enforces clear separation of control vs output channels

### Kernel Monitor Policy

- Choice: linked-list tracked PIDs with periodic RSS checks
- Tradeoff: timer-driven sampling may not catch instantaneous peaks between ticks
- Justification: simple, predictable policy implementation suitable for the assignment’s soft/hard threshold model

### Stop/Hard-Limit Attribution

- Choice: `stop_requested` and forced-stop state tracked in user-space metadata
- Tradeoff: requires careful ordering between signals and reaping
- Justification: satisfies grading requirement to distinguish manual stop flow from hard-limit kill (`SIGKILL` without stop request)

## 6. Scheduler Experiment Results

Run these in separate terminals while supervisor is active:

```bash
# Terminal A
sudo ./engine run cpu_low  ./rootfs-alpha /cpu_hog --nice 10

# Terminal B
sudo ./engine run cpu_high ./rootfs-beta  /cpu_hog --nice 0
```

```bash
# Terminal A
sudo ./engine run cpu_mix ./rootfs-alpha /cpu_hog --nice 0

# Terminal B
sudo ./engine run io_mix  ./rootfs-beta  /io_pulse --nice 0
```

Record:

- start/end timestamps
- command exit status
- log progress cadence from `engine logs <id>`
- observable completion ordering and responsiveness

Recommended result table:

| Experiment | Workload A | Workload B | Scheduler Config | Outcome Summary |
| --- | --- | --- | --- | --- |
| CPU vs CPU priority split | `cpu_hog` | `cpu_hog` | `nice 10` vs `nice 0` | fill after run |
| CPU vs I/O concurrency | `cpu_hog` | `io_pulse` | both `nice 0` | fill after run |

## 7. Task Coverage Map

- Task 1: Multi-container supervisor runtime implemented in `boilerplate/engine.c`
- Task 2: Full CLI contract with control IPC and signal-aware `run` flow implemented in `boilerplate/engine.c`
- Task 3: Bounded-buffer logging pipeline with producer/consumer threading in `boilerplate/engine.c`
- Task 4: Kernel memory monitor with register/unregister, soft-limit warnings, hard-limit kills in `boilerplate/monitor.c`
- Task 5: Scheduler experiment workflow and runtime support (`--nice`, workload binaries, reproducible runbook) in README + workloads
- Task 6: End-to-end cleanup logic in supervisor shutdown, reaper flow, producer/consumer shutdown, and module unload cleanup
