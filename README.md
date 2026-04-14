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

Conclusion of scheduling experiment:

- The `nice 0` run finished about `1.322s` faster than `nice 10`.
- This is consistent with Linux CFS behavior, where lower niceness values receive higher scheduling weight.
- Both runs completed successfully (`normal_exit`), so the scheduler still preserved forward progress for the lower-priority task.
- Since `run` is foreground and blocking, these timings should be interpreted as priority impact under similar system conditions, not as strict same-instant parallel benchmarking.

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

## 5. Engineering Analysis

### 5.1 Isolation Mechanisms

The runtime uses Linux namespaces and filesystem root switching to provide container boundaries in user space without a full VM. `PID` namespace isolation ensures each container sees its own process tree. `UTS` namespace isolation gives each container its own hostname domain. `mount` namespace isolation provides a separate mount table so `/proc` can be mounted inside the container root without polluting host mounts.

`chroot` places each container in its own rootfs copy (`rootfs-alpha`, `rootfs-beta`, etc.), which prevents normal absolute-path traversal to host files. Combined with per-container writable rootfs, this avoids direct filesystem interference between containers. Mounting `/proc` inside that root is necessary so process tools (`ps`, etc.) reflect container-visible PIDs and state.

This is still OS-level isolation, not hardware virtualization. All containers share the same host kernel, scheduler, and physical memory manager. As a result, kernel bugs, global CPU contention, and host-level memory pressure remain shared realities across containers.

### 5.2 Supervisor and Process Lifecycle

The architecture uses a long-running supervisor as a control plane and state authority. CLI commands are short-lived clients that submit requests and exit, while the supervisor remains responsible for lifecycle continuity even when clients terminate.

Lifecycle management includes:

- Container creation with configured policy (`soft/hard` memory limits, `nice`, rootfs path).
- State transitions (`starting`, `running`, `exited`, `stopped`, `hard_limit_killed`) persisted in metadata.
- Exit attribution via signal/exit code plus stop intent (`stop_requested`) to separate manual stop from monitor kill.
- Reaping of children so terminated processes do not remain as zombies.

This design improves correctness and observability. If control logic were distributed across transient CLI processes, state would fragment, race handling would be weaker, and final attribution in `engine ps` would be unreliable.

### 5.3 IPC, Threads, and Synchronization

The project intentionally uses two IPC mechanisms for two different traffic patterns:

- Control IPC: UNIX domain socket (`/tmp/mini_runtime.sock`) for request/response commands.
- Data IPC: pipes for unstructured stdout/stderr streams from containers.

For logs, producers and consumers are decoupled using a bounded queue. Producer thread(s) perform pipe reads and enqueue chunks. Consumer thread(s) dequeue and append to persistent per-container log files. This separation prevents slow disk writes from directly blocking container output as often as direct synchronous file writes would.

Synchronization is required at multiple layers:

- Queue lock + condition variables to protect head/tail indices and full/empty conditions.
- Metadata lock to protect container records while multiple threads update state and timestamps.
- Shutdown coordination so producers stop cleanly, consumers drain remaining entries, and joins complete without deadlock.

Without these controls, typical failure modes include queue corruption, dropped or interleaved logs, deadlocks when buffers fill, and incorrect final states caused by concurrent updates.

### 5.4 Memory Management and Enforcement

The kernel module monitors host PIDs registered by the supervisor and periodically samples RSS. RSS represents resident physical pages currently mapped for the process and is a practical signal for memory pressure, but it is not a complete memory-accounting view of every kernel-side allocation. This is why limits are policy controls, not exact byte-perfect accounting guarantees.

The two-threshold policy captures two different operational intents:

- Soft limit: warning-level signal to indicate abnormal growth while allowing workload continuation.
- Hard limit: enforcement boundary that terminates the process (`SIGKILL`) to protect host stability and other containers.

Kernel-space enforcement is a key engineering decision. User-space monitors can observe and request termination, but they cannot provide the same timing authority or race resistance under heavy load. By enforcing in kernel context, the project achieves more deterministic kill behavior and clearer safety boundaries. The resulting kill attribution is propagated back to user-space metadata as `hard_limit_killed`.

### 5.5 Scheduling Behavior

The scheduling experiment measured CPU-bound workloads at two priority levels:

- `nice 0`: `real 9.238s`
- `nice 10`: `real 10.560s`

Difference:

- Absolute gap: `1.322s`
- Relative slowdown for `nice 10`: about `14.3%`

Interpretation:

- CFS did not starve either job; both exited normally.
- The higher-priority job (`nice 0`) received enough additional CPU share to complete sooner.
- The outcome aligns with CFS virtual-runtime weighting, where niceness adjusts fairness weight rather than creating hard real-time guarantees.

Methodological note:

- Because `run` is foreground, these are sequential timed runs under similar machine conditions.
- For stronger reproducibility, repeated trials and low background host load should be used, and optional parallel start-based experiments can be added.

## 6. Design Decisions and Tradeoffs

### 6.1 Namespace and Filesystem Isolation Strategy

Choice:

- Use `PID/UTS/mount` namespaces with per-container `chroot` and `/proc` mount.

Tradeoff:

- Faster to implement and debug than `pivot_root`, but less strict root-switch semantics.

Why this was chosen:

- It satisfies assignment isolation goals with clear runtime behavior and minimal bootstrap complexity.

Impact:

- Containers are sufficiently isolated for process/filesystem experiments, while keeping implementation size manageable.

### 6.2 Supervisor-Centric Control Plane

Choice:

- Keep one persistent supervisor responsible for orchestration, metadata, and logging ownership.

Tradeoff:

- Adds synchronization and shutdown complexity because many concurrent actions converge in one process.

Why this was chosen:

- Single ownership model gives deterministic state and clear CLI semantics, especially for `run`, `stop`, and final reason reporting.

Impact:

- Better operational visibility (`engine ps`) and cleaner lifecycle handling compared to peer-to-peer or ad hoc control.

### 6.3 Dual IPC Channels (Control vs Output)

Choice:

- Use UNIX socket for control and pipes for container output.

Tradeoff:

- More moving parts than a single channel implementation.

Why this was chosen:

- Control messages are structured and low volume; logs are streaming and bursty. Separate channels match these traffic profiles and simplify parser and backpressure logic.

Impact:

- Improved robustness and easier debugging of protocol vs stream failures.

### 6.4 Bounded Logging Queue Design

Choice:

- Introduce bounded producer-consumer buffering between pipe readers and file writers.

Tradeoff:

- Requires careful condition-variable logic and termination coordination.

Why this was chosen:

- Prevents direct coupling between container write throughput and filesystem latency; supports clean flushing during shutdown.

Impact:

- More stable logging under bursty output and better correctness around abrupt container exits.

### 6.5 Kernel-Space Memory Policy Enforcement

Choice:

- Enforce soft/hard thresholds in the kernel module using periodic checks on registered PIDs.

Tradeoff:

- Sampling-based policy can miss very short transient peaks between ticks.

Why this was chosen:

- Straightforward implementation with deterministic enforcement path and clear audit trail via kernel logs and supervisor metadata.

Impact:

- Reliable hard-limit containment and transparent post-mortem reasoning (`hard_limit_killed`).

### 6.6 Stop Intent and Final Reason Attribution

Choice:

- Track `stop_requested` in user-space metadata before signaling, then resolve final reason after reap.

Tradeoff:

- Ordering bugs can mislabel termination source if not synchronized with reap/monitor actions.

Why this was chosen:

- Assignment grading requires clear distinction between manual stop, normal exit, and hard-limit kill.

Impact:

- `engine ps` becomes a trustworthy forensic summary rather than a raw signal dump.
