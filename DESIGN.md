# Architecture & System Design Document (DESIGN.md)

## 1. Architectural Rationale

The monitoring service is designed to handle **5,000+ targets checked at 1-second intervals** within a strict **1 GB JVM heap (`-Xmx1g`)** while guaranteeing **0 Event Loop blocked thread warnings**.

To achieve this, we chose an **Asynchronous Actor-based Architecture** using **Eclipse Vert.x 5.0.0** and **Java 21**. State is decoupled across discrete Verticles communicating via an in-memory `localConsumer` EventBus.

---

## 2. Component Sizing & Rationale

| Component | Instance Count | Threading Model | Rationale |
| :--- | :---: | :--- | :--- |
| **`PersistenceWorker`** | **1** | Standard Verticle (EventLoop) + Dedicated Worker Pool | Single verticle manages the in-memory persistence mirror and coordinates batch flushes. Avoids multi-process file contention. |
| **`TargetManager`** | **1** | Standard Verticle (EventLoop) | Single source of truth for target registration and CRUD. Eliminates split-brain registry states and avoids cluster sync overhead. |
| **`TargetScheduler`** | **1** | Standard Verticle (EventLoop) | A single Min-Heap (`PriorityQueue`) driven by a single 20ms tick timer efficiently tracks all 5,000 targets without duplicate tick events. |
| **`StatsManager`** | **1** | Standard Verticle (EventLoop) | Maintains in-memory 300s ring buffers and health state machines for all targets. Single-threaded access guarantees zero-lock math computations. |
| **`CheckManager`** | **2** | Standard Verticle (EventLoop) | Scales outbound network I/O. Vert.x automatically round-robins check dispatch events across the 2 instances, distributing HTTP/TCP socket polling across CPU cores. |
| **`HttpServer`** | **2** | Standard Verticle (EventLoop) | Listens on port 8080 with socket reuse (`SO_REUSEPORT`). Inbound HTTP requests are automatically load-balanced across 2 event loops. |
| **`persistence-worker-pool`** | **5 threads** | Dedicated `WorkerExecutor` | Dedicated bulkhead pool isolating all blocking disk I/O. Prevents disk stalls from starving the default Vert.x worker pool. |

---

## 3. Event Loop vs. Worker Execution Boundaries

The Vert.x Golden Rule (**"Never block the Event Loop"**) is strictly enforced across the codebase.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EVENT LOOP THREADS (Non-Blocking)                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  • Inbound REST API Routing & JSON Serialization (HttpServer)               │
│  • Target Registration & ID Generation (TargetManager)                      │
│  • Min-Heap 20ms Priority Tick Evaluation (TargetScheduler)                 │
│  • Non-blocking HTTP (WebClient) & TCP (NetClient) Probe Dispatch           │
│  • Zero-Allocation 300s Ring Buffer Updates (SlidingWindowStats)            │
│  • Anti-Flapping State Machine Transitions (TargetHealthFSM)                │
│  • RAM Map/Queue Updates in PersistenceWorker (persistedTargets, auditBuffer)│
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ Offloaded via executor.executeBlocking()
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                  DEDICATED WORKER POOL (Blocking Disk I/O)                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  • Initial Startup Target Loading (Files.readString)                        │
│  • Periodic / Triggered Target File Overwrites (Files.writeString)          │
│  • Batch Audit Log Appending (Files.writeString with APPEND option)         │
│  • Pretty JSON File Formatting for Disk Exports                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Shared State & Concurrency Protection

To maximize performance, shared state is minimized and isolated using the Actor model and concurrency primitives:

### A. Target Registry & Scheduler State (Thread-Confined)
- **`TargetManager`** and **`TargetScheduler`** maintain state exclusively on their assigned EventLoop threads.
- All external interactions occur via immutable EventBus messages.
- **Protection**: **Thread Confinement**. Because all mutations happen sequentially on the single EventLoop thread, no synchronization locks or atomic wrappers are needed for target storage or priority queue mutations.

### B. Mathematical Engine & Health FSM (Thread-Confined)
- **`SlidingWindowStats`** (primitive arrays: `long[] timestamps`, `long[] latencies`, `int[] successCounts`) and **`TargetHealthFSM`** (3 primitive fields) are confined to **`StatsManager`**'s EventLoop thread.
- **Protection**: **Thread Confinement**. Zero mutexes, zero atomic contention, and zero GC allocation on `noChange` updates.

### C. Persistence & Audit Buffer (Multi-Thread Protected)
- **`persistedTargets`**: `ConcurrentHashMap<String, JsonObject>`
  - *Protection*: Lock-free reads/writes via CPU CAS and bucket-level locks. Defensive copies (`target.copy()`) prevent external mutation.
- **`auditBuffer` & `bufferSize`**: `ConcurrentLinkedQueue<JsonObject>` & `AtomicInteger`
  - *Protection*: Lock-free FIFO queue (Michael-Scott algorithm) for nanosecond event ingestion and atomic size tracking.
- **Disk File Locks**: `synchronized (targetFileLock)` and `synchronized (auditFileLock)`
  - *Protection*: JVM monitor synchronization inside `executeBlocking` guarantees physical files (`targets.json`, `audit.jsonl`) are never corrupted by concurrent worker threads.

---

## 5. Queue Behavior & Load Growth Analysis

Under heavy load (5,000 targets monitored every 1 second = 5,000 checks/sec), queues grow in strictly controlled, bounded structures:

```
[ 5,000 Targets Scheduled ]
             │
             ▼
┌────────────────────────────────────────────────────────┐
│ 1. TargetScheduler Min-Heap PriorityQueue              │
│    - Bounded to exactly N targets (O(N) memory)        │
│    - 5,000 targets occupy < 2 MB heap                  │
│    - Evaluated every 20ms: pop due items in O(k log N) │
└──────────────────────────┬─────────────────────────────┘
                           │
                 EventBus Dispatch (Round-Robin)
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ 2. CheckManager Active Semaphore Throttle              │
│    - maxConcurrentChecks = 500 per instance (1,000 max)│
│    - Backpressure: Excess checks rejected gracefully   │
│    - Prevents OS socket exhaustion (EMFILE)            │
└──────────────────────────┬─────────────────────────────┘
                           │
                 Check Results Broadcast
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ 3. PersistenceWorker Audit Buffer Queue                │
│    - ConcurrentLinkedQueue<JsonObject>                 │
│    - Flushed immediately when size >= bufferCapacity   │
│    - Flushed periodically every flushIntervalMs (1s)   │
│    - Peak memory bounded to < 500 KB                   │
└────────────────────────────────────────────────────────┘
```

1. **`TargetScheduler` Min-Heap (`PriorityQueue<ScheduledTarget>`)**:
   - Holds exactly $N$ elements (5,000 entries).
   - In each 20ms tick, it pops only targets whose `nextCheckTimestamp <= now`, reschedules them to `now + interval`, and pushes back. Memory footprint is strictly $O(N)$ ($\approx 1.8\text{ MB}$).
2. **`CheckManager` Concurrency Throttle**:
   - Caps concurrent in-flight probes to `maxConcurrentChecks` (500 per instance).
   - Protects the JVM and OS network stack from exhausting file descriptors (`ulimit -n`).
3. **`PersistenceWorker` Audit Buffer**:
   - Buffered in memory and flushed in batches of 100 entries or every 1,000ms.
   - Converts thousands of individual disk writes into single sequential disk appends, reducing disk IOPS by over 98%.
