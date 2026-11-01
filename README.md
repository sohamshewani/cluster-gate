# cluster-gate
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/cluster-gate/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/cluster-gate/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/cluster-gate) [![Stars](https://img.shields.io/github/stars/sohamshewani/cluster-gate?style=flat)](https://github.com/sohamshewani/cluster-gate)

## Executive Summary & Problem Space

`cluster-gate` is an ultra-low-latency, zero-allocation-on-hot-path concurrency gateway engine engineered in C11. It is designed to solve high-frequency synchronization, distributed rate-limiting, and ingress event policing challenges in high-throughput microservices and distributed storage systems. 

At extreme scales (>1M QPS per node), traditional synchronization primitives—such as POSIX mutexes, condition variables, and dynamic heap allocators (`malloc`/`free`)—induce severe lock contention, priority inversions, and cacheline bouncing. `cluster-gate` eliminates these bottlenecks through a deterministic architecture built upon lock-free multi-producer multi-consumer (MPMC) ring buffers, localized memory arenas, time-bucketed sliding windows, and deterministic clock-skew mitigation routines. It provides predictable nanosecond-scale tail latencies (p99.9) under heavy saturation, making it suitable for mission-critical infrastructure where deterministic bounded execution is mandatory.

---

## Structural ASCII Architecture / Dataflow Diagram

The system is organized into modular layers, separating memory management, lockless concurrency, and stateful tracking engines.

```
 +-----------------------------------------------------------------------+
 |                        Inbound Client Requests                        |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                     Concurrency / Ring Buffer                         |
 |           (src/concurrency/ring_buffer.c - Lockless MPMC)             |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                          Engine & State Logic                         |
 |              (include/cluster_gate/engine.h & src/core/)              |
 |                                                                       |
 |   +------------------------+      +-------------------------------+   |
 |   |      Bucket Engine     | <--> |      Clock Skew Evaluator     |   |
 |   |  (src/core/bucket.c)   |      |      (src/core/skew.c)        |   |
 |   +------------------------+      +-------------------------------+   |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                       Memory Allocation Subsystem                     |
 |       Arena Allocator (src/memory/arena.c) - Zero Hot-Path Malloc     |
 +-----------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

`cluster-gate` employs strict concurrency and memory safety invariants to guarantee safety across parallel worker threads without reliance on heavy kernel-space synchronization.

* **Lock-Free Ring Buffers (`src/concurrency/ring_buffer.c`):** Data ingestion utilizes circular buffers backed by atomic read/write head pointers. Memory ordering relies on `memory_order_acquire` and `memory_order_release` semantics to prevent instruction reordering while maximizing CPU pipeline efficiency.
* **Lock Contention Prevention:** Hot-path execution paths avoid mutexes entirely, relying instead on compare-and-swap (`atomic_compare_exchange_weak`) operations. Contended operations gracefully yield CPU time slices or spin adaptively depending on workload profiles.
* **Memory Ownership & Arenas (`src/memory/arena.c`):** All persistent state memory is pre-allocated at startup inside contiguous block arenas. Individual worker threads execute within isolated allocation contexts, completely eradicating heap fragmentation and avoiding system call overhead (`brk`/`mmap`) during request processing.
* **Thread Safety:** Engines are architected as share-nothing or partition-parallel structures. State modifications across core boundaries pass strictly through immutable value copies or atomic state registers.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation / Path | Time Complexity | Space Complexity | Estimated Latency (p99) |
| :--- | :--- | :--- | :--- |
| **Ring Buffer Push/Pop** | $\mathcal{O}(1)$ | $\mathcal{O}(N)$ fixed | $< 45\text{ ns}$ |
| **Bucket State Evaluation** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ localized | $< 80\text{ ns}$ |
| **Clock Skew Adjustment** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | $< 30\text{ ns}$ |
| **Arena Allocation** | $\mathcal{O}(1)$ | $\mathcal{O}(B)$ block-based | $< 12\text{ ns}$ |
| **Full Invariant Validation** | $\mathcal{O}(V)$ | $\mathcal{O}(1)$ | $< 1.2\ \mu\text{s}$ |

---

## Security Architecture & Threat Model

`cluster-gate` is engineered with an explicit threat model assuming hostile, high-frequency inputs and potential memory corruption vectors.

* **Input Boundaries & Sanitization:** All inbound payloads entering via the ring buffer undergo rigorous bounds checking against predefined fixed-width structures (`include/cluster_gate/types.h`). Malformed headers or out-of-range timestamps are dropped instantly at the ingress gate.
* **Memory Corruption Protections:** Buffer overflows are mitigated by design through fixed-size layout allocations inside pre-allocated arenas, avoiding dynamic string formatting (`sprintf`/`strcpy`) on the hot path. 
* **Integer Overflow Defense:** Arithmetic operations tracking window counters and clock skews implement explicit wrapping guards and saturating arithmetic checks.
* **Compiler-Enforced Hardening:** The build system enforces strict warning flags, stack protector checks, and pointer validation sanitizers (`-fstack-protector-strong`, `-D_FORTIFY_SOURCE=2`).

---

## Build, Verification & Benchmarking Recipes

To compile the repository, execute the comprehensive test suites, and run performance benchmarks, use the following production recipes.

### Prerequisites
* A C11-compliant compiler (`gcc` >= 9.0 or `clang` >= 10.0)
* GNU Make
* POSIX-compliant environment (Linux / macOS)

### 1. Compilation & Build
Compile the core library, test runners, and benchmark harnesses in release mode with maximum optimizations:
```bash
make clean && make release
```

### 2. Verification & Invariant Testing
Run the comprehensive test suite to validate concurrency safety, memory bounds, and logical correctness:
```bash
make test
```
*To run tests under AddressSanitizer (ASan) and UndefinedBehaviorSanitizer (UBSan):*
```bash
make test-sanitized
```

### 3. Benchmarking Execution
Execute the standalone benchmark engine to profile hot-path performance, throughput limits, and tail latencies:
```bash
make benchmark