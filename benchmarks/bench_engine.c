#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <sched.h>
#include <immintrin.h>

#define WARMUP_ITERATIONS 1000000
#define BENCHMARK_ITERATIONS 10000000
#define PERCENTILE_SAMPLES BENCHMARK_ITERATIONS

/* 
 * Simulated 'cluster-gate' core structure.
 * Represents a synchronization/routing gate in a clustering fabric.
 */
typedef struct {
    volatile uint64_t state;
    volatile uint64_t counter;
    uint8_t padding[112]; /* Prevent false sharing / cache line ping-pong */
} cluster_gate_t;

static cluster_gate_t gate;
static uint64_t *latencies = NULL;

static inline uint64_t rdtsc(void) {
    unsigned int lo, hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    if (fa < fb) return -1;
    if (fa > fb) return 1;
    return 0;
}

static inline void cluster_gate_enter(cluster_gate_t *g) {
    /* Simulate atomic gate check-in and barrier traversal */
    while (__atomic_fetch_add(&g->state, 1, __ATOMIC_ACQUIRE) & 1) {
        __atomic_fetch_sub(&g->state, 1, __ATOMIC_RELEASE);
        _mm_pause();
    }
}

static inline void cluster_gate_exit(cluster_gate_t *g) {
    __atomic_fetch_add(&g->counter, 1, __ATOMIC_RELEASE);
    __atomic_fetch_sub(&g->state, 1, __ATOMIC_RELEASE);
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    memset(&gate, 0, sizeof(cluster_gate_t));

    latencies = (uint64_t *)malloc(PERCENTILE_SAMPLES * sizeof(uint64_t));
    if (!latencies) {
        perror("Failed to allocate latency tracking buffer");
        return 1;
    }

    /* Pin to core 0 for deterministic measurement */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    /* Warmup phase */
    for (uint64_t i = 0; i < WARMUP_ITERATIONS; i++) {
        cluster_gate_enter(&gate);
        cluster_gate_exit(&gate);
    }

    /* Reset counter for actual benchmark */
    gate.counter = 0;

    /* Benchmark execution with per-op cycle sampling */
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (uint64_t i = 0; i < BENCHMARK_ITERATIONS; i++) {
        uint64_t start_cycle = rdtsc();
        
        cluster_gate_enter(&gate);
        cluster_gate_exit(&gate);
        
        uint64_t end_cycle = rdtsc();
        latencies[i] = (end_cycle >= start_cycle) ? (end_cycle - start_cycle) : 0;
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    double elapsed_sec = (end_time.tv_sec - start_time.tv_sec) +
                         (end_time.tv_nsec - start_time.tv_nsec) / 1e9;

    double ops_sec = (double)BENCHMARK_ITERATIONS / elapsed_sec;

    /* Sort latencies for percentile extraction */
    qsort(latencies, PERCENTILE_SAMPLES, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(PERCENTILE_SAMPLES * 0.50)];
    uint64_t p90 = latencies[(size_t)(PERCENTILE_SAMPLES * 0.90)];
    uint64_t p99 = latencies[(size_t)(PERCENTILE_SAMPLES * 0.99)];
    uint64_t p999 = latencies[(size_t)(PERCENTILE_SAMPLES * 0.999)];

    /* Output formatted tabular metrics */
    printf("\n");
    printf("+------------------------------------------------------------+\n");
    printf("|         CLUSTER-GATE THROUGHPUT & LATENCY BENCHMARK        |\n");
    printf("+------------------------------+-----------------------------+\n");
    printf("| Metric                       | Value                       |\n");
    printf("+------------------------------+-----------------------------+\n");
    printf("| Total Iterations             | %-27lu |\n", (unsigned long)BENCHMARK_ITERATIONS);
    printf("| Elapsed Time (s)             | %-27.6f |\n", elapsed_sec);
    printf("| Throughput (ops/sec)         | %-27.2f |\n", ops_sec);
    printf("+------------------------------+-----------------------------+\n");
    printf("| Latency p50 (cycles)         | %-27lu |\n", (unsigned long)p50);
    printf("| Latency p90 (cycles)         | %-27lu |\n", (unsigned long)p90);
    printf("| Latency p99 (cycles)         | %-27lu |\n", (unsigned long)p99);
    printf("| Latency p99.9 (cycles)       | %-27lu |\n", (unsigned long)p999);
    printf("+------------------------------+-----------------------------+\n");
    printf("\n");

    free(latencies);
    return 0;
}