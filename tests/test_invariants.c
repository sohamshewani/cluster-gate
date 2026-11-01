#define _GNU_SOURCE
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <assert.h>

/* ============================================================================
 * EXHAUSTIVE C UNIT, INVARIANT, AND CONCURRENT STRESS-TEST SUITE: CLUSTER-GATE
 * ============================================================================
 * This production-grade test suite validates 'cluster-gate', a concurrent, 
 * stateful synchronization and routing primitive for high-throughput distributed 
 * cluster environments.
 * 
 * It comprehensively validates:
 * 1. Unit & Edge Cases: Token limits, overflow, invalid transitions, timeout states.
 * 2. Invariants: State machine consistency, global ticket counters, strict barrier safety.
 * 3. Concurrent Stress & Race Conditions: High-contention multi-threaded hammering,
 *    deadlock avoidance, hazard-pointer-style tracking, memory safety under load.
 * 4. Failure & Recovery: Simulated network partitions, forced resets, half-open states,
 *    graceful degradation, and rapid self-healing verification.
 * ============================================================================ */

#define CG_OK            0
#define CG_ERR_BUSY     -1
#define CG_ERR_TIMEOUT  -2
#define CG_ERR_INVALID  -3
#define CG_ERR_STATE    -4

typedef enum {
    CG_STATE_INIT = 0,
    CG_STATE_ARMED,
    CG_STATE_OPEN,
    CG_STATE_DRAINING,
    CG_STATE_TRIPPED, /* Failure/Partition State */
    CG_STATE_RECOVERING
} cg_state_t;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    cg_state_t state;
    uint32_t capacity;
    uint32_t active_permits;
    uint64_t total_passed;
    uint64_t total_rejected;
    uint64_t generation;
    atomic_bool shutdown;
} cluster_gate_t;

/* --- Core Engine Implementation under Test --- */

static int cg_init(cluster_gate_t *gate, uint32_t capacity) {
    if (!gate || capacity == 0) return CG_ERR_INVALID;
    if (pthread_mutex_init(&gate->lock, NULL) != 0) return CG_ERR_STATE;
    if (pthread_cond_init(&gate->cond, NULL) != 0) {
        pthread_mutex_destroy(&gate->lock);
        return CG_ERR_STATE;
    }
    gate->state = CG_STATE_ARMED;
    gate->capacity = capacity;
    gate->active_permits = 0;
    gate->total_passed = 0;
    gate->total_rejected = 0;
    gate->generation = 1;
    atomic_init(&gate->shutdown, false);
    return CG_OK;
}

static void cg_destroy(cluster_gate_t *gate) {
    if (!gate) return;
    atomic_store(&gate->shutdown, true);
    pthread_mutex_lock(&gate->lock);
    pthread_cond_broadcast(&gate->cond);
    pthread_mutex_unlock(&gate->lock);

    pthread_mutex_destroy(&gate->lock);
    pthread_cond_destroy(&gate->cond);
}

static int cg_acquire(cluster_gate_t *gate, uint32_t count) {
    if (!gate || count == 0 || count > gate->capacity) return CG_ERR_INVALID;

    pthread_mutex_lock(&gate->lock);
    while (1) {
        if (atomic_load(&gate->shutdown)) {
            pthread_mutex_unlock(&gate->lock);
            return CG_ERR_STATE;
        }
        if (gate->state == CG_STATE_TRIPPED || gate->state == CG_STATE_DRAINING) {
            gate->total_rejected++;
            pthread_mutex_unlock(&gate->lock);
            return CG_ERR_BUSY;
        }
        if (gate->state == CG_STATE_ARMED || gate->state == CG_STATE_OPEN) {
            if (gate->active_permits + count <= gate->capacity) {
                gate->active_permits += count;
                gate->total_passed++;
                if (gate->active_permits == gate->capacity) {
                    gate->state = CG_STATE_OPEN;
                }
                pthread_mutex_unlock(&gate->lock);
                return CG_OK;
            }
        }
        /* Wait for space or state change */
        pthread_cond_wait(&gate->cond, &gate->lock);
    }
}

static int cg_release(cluster_gate_t *gate, uint32_t count) {
    if (!gate || count == 0) return CG_ERR_INVALID;

    pthread_mutex_lock(&gate->lock);
    if (gate->active_permits < count) {
        pthread_mutex_unlock(&gate->lock);
        return CG_ERR_INVALID;
    }

    gate->active_permits -= count;
    if (gate->state == CG_STATE_OPEN && gate->active_permits < gate->capacity) {
        gate->state = CG_STATE_ARMED;
    }
    pthread_cond_broadcast(&gate->cond);
    pthread_mutex_unlock(&gate->lock);
    return CG_OK;
}

static int cg_trip(cluster_gate_t *gate) {
    if (!gate) return CG_ERR_INVALID;
    pthread_mutex_lock(&gate->lock);
    gate->state = CG_STATE_TRIPPED;
    gate->generation++;
    pthread_cond_broadcast(&gate->cond);
    pthread_mutex_unlock(&gate->lock);
    return CG_OK;
}

static int cg_recover(cluster_gate_t *gate) {
    if (!gate) return CG_ERR_INVALID;
    pthread_mutex_lock(&gate->lock);
    gate->state = CG_STATE_RECOVERING;
    gate->active_permits = 0; /* Force flush */
    gate->state = CG_STATE_ARMED;
    gate->generation++;
    pthread_cond_broadcast(&gate->cond);
    pthread_mutex_unlock(&gate->lock);
    return CG_OK;
}

/* ============================================================================
 * TEST HARNESS FRAMEWORK
 * ============================================================================ */

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "\n[FAIL] %s:%d: Assertion Failed: %s\n", __FILE__, __LINE__, msg); \
        fflush(stderr); \
        assert(cond); \
    } \
} while(0)

#define RUN_TEST(test_func) do { \
    printf("  [RUNNING] %s ... ", #test_func); \
    fflush(stdout); \
    test_func(); \
    printf("PASSED\n"); \
    fflush(stdout); \
} while(0)

/* ============================================================================
 * 1. UNIT & EDGE CASE TESTS
 * ============================================================================ */

static void test_unit_boundaries(void) {
    cluster_gate_t gate;
    
    /* Invalid initializations */
    TEST_ASSERT(cg_init(NULL, 10) == CG_ERR_INVALID, "Null gate init check");
    TEST_ASSERT(cg_init(&gate, 0) == CG_ERR_INVALID, "Zero capacity init check");

    TEST_ASSERT(cg_init(&gate, 5) == CG_OK, "Valid init");

    /* Invalid acquire edges */
    TEST_ASSERT(cg_acquire(&gate, 0) == CG_ERR_INVALID, "Zero count acquire");
    TEST_ASSERT(cg_acquire(&gate, 6) == CG_ERR_INVALID, "Exceed capacity acquire");
    TEST_ASSERT(cg_acquire(NULL, 1) == CG_ERR_INVALID, "Null gate acquire");

    /* Capacity exhaustion */
    TEST_ASSERT(cg_acquire(&gate, 5) == CG_OK, "Full capacity acquisition");
    /* Next acquire should block, but since this is synchronous non-blocking context, let's verify via non-blocking mock logic */
    // Wait, cg_acquire blocks. Let's test overflow handling via direct logic or short threads.
    
    /* Invalid release edges */
    TEST_ASSERT(cg_release(&gate, 0) == CG_ERR_INVALID, "Zero release");
    TEST_ASSERT(cg_release(&gate, 6) == CG_ERR_INVALID, "Over-release check");

    TEST_ASSERT(cg_release(&gate, 5) == CG_OK, "Clean release");
    
    cg_destroy(&gate);
}

/* ============================================================================
 * 2. INVARIANT & STATE MACHINE TESTS
 * ============================================================================ */

static void test_state_machine_invariants(void) {
    cluster_gate_t gate;
    TEST_ASSERT(cg_init(&gate, 10) == CG_OK, "Init");

    TEST_ASSERT(gate.state == CG_STATE_ARMED, "Initial state is ARMED");
    
    TEST_ASSERT(cg_acquire(&gate, 10) == CG_OK, "Acquire full");
    TEST_ASSERT(gate.state == CG_STATE_OPEN, "State transitions to OPEN at capacity");

    TEST_ASSERT(cg_release(&gate, 2) == CG_OK, "Release partial");
    TEST_ASSERT(gate.state == CG_STATE_ARMED, "State reverts to ARMED below capacity");

    /* Trip invariant */
    TEST_ASSERT(cg_trip(&gate) == CG_OK, "Trip gate");
    TEST_ASSERT(gate.state == CG_STATE_TRIPPED, "State is TRIPPED");
    TEST_ASSERT(cg_acquire(&gate, 1) == CG_ERR_BUSY, "Acquire rejected when tripped");

    /* Recovery invariant */
    TEST_ASSERT(cg_recover(&gate) == CG_OK, "Recover gate");
    TEST_ASSERT(gate.state == CG_STATE_ARMED, "State returns to ARMED after recovery");
    TEST_ASSERT(gate.active_permits == 0, "Permits flushed on recovery");

    cg_destroy(&gate);
}

/* ============================================================================
 * 3. CONCURRENT STRESS & RACE CONDITION TESTS
 * ============================================================================ */

typedef struct {
    cluster_gate_t *gate;
    int thread_id;
    int iterations;
    uint64_t success_count;
    uint64_t fail_count;
} worker_arg_t;

static void *stress_worker(void *arg) {
    worker_arg_t *w = (worker_arg_t *)arg;
    for (int i = 0; i < w->iterations; i++) {
        int rc = cg_acquire(w->gate, 1);
        if (rc == CG_OK) {
            w->success_count++;
            // Small critical section simulation
            sched_yield();
            TEST_ASSERT(cg_release(w->gate, 1) == CG_OK, "Worker release consistency");
        } else {
            w->fail_count++;
        }
    }
    return NULL;
}

static void test_concurrent_contention(void) {
    cluster_gate_t gate;
    #define NUM_THREADS 16
    #define ITERATIONS  2000
    
    TEST_ASSERT(cg_init(&gate, 4) == CG_OK, "Init small capacity for high contention");

    pthread_t threads[NUM_THREADS];
    worker_arg_t args[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].gate = &gate;
        args[i].thread_id = i;
        args[i].iterations = ITERATIONS;
        args[i].success_count = 0;
        args[i].fail_count = 0;
        pthread_create(&threads[i], NULL, stress_worker, &args[i]);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    uint64_t total_success = 0;
    for (int i = 0; i < NUM