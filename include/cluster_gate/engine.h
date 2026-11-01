#ifndef CLUSTER_GATE_ENGINE_H
#define CLUSTER_GATE_ENGINE_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <time.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#define CG_CACHE_LINE 64
#define CG_MAX_NODES 64
#define CG_SKEW_WINDOW 32
#define CG_MAX_PEERS 16
#define CG_DEFAULT_CAPACITY 1000000ULL

typedef enum {
    CG_OK = 0,
    CG_ERR_NOMEM = -1,
    CG_ERR_INVAL = -2,
    CG_ERR_TIMEOUT = -3,
    CG_ERR_BUSY = -4,
    CG_ERR_SPLIT_BRAIN = -5,
    CG_ERR_SHUTDOWN = -6
} cg_status_t;

typedef enum {
    CG_MODE_NORMAL = 0,
    CG_MODE_LOCAL_FALLBACK = 1,
    CG_MODE_LOGICAL_CLOCKS = 2,
    CG_MODE_FAIL_CLOSED = 3
} cg_resiliency_mode_t;

typedef struct {
    uint8_t *buffer;
    size_t capacity;
    size_t offset;
    pthread_mutex_t lock;
} cg_arena_t;

typedef struct {
    uint64_t offsets[CG_SKEW_WINDOW];
    uint32_t head;
    uint32_t count;
    int64_t current_skew_ns;
    pthread_spinlock_t lock;
} __attribute__((alignas(CG_CACHE_LINE))) cg_skew_tracker_t;

typedef struct {
    uint64_t packed_state; /* High 32 bits: tokens, Low 32 bits: epoch timestamp */
    uint32_t capacity;
    uint32_t refill_rate;
    uint32_t padding_guard[8];
} __attribute__((alignas(CG_CACHE_LINE))) cg_bucket_t;

typedef struct {
    uint64_t sequence;
    void *item;
} cg_ring_element_t;

typedef struct {
    char pad0[CG_CACHE_LINE];
    uint64_t head;
    char pad1[CG_CACHE_LINE];
    uint64_t tail;
    char pad2[CG_CACHE_LINE];
    uint64_t capacity_mask;
    cg_ring_element_t *elements;
    char pad3[CG_CACHE_LINE];
} __attribute__((alignas(CG_CACHE_LINE))) cg_ring_buffer_t;

typedef struct {
    uint32_t node_id;
    struct sockaddr_in addr;
    int socket_fd;
    bool active;
    uint64_t last_heartbeat_ns;
} cg_peer_t;

typedef struct {
    uint32_t node_id;
    cg_arena_t arena;
    cg_bucket_t global_bucket;
    cg_skew_tracker_t skew_tracker;
    cg_ring_buffer_t event_ring;
    cg_peer_t peers[CG_MAX_PEERS];
    uint32_t peer_count;
    _Atomic cg_resiliency_mode_t resiliency_mode;
    _Atomic bool running;
    pthread_t worker_thread;
    int epoll_fd;
    uint64_t max_skew_threshold_ns;
} cg_engine_t;

static inline uint64_t cg_monotonic_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline cg_status_t cg_arena_init(cg_arena_t *arena, size_t size) {
    if (!arena) return CG_ERR_INVAL;
    size_t page_size = sysconf(_SC_PAGESIZE);
    size_t aligned_size = (size + page_size - 1) & ~(page_size - 1);
    
    arena->buffer = mmap(NULL, aligned_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (arena->buffer == MAP_FAILED) {
        return CG_ERR_NOMEM;
    }
#ifdef MADV_HUGEPAGE
    madvise(arena->buffer, aligned_size, MADV_HUGEPAGE);
#endif
    arena->capacity = aligned_size;
    arena->offset = 0;
    pthread_mutex_init(&arena->lock, NULL);
    return CG_OK;
}

static inline void *cg_arena_alloc(cg_arena_t *arena, size_t size) {
    if (!arena) return NULL;
    size_t aligned_size = (size + 7) & ~7ULL;
    pthread_mutex_lock(&arena->lock);
    if (arena->offset + aligned_size > arena->capacity) {
        pthread_mutex_unlock(&arena->lock);
        return NULL;
    }
    void *ptr = arena->buffer + arena->offset;
    arena->offset += aligned_size;
    pthread_mutex_unlock(&arena->lock);
    return ptr;
}

static inline void cg_arena_destroy(cg_arena_t *arena) {
    if (!arena || !arena->buffer) return;
    munmap(arena->buffer, arena->capacity);
    pthread_mutex_destroy(&arena->lock);
}

static inline void cg_skew_tracker_init(cg_skew_tracker_t *tracker) {
    if (!tracker) return;
    memset(tracker, 0, sizeof(cg_skew_tracker_t));
    pthread_spin_init(&tracker->lock, PTHREAD_PROCESS_PRIVATE);
}

static inline void cg_skew_tracker_add(cg_skew_tracker_t *tracker, int64_t offset_ns) {
    if (!tracker) return;
    pthread_spin_lock(&tracker->lock);
    tracker->offsets[tracker->head] = (uint64_t)offset_ns;
    tracker->head = (tracker->head + 1) % CG_SKEW_WINDOW;
    if (tracker->count < CG_SKEW_WINDOW) {
        tracker->count++;
    }
    
    uint64_t temp[CG_SKEW_WINDOW];
    for (uint32_t i = 0; i < tracker->count; i++) {
        temp[i] = tracker->offsets[i];
    }
    for (uint32_t i = 1; i < tracker->count; i++) {
        uint64_t key = temp[i];
        int j = (int)i - 1;
        while (j >= 0 && (int64_t)temp[j] > (int64_t)key) {
            temp[j + 1] = temp[j];
            j--;
        }
        temp[j + 1] = key;
    }
    if (tracker->count > 0) {
        tracker->current_skew_ns = (int64_t)temp[tracker->count / 2];
    }
    pthread_spin_unlock(&tracker->lock);
}

static inline cg_status_t cg_ring_init(cg_ring_buffer_t *ring, size_t capacity, cg_arena_t *arena) {
    if (!ring || !arena) return CG_ERR_INVAL;
    size_t cap = 1;
    while (cap < capacity) cap <<= 1;
    
    ring->elements = cg_arena_alloc(arena, cap * sizeof(cg_ring_element_t));
    if (!ring->elements) return CG_ERR_NOMEM;
    
    ring->capacity_mask = cap - 1;
    ring->head = 0;
    ring->tail = 0;
    for (size_t i = 0; i < cap; i++) {
        ring->elements[i].sequence = i;
        ring->elements[i].item = NULL;
    }
    return CG_OK;
}

static inline bool cg_ring_push(cg_ring_buffer_t *ring, void *item) {
    cg_ring_element_t *element;
    uint64_t pos = __atomic_load_n(&ring->tail, __ATOMIC_RELAXED);
    for (;;) {
        element = &ring->elements[pos & ring->capacity_mask];
        uint64_t seq = __atomic_load_n(&element->sequence, __ATOMIC_ACQUIRE);
        int64_t diff = (int64_t)seq - (int64_t)pos;
        if (diff == 0) {
            if (__atomic_compare_exchange_n(&ring->tail, &pos, pos + 1, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                break;
            }
        } else if (diff < 0) {
            return false;
        } else {
            pos = __atomic_load_n(&ring->tail, __ATOMIC_RELAXED);
        }
    }
    element->item = item;
    __atomic_store_n(&element->sequence, pos + 1, __ATOMIC_RELEASE);
    return true;
}

static inline bool cg_ring_pop(cg_ring_buffer_t *ring, void **item) {
    cg_ring_element_t *element;
    uint64_t pos = __atomic_load_n(&ring->head, __ATOMIC_RELAXED);
    for (;;) {
        element = &ring->elements[pos & ring->capacity_mask];
        uint64_t seq = __atomic_load_n(&element->sequence, __ATOMIC_ACQUIRE);
        int64_t diff = (int64_t)seq - (int64_t)(pos + 1);
        if (diff == 0) {
            if (__atomic_compare_exchange_n(&ring->head, &pos, pos + 1, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                break;
            }
        } else if (diff < 0) {
            return false;
        } else {
            pos = __atomic_load_n(&ring->head, __ATOMIC_RELAXED);
        }
    }
    *item = element->item;
    __atomic_store_n(&element->sequence, pos + ring->capacity_mask + 1, __ATOMIC_RELEASE);
    return true;
}

static inline void cg_bucket_init(cg_bucket_t *bucket, uint32_t capacity, uint32_t refill_rate) {
    if (!bucket) return;
    uint64_t now = cg_monotonic_time_ns();
    uint64_t packed = ((uint64_t)capacity << 32) | (now & 0xFFFFFFFFULL);
    __atomic_store_n(&bucket->packed_state, packed, __ATOMIC_SEQ_CST);
    bucket->capacity = capacity;
    bucket->refill_rate = refill_rate;
}

static inline bool cg_bucket_consume(cg_bucket_t *bucket, uint32_t tokens, cg_engine_t *engine) {
    if (!bucket || !engine) return false;
    uint64_t current = __atomic_load_n(&bucket->packed_state, __ATOMIC_ACQUIRE);
    
    for (;;) {
        uint32_t current_tokens = (uint32_t)(current >> 32);
        uint32_t last_epoch = (uint32_t)(current & 0xFFFFFFFFULL);
        uint64_t now_full = cg_monotonic_time_ns();
        
        int64_t skew = 0;
        if (__atomic_load_n(&engine->resiliency_mode, __ATOMIC_ACQUIRE) == CG_MODE_NORMAL) {
            pthread_spin_lock(&engine->skew_tracker.lock);
            skew = engine->skew_tracker.current_skew_ns;
            pthread_spin_unlock(&engine->skew_tracker.lock);
            
            if (engine->max_skew_threshold_ns > 0 && (uint64_t)(skew < 0 ? -skew : skew) > engine->max_skew_threshold_ns) {
                __atomic_store_n(&engine->resiliency_mode, CG_MODE_LOGICAL_CLOCKS, __ATOMIC_RELEASE);
            }
        }

        uint32_t now_epoch = (uint32_t)(now_full & 0xFFFFFFFFULL);
        uint64_t elapsed_ns = (now_epoch >= last_epoch) ? (uint32_t)(now_epoch - last_epoch) : (0xFFFFFFFFULL - last_epoch + now_epoch + 1);
        
        uint32_t generated = (uint32_t)((elapsed_ns * bucket->refill_rate) / 1000000000ULL);
        uint32_t new_tokens = current_tokens + generated;
        if (new_tokens > bucket->capacity) {
            new_tokens = bucket->capacity;
        }

        if (new_tokens < tokens) {
            cg_resiliency_mode_t mode = __atomic_load_n(&engine->resiliency_mode, __ATOMIC_ACQUIRE);
            if (mode == CG_MODE_LOCAL_FALLBACK || mode == CG_MODE_LOGICAL_CLOCKS) {
                if (current_tokens >= tokens) {
                    new_tokens = current_tokens - tokens;
                    uint64_t next = ((uint64_t)new_tokens << 32) | (now_epoch & 0xFFFFFFFFULL);
                    if (__atomic_compare_exchange_n(&bucket->packed_state, &current, next, true, __ATOMIC_SEQ_CST, __ATOMIC_ACQUIRE)) {
                        return true;
                    }
                    continue;
                }
            }
            return false;
        }

        new_tokens -= tokens;
        uint64_t next = ((uint64_t)new_tokens << 32) | (now_epoch & 0xFFFFFFFFULL);
        if (__atomic_compare_exchange_n(&bucket->packed_state, &current, next, true, __ATOMIC_SEQ_CST, __ATOMIC_ACQUIRE)) {
            return true;
        }
    }
}

cg_status_t cg_engine_init(cg_engine_t *engine, uint32_t node_id, size_t arena_size) {
    if (!engine) return CG_ERR_INVAL;
    memset(engine, 0, sizeof(cg_engine_t));
    engine->node_id = node_id;
    engine->max_skew_threshold_ns = 50000000ULL; /* 50ms default */
    
    cg_status_t status = cg_arena_init(&engine->arena, arena_size);
    if (status != CG_OK) return status;
    
    cg_bucket_init(&engine->global_bucket, CG_DEFAULT_CAPACITY, CG_DEFAULT_CAPACITY / 10);
    cg_skew_tracker_init(&engine->skew_tracker);
    
    status = cg_ring_init(&engine->event_ring, 4096, &engine->arena);
    if (status != CG_OK) {
        cg_arena_destroy(&engine->arena);
        return status;
    }
    
    __atomic_store_n(&engine->resiliency_mode, CG_MODE_NORMAL, __ATOMIC_RELEASE);
    __atomic_store_n(&engine->running, true, __ATOMIC_RELEASE);
    return CG_OK;
}

void cg_engine_destroy(cg_engine_t *engine) {
    if (!engine) return;
    __atomic_