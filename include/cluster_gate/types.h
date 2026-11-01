#ifndef CLUSTER_GATE_TYPES_H
#define CLUSTER_GATE_TYPES_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>

#define CG_CACHE_LINE_SIZE 64
#define CG_ALIGNAS(x) __attribute__((aligned(x)))
#define CG_PACKED __attribute__((__packed__))

#define CG_MAX_NODES 64
#define CG_SKEW_WINDOW_SIZE 16
#define CG_DEFAULT_BUCKET_CAPACITY 1000000UL
#define CG_MAX_SKEW_THRESHOLD_MS 50UL

typedef enum {
    CG_OK = 0,
    CG_ERR_INVALID_ARG = -1,
    CG_ERR_NO_MEMORY = -2,
    CG_ERR_BUCKET_FULL = -3,
    CG_ERR_RATE_LIMITED = -4,
    CG_ERR_PARTITION = -5,
    CG_ERR_CLOCK_SKEW = -6,
    CG_ERR_TIMEOUT = -7,
    CG_ERR_CLOSED = -8
} cg_status_t;

typedef enum {
    CG_MODE_NORMAL = 0,
    CG_MODE_LOCAL_QUOTA = 1,
    CG_MODE_LOGICAL_CLOCKS = 2,
    CG_MODE_FAIL_OPEN = 3,
    CG_MODE_FAIL_CLOSED = 4
} cg_resiliency_mode_t;

typedef struct CG_ALIGNAS(CG_CACHE_LINE_SIZE) {
    uint64_t packed_state;
    uint32_t capacity;
    uint32_t refill_rate;
    uint32_t node_id;
    uint32_t padding_align;
} cg_bucket_t;

typedef struct CG_ALIGNAS(CG_CACHE_LINE_SIZE) {
    int64_t offsets[CG_SKEW_WINDOW_SIZE];
    uint32_t head;
    uint32_t count;
    uint64_t last_sync_ns;
    bool logical_mode_active;
    char pad[CG_CACHE_LINE_SIZE - sizeof(int64_t) * CG_SKEW_WINDOW_SIZE - sizeof(uint32_t) * 2 - sizeof(uint64_t) - sizeof(bool)];
} cg_skew_tracker_t;

typedef struct CG_ALIGNAS(CG_CACHE_LINE_SIZE) {
    uint32_t head CG_ALIGNAS(CG_CACHE_LINE_SIZE);
    uint32_t tail CG_ALIGNAS(CG_CACHE_LINE_SIZE);
    uint32_t capacity;
    uint32_t mask;
    size_t element_size;
    void *buffer;
} cg_ring_buffer_t;

typedef struct CG_ALIGNAS(CG_CACHE_LINE_SIZE) {
    void *base;
    void *current;
    size_t size;
    size_t used;
    int huge_fd;
} cg_arena_t;

typedef struct CG_ALIGNAS(CG_CACHE_LINE_SIZE) {
    uint64_t epoch;
    uint32_t node_id;
    uint32_t version;
    bool active;
} cg_hazard_node_t;

typedef struct CG_ALIGNAS(CG_CACHE_LINE_SIZE) {
    cg_arena_t arena;
    cg_bucket_t global_bucket;
    cg_skew_tracker_t skew_tracker;
    cg_resiliency_mode_t current_mode;
    uint32_t node_id;
    uint32_t cluster_size;
    bool initialized;
} cg_cluster_gate_t;

static inline uint64_t cg_pack_state(uint32_t tokens, uint32_t epoch) {
    return ((uint64_t)tokens << 32) | (uint64_t)epoch;
}

static inline void cg_unpack_state(uint64_t packed, uint32_t *tokens, uint32_t *epoch) {
    if (tokens) *tokens = (uint32_t)(packed >> 32);
    if (epoch) *epoch = (uint32_t)(packed & 0xFFFFFFFFULL);
}

static inline bool cg_bucket_try_consume(cg_bucket_t *bucket, uint32_t tokens_requested, uint32_t current_epoch) {
    uint64_t current = __atomic_load_n(&bucket->packed_state, __ATOMIC_ACQUIRE);
    uint32_t tokens;
    uint32_t epoch;
    
    while (true) {
        cg_unpack_state(current, &tokens, &epoch);
        
        if (tokens < tokens_requested) {
            return false;
        }
        
        uint32_t new_tokens = tokens - tokens_requested;
        uint64_t next = cg_pack_state(new_tokens, current_epoch);
        
        if (__atomic_compare_exchange_n(&bucket->packed_state, &current, next,
                                        true, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            return true;
        }
    }
}

static inline void cg_skew_tracker_init(cg_skew_tracker_t *tracker) {
    for (size_t i = 0; i < CG_SKEW_WINDOW_SIZE; i++) {
        tracker->offsets[i] = 0;
    }
    tracker->head = 0;
    tracker->count = 0;
    tracker->last_sync_ns = 0;
    tracker->logical_mode_active = false;
}

static inline void cg_skew_tracker_add(cg_skew_tracker_t *tracker, int64_t offset_ns) {
    uint32_t idx = __atomic_load_n(&tracker->head, __ATOMIC_RELAXED);
    tracker->offsets[idx] = offset_ns;
    uint32_t next_idx = (idx + 1) % CG_SKEW_WINDOW_SIZE;
    __atomic_store_n(&tracker->head, next_idx, __ATOMIC_RELEASE);
    
    uint32_t current_count = __atomic_load_n(&tracker->count, __ATOMIC_ACQUIRE);
    if (current_count < CG_SKEW_WINDOW_SIZE) {
        __atomic_add_fetch(&tracker->count, 1, __ATOMIC_ACQ_REL);
    }
    
    if (offset_ns > (int64_t)(CG_MAX_SKEW_THRESHOLD_MS * 1000000ULL) ||
        offset_ns < -(int64_t)(CG_MAX_SKEW_THRESHOLD_MS * 1000000ULL)) {
        __atomic_store_n(&tracker->logical_mode_active, true, __ATOMIC_RELEASE);
    }
}

#endif /* CLUSTER_GATE_TYPES_H */