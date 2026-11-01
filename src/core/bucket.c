#include "core/bucket.h"
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define CG_CACHE_LINE_SIZE 64
#define CG_MAX_SKEW_THRESHOLD_NS 50000000ULL /* 50ms */

/* Arena implementation helpers */
struct cg_arena {
    uint8_t *base;
    size_t capacity;
    _Atomic size_t offset;
    int use_huge_pages;
};

cg_arena_t *cg_arena_create(size_t capacity, int use_huge_pages) {
    size_t page_size = 4096;
    if (use_huge_pages) {
        page_size = 2 * 1024 * 1024; /* 2MB huge page */
    }
    
    /* Align capacity to page size */
    size_t aligned_capacity = (capacity + page_size - 1) & ~(page_size - 1);
    
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
    void *ptr = mmap(NULL, aligned_capacity, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (ptr == MAP_FAILED) {
        return NULL;
    }
    
    if (use_huge_pages) {
        madvise(ptr, aligned_capacity, MADV_HUGEPAGE);
    }
    
    cg_arena_t *arena = (cg_arena_t *)ptr;
    arena->base = (uint8_t *)ptr;
    arena->capacity = aligned_capacity;
    atomic_init(&arena->offset, sizeof(cg_arena_t));
    arena->use_huge_pages = use_huge_pages;
    
    return arena;
}

void cg_arena_destroy(cg_arena_t *arena) {
    if (!arena) {
        return;
    }
    munmap(arena, arena->capacity);
}

void *cg_arena_alloc(cg_arena_t *arena, size_t size, size_t alignment) {
    size_t current_offset, new_offset;
    void *ptr = NULL;
    
    do {
        current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
        size_t padding = (alignment - (current_offset % alignment)) % alignment;
        new_offset = current_offset + padding + size;
        
        if (new_offset > arena->capacity) {
            return NULL; /* Out of memory in arena */
        }
        
        if (atomic_compare_exchange_weak_explicit(&arena->offset,
                                                  &current_offset,
                                                  new_offset,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
            ptr = (void *)(arena->base + current_offset + padding);
            break;
        }
    } while (1);
    
    return ptr;
}

void cg_arena_reset(cg_arena_t *arena) {
    if (!arena) {
        return;
    }
    atomic_store_explicit(&arena->offset, sizeof(cg_arena_t), memory_order_release);
}

/* Monotonic time helper with skew correction */
static uint64_t cg_get_monolithic_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Skew tracker operations */
void cg_skew_tracker_init(cg_skew_tracker_t *tracker, uint64_t max_skew_ns) {
    tracker->head = 0;
    tracker->max_skew_ns = max_skew_ns ? max_skew_ns : CG_MAX_SKEW_THRESHOLD_NS;
    for (size_t i = 0; i < CG_SKEW_WINDOW_SIZE; i++) {
        atomic_init(&tracker->offsets[i], 0);
    }
}

void cg_skew_tracker_update(cg_skew_tracker_t *tracker, int64_t offset_ns) {
    uint32_t head = atomic_fetch_add_explicit(&tracker->head, 1, memory_order_relaxed);
    size_t idx = head % CG_SKEW_WINDOW_SIZE;
    atomic_store_explicit(&tracker->offsets[idx], offset_ns, memory_order_release);
}

int64_t cg_skew_tracker_median(const cg_skew_tracker_t *tracker) {
    int64_t samples[CG_SKEW_WINDOW_SIZE];
    size_t count = 0;
    
    for (size_t i = 0; i < CG_SKEW_WINDOW_SIZE; i++) {
        int64_t val = atomic_load_explicit(&tracker->offsets[i], memory_order_acquire);
        if (val != 0 || i == 0) {
            samples[count++] = val;
        }
    }
    
    if (count == 0) {
        return 0;
    }
    
    /* Simple insertion sort for median filtering */
    for (size_t i = 1; i < count; i++) {
        int64_t key = samples[i];
        int j = (int)i - 1;
        while (j >= 0 && samples[j] > key) {
            samples[j + 1] = samples[j];
            j--;
        }
        samples[j + 1] = key;
    }
    
    return samples[count / 2];
}

bool cg_skew_tracker_exceeded(const cg_skew_tracker_t *tracker) {
    int64_t median = cg_skew_tracker_median(tracker);
    int64_t abs_val = median < 0 ? -median : median;
    return (uint64_t)abs_val > tracker->max_skew_ns;
}

/* Bucket operations */
void cg_bucket_init(cg_bucket_t *bucket, uint32_t capacity, uint32_t refill_rate_per_sec) {
    uint64_t now = cg_get_monolithic_time_ns();
    uint32_t epoch = (uint32_t)(now / 1000000ULL); /* Milliseconds epoch */
    
    bucket->capacity = capacity;
    bucket->refill_rate_per_sec = refill_rate_per_sec;
    
    /* Pack tokens (high 32 bits) and epoch (low 32 bits) */
    uint64_t packed = ((uint64_t)capacity << 32) | (epoch & 0xFFFFFFFFULL);
    atomic_init(&bucket->packed_state, packed);
}

bool cg_bucket_try_consume(cg_bucket_t *bucket, uint32_t tokens, const cg_skew_tracker_t *tracker) {
    if (tokens > bucket->capacity) {
        return false;
    }
    
    uint64_t now_ns = cg_get_monolithic_time_ns();
    if (tracker && cg_skew_tracker_exceeded(tracker)) {
        /* Fallback to logical vector clock mode / safe degradation */
        now_ns += (uint64_t)cg_skew_tracker_median(tracker);
    }
    
    uint32_t current_epoch = (uint32_t)(now_ns / 1000000ULL);
    
    uint64_t old_state, new_state;
    do {
        old_state = atomic_load_explicit(&bucket->packed_state, memory_order_acquire);
        uint32_t current_tokens = (uint32_t)(old_state >> 32);
        uint32_t last_epoch = (uint32_t)(old_state & 0xFFFFFFFFULL);
        
        uint32_t calculated_tokens = current_tokens;
        uint32_t effective_epoch = current_epoch;
        
        if (current_epoch > last_epoch) {
            uint64_t delta_ms = (uint64_t)(current_epoch - last_epoch);
            uint64_t generated = (delta_ms * bucket->refill_rate_per_sec) / 1000ULL;
            
            uint64_t interim_tokens = (uint64_t)current_tokens + generated;
            calculated_tokens = interim_tokens > bucket->capacity ? bucket->capacity : (uint32_t)interim_tokens;
        } else {
            effective_epoch = last_epoch;
        }
        
        if (calculated_tokens < tokens) {
            return false; /* Insufficient tokens */
        }
        
        uint32_t remaining_tokens = calculated_tokens - tokens;
        new_state = ((uint64_t)remaining_tokens << 32) | (effective_epoch & 0xFFFFFFFFULL);
        
    } while (!atomic_compare_exchange_weak_explicit(&bucket->packed_state,
                                                    &old_state,
                                                    new_state,
                                                    memory_order_acq_rel,
                                                    memory_order_acquire));
    
    return true;
}

uint32_t cg_bucket_get_tokens(cg_bucket_t *bucket) {
    uint64_t state = atomic_load_explicit(&bucket->packed_state, memory_order_acquire);
    return (uint32_t)(state >> 32);
}

/* SPSC Ring Buffer implementation */
bool cg_ring_buffer_spsc_init(cg_ring_buffer_spsc_t *rb, size_t capacity, cg_arena_t *arena) {
    /* Ensure capacity is power of two */
    if ((capacity == 0) || (capacity & (capacity - 1)) != 0) {
        return false;
    }
    
    rb->capacity = capacity;
    rb->mask = capacity - 1;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    
    rb->buffer = (void **)cg_arena_alloc(arena, capacity * sizeof(void *), CG_CACHE_LINE_SIZE);
    if (!rb->buffer) {
        return false;
    }
    
    return true;
}

bool cg_ring_buffer_spsc_push(cg_ring_buffer_spsc_t *rb, void *item) {
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    
    if (head - tail >= rb->capacity) {
        return false; /* Full */
    }
    
    rb->buffer[head & rb->mask] = item;
    atomic_store_explicit(&rb->head, head + 1, memory_order_release);
    return true;
}

bool cg_ring_buffer_spsc_pop(cg_ring_buffer_spsc_t *rb, void **item) {
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    
    if (tail == head) {
        return false; /* Empty */
    }
    
    *item = rb->buffer[tail & rb->mask];
    atomic_store_explicit(&rb->tail, tail + 1, memory_order_release);
    return true;
}