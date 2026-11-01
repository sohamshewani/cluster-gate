#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <limits.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#define CG_CACHE_LINE 64
#define CG_MAX_SAMPLES 32
#define CG_SKEW_THRESHOLD_NS (50LL * 1000LL * 1000LL) // 50ms

typedef struct __attribute__((aligned(CG_CACHE_LINE))) {
    uint64_t samples[CG_MAX_SAMPLES];
    uint32_t head;
    uint32_t count;
    uint64_t last_sync_ns;
    int64_t current_offset_ns;
    bool logical_mode;
    uint8_t pad[39];
} cg_skew_tracker_t;

static inline uint64_t cg_get_monotonic_raw_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    if (fa < fb) return -1;
    if (fa > fb) return 1;
    return 0;
}

void cg_skew_tracker_init(cg_skew_tracker_t *tracker) {
    if (!tracker) return;
    memset(tracker, 0, sizeof(cg_skew_tracker_t));
    tracker->head = 0;
    tracker->count = 0;
    tracker->last_sync_ns = cg_get_monotonic_raw_ns();
    tracker->current_offset_ns = 0;
    tracker->logical_mode = false;
}

void cg_skew_tracker_add_sample(cg_skew_tracker_t *tracker, int64_t offset_ns) {
    if (!tracker) return;

    uint32_t idx = __atomic_load_n(&tracker->head, __ATOMIC_RELAXED);
    tracker->samples[idx] = (uint64_t)offset_ns;
    
    uint32_t next_idx = (idx + 1) % CG_MAX_SAMPLES;
    __atomic_store_n(&tracker->head, next_idx, __ATOMIC_RELEASE);

    uint32_t current_count = __atomic_load_n(&tracker->count, __ATOMIC_RELAXED);
    if (current_count < CG_MAX_SAMPLES) {
        __atomic_add_fetch(&tracker->count, 1, __ATOMIC_RELEASE);
    }
}

int64_t cg_skew_tracker_get_offset(cg_skew_tracker_t *tracker) {
    if (!tracker) return 0;

    uint32_t cnt = __atomic_load_n(&tracker->count, __ATOMIC_ACQUIRE);
    if (cnt == 0) {
        return __atomic_load_n(&tracker->current_offset_ns, __ATOMIC_RELAXED);
    }

    uint64_t sorted[CG_MAX_SAMPLES];
    for (uint32_t i = 0; i < cnt; i++) {
        sorted[i] = tracker->samples[i];
    }

    // Sort to find median (Berkeley algorithm derivative)
    qsort(sorted, cnt, sizeof(uint64_t), compare_uint64);

    int64_t median;
    if (cnt % 2 == 0) {
        median = (int64_t)((sorted[cnt / 2 - 1] + sorted[cnt / 2]) / 2);
    } else {
        median = (int64_t)sorted[cnt / 2];
    }

    __atomic_store_n(&tracker->current_offset_ns, median, __ATOMIC_RELAXED);

    if (median > CG_SKEW_THRESHOLD_NS || median < -CG_SKEW_THRESHOLD_NS) {
        __atomic_store_n(&tracker->logical_mode, true, __ATOMIC_RELEASE);
    } else {
        __atomic_store_n(&tracker->logical_mode, false, __ATOMIC_RELEASE);
    }

    return median;
}

bool cg_skew_tracker_is_logical_mode(const cg_skew_tracker_t *tracker) {
    if (!tracker) return false;
    return __atomic_load_n(&tracker->logical_mode, __ATOMIC_ACQUIRE);
}

uint64_t cg_skew_compensated_time(cg_skew_tracker_t *tracker) {
    uint64_t raw_time = cg_get_monotonic_raw_ns();
    if (!tracker) return raw_time;

    if (cg_skew_tracker_is_logical_mode(tracker)) {
        return raw_time;
    }

    int64_t offset = __atomic_load_n(&tracker->current_offset_ns, __ATOMIC_RELAXED);
    if (offset >= 0) {
        return raw_time + (uint64_t)offset;
    } else {
        uint64_t abs_offset = (uint64_t)(-offset);
        if (raw_time > abs_offset) {
            return raw_time - abs_offset;
        }
        return 0;
    }
}