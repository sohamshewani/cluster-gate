#include "cluster_gate_ring_buffer.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define CACHE_LINE_SIZE 64

static inline size_t round_up_power_of_two(size_t v) {
    if (v == 0) return 1;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
#if SIZE_MAX > 0xFFFFFFFF
    v |= v >> 32;
#endif
    v++;
    return v;
}

int cg_ring_buffer_init(cg_ring_buffer_t *rb, size_t capacity, cg_arena_t *arena) {
    if (!rb || capacity == 0) {
        return -1;
    }

    size_t actual_capacity = round_up_power_of_two(capacity);
    rb->capacity = actual_capacity;
    rb->mask = actual_capacity - 1;
    rb->head = 0;
    rb->tail = 0;

    size_t slots_size = actual_capacity * sizeof(void *);
    size_t seqs_size = actual_capacity * sizeof(uint32_t);

    if (arena) {
        rb->slots = (void **)cg_arena_alloc(arena, slots_size, CACHE_LINE_SIZE);
        rb->sequences = (uint32_t *)cg_arena_alloc(arena, seqs_size, CACHE_LINE_SIZE);
    } else {
        if (posix_memalign((void **)&rb->slots, CACHE_LINE_SIZE, slots_size) != 0) {
            return -1;
        }
        if (posix_memalign((void **)&rb->sequences, CACHE_LINE_SIZE, seqs_size) != 0) {
            free(rb->slots);
            rb->slots = NULL;
            return -1;
        }
    }

    if (!rb->slots || !rb->sequences) {
        if (!arena) {
            free(rb->slots);
            free(rb->sequences);
        }
        return -1;
    }

    memset(rb->slots, 0, slots_size);
    for (size_t i = 0; i < actual_capacity; i++) {
        __atomic_store_n(&rb->sequences[i], (uint32_t)i, __ATOMIC_STORED);
    }

    __atomic_store_n(&rb->head, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&rb->tail, 0, __ATOMIC_SEQ_CST);

    return 0;
}

void cg_ring_buffer_destroy(cg_ring_buffer_t *rb, cg_arena_t *arena) {
    if (!rb) {
        return;
    }
    if (!arena) {
        free(rb->slots);
        free(rb->sequences);
    }
    rb->slots = NULL;
    rb->sequences = NULL;
    rb->capacity = 0;
    rb->mask = 0;
}

bool cg_ring_buffer_mpmc_push(cg_ring_buffer_t *rb, void *item) {
    if (!rb || !item) {
        return false;
    }

    uint32_t tail = __atomic_load_n(&rb->tail, __ATOMIC_ACQUIRE);
    for (;;) {
        uint32_t idx = tail & rb->mask;
        uint32_t seq = __atomic_load_n(&rb->sequences[idx], __ATOMIC_ACQUIRE);
        intptr_t diff = (intptr_t)seq - (intptr_t)tail;

        if (diff == 0) {
            if (__atomic_compare_exchange_n(&rb->tail, &tail, tail + 1,
                                            true, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
                break;
            }
        } else if (diff < 0) {
            return false;
        } else {
            tail = __atomic_load_n(&rb->tail, __ATOMIC_ACQUIRE);
        }
    }

    uint32_t idx = tail & rb->mask;
    rb->slots[idx] = item;
    __atomic_store_n(&rb->sequences[idx], tail + 1, __ATOMIC_RELEASE);
    return true;
}

bool cg_ring_buffer_mpmc_pop(cg_ring_buffer_t *rb, void **item) {
    if (!rb || !item) {
        return false;
    }

    uint32_t head = __atomic_load_n(&rb->head, __ATOMIC_ACQUIRE);
    for (;;) {
        uint32_t idx = head & rb->mask;
        uint32_t seq = __atomic_load_n(&rb->sequences[idx], __ATOMIC_ACQUIRE);
        intptr_t diff = (intptr_t)seq - (intptr_t)(head + 1);

        if (diff == 0) {
            if (__atomic_compare_exchange_n(&rb->head, &head, head + 1,
                                            true, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
                break;
            }
        } else if (diff < 0) {
            return false;
        } else {
            head = __atomic_load_n(&rb->head, __ATOMIC_ACQUIRE);
        }
    }

    uint32_t idx = head & rb->mask;
    *item = rb->slots[idx];
    rb->slots[idx] = NULL;
    __atomic_store_n(&rb->sequences[idx], head + rb->capacity, __ATOMIC_RELEASE);
    return true;
}

bool cg_ring_buffer_spsc_push(cg_ring_buffer_t *rb, void *item) {
    if (!rb || !item) {
        return false;
    }

    uint32_t tail = __atomic_load_n(&rb->tail, __ATOMIC_RELAXED);
    uint32_t head = __atomic_load_n(&rb->head, __ATOMIC_ACQUIRE);

    if (tail - head >= (uint32_t)rb->capacity) {
        return false;
    }

    uint32_t idx = tail & rb->mask;
    rb->slots[idx] = item;
    __atomic_store_n(&rb->tail, tail + 1, __ATOMIC_RELEASE);
    return true;
}

bool cg_ring_buffer_spsc_pop(cg_ring_buffer_t *rb, void **item) {
    if (!rb || !item) {
        return false;
    }

    uint32_t head = __atomic_load_n(&rb->head, __ATOMIC_RELAXED);
    uint32_t tail = __atomic_load_n(&rb->tail, __ATOMIC_ACQUIRE);

    if (head == tail) {
        return false;
    }

    uint32_t idx = head & rb->mask;
    *item = rb->slots[idx];
    rb->slots[idx] = NULL;
    __atomic_store_n(&rb->head, head + 1, __ATOMIC_RELEASE);
    return true;
}

size_t cg_ring_buffer_size(const cg_ring_buffer_t *rb) {
    if (!rb) {
        return 0;
    }
    uint32_t tail = __atomic_load_n(&rb->tail, __ATOMIC_ACQUIRE);
    uint32_t head = __atomic_load_n(&rb->head, __ATOMIC_ACQUIRE);
    if (tail >= head) {
        return (size_t)(tail - head);
    }
    return (size_t)((uint64_t)tail + rb->capacity - head);
}

bool cg_ring_buffer_empty(const cg_ring_buffer_t *rb) {
    if (!rb) {
        return true;
    }
    uint32_t tail = __atomic_load_n(&rb->tail, __ATOMIC_ACQUIRE);
    uint32_t head = __atomic_load_n(&rb->head, __ATOMIC_ACQUIRE);
    return head == tail;
}