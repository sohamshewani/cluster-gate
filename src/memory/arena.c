#include "memory/arena.h"
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000
#endif

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << 21)
#endif

#define CG_CACHE_LINE_SIZE 64

struct cg_arena {
    uint8_t *buffer;
    size_t capacity;
    _Atomic size_t offset;
    bool is_huge_page;
    char name[32];
};

cg_arena_t *cg_arena_create(size_t capacity, const char *name) {
    if (capacity == 0) {
        return NULL;
    }

    size_t page_size = (size_t)sysconf(_SC_PAGESIZE);
    size_t aligned_capacity = (capacity + page_size - 1) & ~(page_size - 1);

    size_t total_size = sizeof(cg_arena_t) + aligned_capacity;
    total_size = (total_size + CG_CACHE_LINE_SIZE - 1) & ~(CG_CACHE_LINE_SIZE - 1);

    void *raw_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw_mem == MAP_FAILED) {
        return NULL;
    }

    cg_arena_t *arena = (cg_arena_t *)raw_mem;
    arena->buffer = (uint8_t *)raw_mem + sizeof(cg_arena_t);
    arena->capacity = aligned_capacity;
    atomic_init(&arena->offset, 0);
    arena->is_huge_page = false;

    if (name) {
        snprintf(arena->name, sizeof(arena->name), "%s", name);
    } else {
        snprintf(arena->name, sizeof(arena->name), "%s", "default_arena");
    }

    size_t huge_size = 2ULL * 1024 * 1024;
    if (aligned_capacity >= huge_size) {
        void *huge_mem = mmap(NULL, aligned_capacity, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
        if (huge_mem != MAP_FAILED) {
            munmap(raw_mem, total_size);
            
            size_t total_huge_size = sizeof(cg_arena_t) + aligned_capacity;
            total_huge_size = (total_huge_size + huge_size - 1) & ~(huge_size - 1);
            
            void *base_huge = mmap(NULL, total_huge_size, PROT_READ | PROT_WRITE,
                                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
            if (base_huge != MAP_FAILED) {
                arena = (cg_arena_t *)base_huge;
                arena->buffer = (uint8_t *)base_huge + sizeof(cg_arena_t);
                arena->capacity = total_huge_size - sizeof(cg_arena_t);
                atomic_init(&arena->offset, 0);
                arena->is_huge_page = true;
                if (name) {
                    snprintf(arena->name, sizeof(arena->name), "%s", name);
                } else {
                    snprintf(arena->name, sizeof(arena->name), "%s", "huge_arena");
                }
                madvise(arena->buffer, arena->capacity, MADV_HUGEPAGE);
                return arena;
            }
        }
    }

    madvise(arena->buffer, arena->capacity, MADV_HUGEPAGE);
    return arena;
}

void cg_arena_destroy(cg_arena_t *arena) {
    if (!arena) {
        return;
    }

    size_t total_size;
    if (arena->is_huge_page) {
        total_size = sizeof(cg_arena_t) + arena->capacity;
        size_t huge_size = 2ULL * 1024 * 1024;
        total_size = (total_size + huge_size - 1) & ~(huge_size - 1);
    } else {
        total_size = sizeof(cg_arena_t) + arena->capacity;
        total_size = (total_size + CG_CACHE_LINE_SIZE - 1) & ~(CG_CACHE_LINE_SIZE - 1);
    }

    munmap(arena, total_size);
}

void *cg_arena_alloc(cg_arena_t *arena, size_t size, size_t alignment) {
    if (!arena || size == 0) {
        return NULL;
    }

    if (alignment == 0) {
        alignment = CG_CACHE_LINE_SIZE;
    }

    size_t current_offset, new_offset, aligned_offset, padding;

    while (true) {
        current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
        
        uintptr_t current_address = (uintptr_t)(arena->buffer + current_offset);
        uintptr_t aligned_address = (current_address + alignment - 1) & ~(alignment - (uintptr_t)1);
        padding = (size_t)(aligned_address - current_address);
        
        aligned_offset = current_offset + padding;
        new_offset = aligned_offset + size;

        if (new_offset > arena->capacity) {
            return NULL;
        }

        if (atomic_compare_exchange_weak_explicit(&arena->offset, &current_offset, new_offset,
                                                  memory_order_release, memory_order_relaxed)) {
            return (void *)aligned_address;
        }
    }
}

void *cg_arena_calloc(cg_arena_t *arena, size_t num, size_t size, size_t alignment) {
    if (num == 0 || size == 0) {
        return NULL;
    }

    if (num > SIZE_MAX / size) {
        return NULL;
    }

    size_t total_size = num * size;
    void *ptr = cg_arena_alloc(arena, total_size, alignment);
    if (ptr) {
        memset(ptr, 0, total_size);
    }
    return ptr;
}

void cg_arena_reset(cg_arena_t *arena) {
    if (arena) {
        atomic_store_explicit(&arena->offset, 0, memory_order_release);
    }
}

size_t cg_arena_used_bytes(const cg_arena_t *arena) {
    if (!arena) {
        return 0;
    }
    return atomic_load_explicit(&arena->offset, memory_order_relaxed);
}

size_t cg_arena_available_bytes(const cg_arena_t *arena) {
    if (!arena) {
        return 0;
    }
    size_t used = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    if (used >= arena->capacity) {
        return 0;
    }
    return arena->capacity - used;
}