// Replaces the global allocation functions so a test can check that a hot path allocates nothing. The
// counter is thread-local and only counts while counting is on, so the test's own allocations, gtest's
// included, never reach a worker thread's total.

#include "allocation_counter.h"
#include <atomic>
#include <cstdlib>
#include <new>

#if defined(_WIN32) || defined(_WIN64)
#include <malloc.h>  // _aligned_malloc/_aligned_free
#endif

namespace
{

std::atomic<bool> g_counting{false};
thread_local std::uint64_t g_allocations = 0;
thread_local std::size_t g_last_size = 0;

// Never hand malloc a zero size, which it may or may not treat as an allocation
std::size_t usable_size(std::size_t size) noexcept
{
    return size == 0 ? 1 : size;
}

} // namespace

void* operator new(std::size_t size)
{
    if (g_counting.load(std::memory_order_relaxed))
    {
        ++g_allocations;
        g_last_size = size;
    }
    if (void* memory = std::malloc(usable_size(size)))
    {
        return memory;
    }
    throw std::bad_alloc();
}

void* operator new[](std::size_t size)
{
    return ::operator new(size);
}

void operator delete(void* memory) noexcept
{
    std::free(memory);
}

void operator delete[](void* memory) noexcept
{
    std::free(memory);
}

void operator delete(void* memory, std::size_t) noexcept
{
    std::free(memory);
}

void operator delete[](void* memory, std::size_t) noexcept
{
    std::free(memory);
}

#if defined(__cpp_aligned_new)

void* operator new(std::size_t size, std::align_val_t alignment)
{
    if (g_counting.load(std::memory_order_relaxed))
    {
        ++g_allocations;
    }
#if defined(_WIN32) || defined(_WIN64)
    if (void* memory = _aligned_malloc(usable_size(size), static_cast<std::size_t>(alignment)))
#else
    void* memory = nullptr;
    if (posix_memalign(&memory, static_cast<std::size_t>(alignment), usable_size(size)) == 0)
#endif
    {
        return memory;
    }
    throw std::bad_alloc();
}

void* operator new[](std::size_t size, std::align_val_t alignment)
{
    return ::operator new(size, alignment);
}

void operator delete(void* memory, std::align_val_t) noexcept
{
#if defined(_WIN32) || defined(_WIN64)
    _aligned_free(memory);
#else
    std::free(memory);
#endif
}

void operator delete[](void* memory, std::align_val_t alignment) noexcept
{
    ::operator delete(memory, alignment);
}

void operator delete(void* memory, std::size_t, std::align_val_t alignment) noexcept
{
    ::operator delete(memory, alignment);
}

void operator delete[](void* memory, std::size_t, std::align_val_t alignment) noexcept
{
    ::operator delete(memory, alignment);
}

#endif

namespace slick::socket::test
{

std::uint64_t allocations_on_this_thread() noexcept
{
    return g_allocations;
}

void set_allocation_counting(bool enabled) noexcept
{
    g_counting.store(enabled, std::memory_order_relaxed);
}

std::size_t last_allocation_size() noexcept
{
    return g_last_size;
}

} // namespace slick::socket::test
