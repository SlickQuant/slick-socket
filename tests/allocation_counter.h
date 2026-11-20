#pragma once

#include <cstddef>
#include <cstdint>

namespace slick::socket::test
{

// Global operator new calls the calling thread made while counting was on. The counter is thread-local,
// so to see what a worker allocated, the worker reads it itself, e.g. from inside a receive callback.
std::uint64_t allocations_on_this_thread() noexcept;

// Turns counting on or off for every thread. Each thread's counter starts at 0 and is never reset, so
// turning counting on after a worker started counts only what the worker allocates from then on.
void set_allocation_counting(bool enabled) noexcept;

// The size the calling thread asked for last, so a failing test can say what was allocated
std::size_t last_allocation_size() noexcept;

} // namespace slick::socket::test
