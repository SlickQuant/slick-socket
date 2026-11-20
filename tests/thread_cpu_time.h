#pragma once

#include <chrono>
#include <thread>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>  // before windows.h, so the two agree on the socket types
#include <windows.h>
#elif !defined(__APPLE__)
#include <pthread.h>
#include <time.h>
#endif

namespace slick::socket::test
{

// CPU time `thread` has used so far, kernel and user. Not measured on macOS, where it returns -1 and
// leaves `thread` unused.
inline std::chrono::nanoseconds thread_cpu_time([[maybe_unused]] std::thread& thread) noexcept
{
#if defined(_WIN32) || defined(_WIN64)
    FILETIME creation, exit, kernel, user;
    if (!GetThreadTimes(thread.native_handle(), &creation, &exit, &kernel, &user))
    {
        return std::chrono::nanoseconds{-1};
    }
    auto ticks = [](const FILETIME& ft) {
        return (static_cast<uint64_t>(ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
    };
    return std::chrono::nanoseconds{(ticks(kernel) + ticks(user)) * 100};
#elif defined(__APPLE__)
    return std::chrono::nanoseconds{-1};
#else
    clockid_t clock_id;
    timespec ts{};
    if (pthread_getcpuclockid(thread.native_handle(), &clock_id) != 0 || clock_gettime(clock_id, &ts) != 0)
    {
        return std::chrono::nanoseconds{-1};
    }
    return std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
#endif
}

} // namespace slick::socket::test
