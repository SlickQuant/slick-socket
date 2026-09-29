// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <slick/socket/logger.h>
#include <atomic>
#include <cassert>
#include <string>
#include <thread>

// Invoked when a server, client or receiver is destroyed before its worker thread was stopped and joined.
// The base-class destructor runs after the derived object's members are destroyed, so a callback
// running at that point touches destroyed state: call stop()/disconnect() before destruction
// (e.g. in the derived destructor). Asserts in debug builds by default; define this macro before
// including any slick-socket header to customize it (e.g. to count violations or abort). It runs
// inside a noexcept destructor, so it must not throw: an escaping exception calls std::terminate.
#ifndef SLICK_SOCKET_ON_UNSAFE_DESTROY
#define SLICK_SOCKET_ON_UNSAFE_DESTROY() \
    assert(!"slick-socket: call stop()/disconnect() before destroying the derived object")
#endif

namespace slick::socket::detail
{

// Identifies a worker thread without reading its std::thread object, which the starting
// thread may still be assigning when the worker's first callback runs.
class WorkerThreadId
{
public:
    // Marks the calling thread as the worker for the lifetime of the scope
    class Scope
    {
    public:
        explicit Scope(WorkerThreadId& id) noexcept : id_(id)
        {
            id_.id_.store(std::this_thread::get_id(), std::memory_order_relaxed);
        }

        ~Scope()
        {
            // Cleared on exit so a later thread that reuses this id is not mistaken for the worker
            id_.id_.store(std::thread::id{}, std::memory_order_relaxed);
        }

        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

    private:
        WorkerThreadId& id_;
    };

    bool is_current() const noexcept
    {
        return id_.load(std::memory_order_relaxed) == std::this_thread::get_id();
    }

private:
    std::atomic<std::thread::id> id_{};
};

// Returns false (and logs) when a base-class destructor finds the worker not yet joined, i.e.
// stop()/disconnect() was not called from outside the worker before destruction. Checking the
// join state (rather than whether the worker is still executing) keeps the report deterministic.
// The caller invokes SLICK_SOCKET_ON_UNSAFE_DESTROY() so the hook expands in the templated
// destructor rather than in this shared inline function.
inline bool stopped_before_destroy(const std::thread& worker, const std::string& name)
{
    if (worker.joinable())
    {
        LOG_ERROR("{} destroyed while its worker thread is running; call stop()/disconnect() "
                  "before destroying it", name);
        return false;
    }
    return true;
}

// Joins the worker unless called from it (e.g. from one of its callbacks).
// Returns false in that case; the worker exits on its own and is joined later.
inline bool join_unless_current(std::thread& thread, const WorkerThreadId& id)
{
    if (id.is_current())
    {
        return false;
    }
    if (thread.joinable())
    {
        thread.join();
    }
    return true;
}

} // namespace slick::socket::detail
