// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace slick::socket::detail
{

// Lets any number of threads send on a socket concurrently with the thread that closes it, without a
// lock on the send path. A sender enters the gate and may use the socket only if it was admitted;
// close() shuts the gate, then waits until every admitted sender has left, after which the caller
// owns the socket exclusively and may close or replace it.
//
// The closed flag and the number of admitted senders share one atomic word. A sender is admitted by a
// compare-and-swap that only succeeds while the closed bit is clear, so a rejected sender never
// changes the count: once close() sets the bit, the count can only fall, and close() cannot be
// starved by callers that keep trying to send. Entering costs one CAS (retried only when another
// sender enters at the same moment), leaving one atomic decrement.
class SendGate
{
public:
    // A sender's stay in the gate; leaves on destruction. Test it before using the socket.
    class Pass
    {
    public:
        explicit Pass(SendGate& gate) noexcept : gate_(gate)
        {
            uint32_t state = gate_.state_.load(std::memory_order_relaxed);
            while ((state & closed_bit) == 0)
            {
                // Acquire: pairs with open(), so the socket set up before it is visible here
                if (gate_.state_.compare_exchange_weak(state, state + 1, std::memory_order_acquire,
                                                       std::memory_order_relaxed))
                {
                    admitted_ = true;
                    return;
                }
            }
        }

        ~Pass()
        {
            if (admitted_)
            {
                // Release: this sender's socket use happens-before close() returns
                gate_.state_.fetch_sub(1, std::memory_order_release);
            }
        }

        Pass(const Pass&) = delete;
        Pass& operator=(const Pass&) = delete;

        explicit operator bool() const noexcept { return admitted_; }

    private:
        SendGate& gate_;
        bool admitted_ = false;
    };

    // Admits senders. Everything written before (e.g. the new socket handle) is visible to them.
    void open() noexcept
    {
        state_.fetch_and(~closed_bit, std::memory_order_release);
    }

    // Stops admitting senders and waits for those already admitted to leave. Senders blocked waiting
    // on the socket must notice on their own (or be woken) for this to return promptly.
    void close() noexcept
    {
        uint32_t state = state_.fetch_or(closed_bit, std::memory_order_acquire);

        // Admitted senders leave as soon as their current send returns
        while ((state & ~closed_bit) != 0)
        {
            backoff();
            state = state_.load(std::memory_order_acquire);
        }
    }

private:
    static constexpr uint32_t closed_bit = uint32_t{1} << 31;

    // Lets the sender being waited for run. yield() is a plain reschedule on Linux and Windows, but on
    // macOS, yielding while other threads are runnable drops the caller's priority for a whole scheduling
    // quantum (~10 ms), so there it sleeps briefly instead. Not a sleep elsewhere: Windows rounds sleeps
    // up to its timer resolution (1-15.6 ms).
    static void backoff() noexcept
    {
#if defined(__APPLE__)
        std::this_thread::sleep_for(std::chrono::microseconds(50));
#else
        std::this_thread::yield();
#endif
    }

    std::atomic<uint32_t> state_{closed_bit};  // starts closed; low bits count admitted senders
};

} // namespace slick::socket::detail
