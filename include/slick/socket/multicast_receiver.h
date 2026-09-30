// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <slick/socket/logger.h>
#include <slick/socket/worker_thread.h>
#include <vector>
#include <string>
#include <chrono>
#include <atomic>
#include <thread>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace slick::socket
{

struct MulticastReceiverConfig
{
    std::string multicast_address = "224.0.0.1"; // Multicast group to join
    uint16_t port = 5000;
    std::string interface_address = "0.0.0.0"; // Interface to receive on (0.0.0.0 = any)
    bool reuse_address = true; // Allow multiple receivers on same port
    int receive_buffer_size = 65536; // Socket receive buffer size
    std::chrono::milliseconds receive_timeout{1000}; // Timeout for receive operations
};

template<typename DerivedT>
class MulticastReceiverBase
{
public:
    explicit MulticastReceiverBase(std::string name, const MulticastReceiverConfig& config = MulticastReceiverConfig());
    virtual ~MulticastReceiverBase();

    // Neither copyable nor movable: the object owns sockets, and its worker thread holds `this`
    MulticastReceiverBase(const MulticastReceiverBase&) = delete;
    MulticastReceiverBase& operator=(const MulticastReceiverBase&) = delete;
    MulticastReceiverBase(MulticastReceiverBase&&) = delete;
    MulticastReceiverBase& operator=(MulticastReceiverBase&&) = delete;

    // Receiver control
    bool start();
    void stop();

    bool is_running() const noexcept
    {
        return running_.load(std::memory_order_relaxed);
    }

    // Statistics
    uint64_t get_packets_received() const noexcept
    {
        return packets_received_.load(std::memory_order_relaxed);
    }

    uint64_t get_bytes_received() const noexcept
    {
        return bytes_received_.load(std::memory_order_relaxed);
    }

    uint64_t get_receive_errors() const noexcept
    {
        return receive_errors_.load(std::memory_order_relaxed);
    }

protected:
    DerivedT& derived() { return static_cast<DerivedT&>(*this); }
    const DerivedT& derived() const { return static_cast<const DerivedT&>(*this); }

    // Joins the receiver thread unless called from it (i.e. from handle_multicast_data()).
    // Returns false in that case; the loop exits once the callback returns and releases the socket itself.
    bool join_receiver_thread()
    {
        return detail::join_unless_current(receiver_thread_, receiver_thread_id_);
    }

    // Virtual methods to be implemented by derived class
    void receiver_loop();
    void handle_multicast_data(const std::vector<uint8_t>& data, const std::string& sender_address);

#if defined(_WIN32) || defined(_WIN64)
    using SocketT = SOCKET;
    static constexpr SocketT invalid_socket = INVALID_SOCKET;
#else
    using SocketT = int;
    static constexpr SocketT invalid_socket = -1;
#endif

    std::string name_;
    MulticastReceiverConfig config_;
    std::atomic_bool running_{false};

    SocketT socket_ = invalid_socket;
    std::thread receiver_thread_;
    detail::WorkerThreadId receiver_thread_id_;
    
    // Statistics
    std::atomic<uint64_t> packets_received_{0};
    std::atomic<uint64_t> bytes_received_{0};
    std::atomic<uint64_t> receive_errors_{0};

private:
    bool initialize_socket();
    void cleanup_socket();
    bool setup_multicast_options();
    bool join_multicast_group();
    void leave_multicast_group();

    // Leaves the group and closes the socket (no-op when already released)
    void release_socket();
};

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::stop()
{
    const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running)
    {
        LOG_INFO("Stopping {}...", name_);
    }

    // Wait for receiver thread to finish
    if (!join_receiver_thread())
    {
        // Called from handle_multicast_data(); the loop exits after it returns and cleans up
        return;
    }

    release_socket();

    if (was_running)
    {
        LOG_INFO("{} stopped", name_);
    }
}

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "multicast_receiver_win32.h"
#else
#include "multicast_receiver_unix.h"
#endif