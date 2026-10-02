// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include "logger.h"
#include "send_gate.h"
#include <cstdint>
#include <vector>
#include <string>
#include <chrono>
#include <atomic>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace slick::socket
{

struct MulticastSenderConfig
{
    std::string multicast_address = "224.0.0.1"; // Default multicast address
    uint16_t port = 5000;
    std::string interface_address = "0.0.0.0"; // Interface to send from (0.0.0.0 = any)
    int ttl = 1; // Time-to-live for multicast packets
    bool enable_loopback = false; // Enable loopback of multicast packets
    int send_buffer_size = 65536; // Socket send buffer size
};

class MulticastSender
{
public:
    explicit MulticastSender(std::string name, const MulticastSenderConfig& config = MulticastSenderConfig());
    virtual ~MulticastSender();

    // Neither copyable nor movable: the object owns its socket
    MulticastSender(const MulticastSender&) = delete;
    MulticastSender& operator=(const MulticastSender&) = delete;
    MulticastSender(MulticastSender&&) = delete;
    MulticastSender& operator=(MulticastSender&&) = delete;

    // Sender control
    // start()/stop() must not run concurrently with each other; call them from one thread at a time.
    // They replace the socket and are not synchronized against each other. send_data() may run on any
    // thread, concurrently with both.
    bool start();
    void stop();

    bool is_running() const noexcept
    {
        return running_.load(std::memory_order_relaxed);
    }

    // Safe to call from any number of threads, including concurrently with stop(): the socket is never
    // closed while a send is using it. Each call sends one datagram. If the send buffer is full, it
    // waits for room, but gives up (returns false) within poll_interval_ms once stop() is called.
    bool send_data(const std::vector<uint8_t>& data);
    bool send_data(const std::string& data)
    {
        std::vector<uint8_t> buffer(data.begin(), data.end());
        return send_data(buffer);
    }

    // Statistics
    uint64_t get_packets_sent() const noexcept
    {
        return packets_sent_.load(std::memory_order_relaxed);
    }

    uint64_t get_bytes_sent() const noexcept
    {
        return bytes_sent_.load(std::memory_order_relaxed);
    }

    uint64_t get_send_errors() const noexcept
    {
        return send_errors_.load(std::memory_order_relaxed);
    }

protected:

#if defined(_WIN32) || defined(_WIN64)
    using SocketT = SOCKET;
    static constexpr SocketT invalid_socket = INVALID_SOCKET;
#else
    using SocketT = int;
    static constexpr SocketT invalid_socket = -1;
#endif

    std::string name_;
    MulticastSenderConfig config_;
    std::atomic_bool running_{false};

    SocketT socket_ = invalid_socket;
    detail::SendGate send_gate_;  // keeps socket_ open while send_data() uses it from other threads
    
    // Statistics
    std::atomic<uint64_t> packets_sent_{0};
    std::atomic<uint64_t> bytes_sent_{0};
    std::atomic<uint64_t> send_errors_{0};

private:
    bool initialize_socket();
    void cleanup_socket();
    bool setup_multicast_options();

    // Waits up to poll_interval_ms for room in the socket's send buffer
    void wait_writable() const;

    // How long a send blocked on a full buffer waits before re-checking running_
    static constexpr int poll_interval_ms = 1;
};

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "multicast_sender_win32.h"
#else
#include "multicast_sender_unix.h"
#endif