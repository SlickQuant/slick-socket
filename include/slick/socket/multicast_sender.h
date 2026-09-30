// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include "logger.h"
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
    bool start();
    void stop();

    bool is_running() const noexcept
    {
        return running_.load(std::memory_order_relaxed);
    }

    // Send data
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
    
    // Statistics
    std::atomic<uint64_t> packets_sent_{0};
    std::atomic<uint64_t> bytes_sent_{0};
    std::atomic<uint64_t> send_errors_{0};

private:
    bool initialize_socket();
    void cleanup_socket();
    bool setup_multicast_options();
};

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "multicast_sender_win32.h"
#else
#include "multicast_sender_unix.h"
#endif