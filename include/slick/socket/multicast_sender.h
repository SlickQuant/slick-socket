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
#else
#include <netinet/in.h>
#include <arpa/inet.h>
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
    //
    // The pointer form is the primitive: it sends straight from the caller's buffer, so a publisher
    // that builds packets in place never copies or allocates per datagram.
    bool send_data(const uint8_t* data, size_t size);
    bool send_data(const std::vector<uint8_t>& data)
    {
        return send_data(data.data(), data.size());
    }
    bool send_data(const std::string& data)
    {
        return send_data(reinterpret_cast<const uint8_t*>(data.data()), data.size());
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

    // The group address, resolved once by start() rather than parsed on every send. Written before
    // send_gate_ opens, so every send_data() that gets past the gate sees it complete.
    sockaddr_in dest_addr_{};
    bool dest_addr_valid_ = false;

    // Statistics
    std::atomic<uint64_t> packets_sent_{0};
    std::atomic<uint64_t> bytes_sent_{0};
    std::atomic<uint64_t> send_errors_{0};

private:
    // Resolves config_.multicast_address into dest_addr_. An invalid address is not a start() failure:
    // it is reported, and every send then fails and counts a send error.
    void resolve_destination()
    {
        dest_addr_ = sockaddr_in{};
        dest_addr_.sin_family = AF_INET;
        dest_addr_.sin_port = htons(config_.port);
        dest_addr_valid_ = inet_pton(AF_INET, config_.multicast_address.c_str(), &dest_addr_.sin_addr) == 1;
        if (!dest_addr_valid_)
        {
            LOG_ERROR("Invalid multicast address: {}", config_.multicast_address);
        }
    }

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