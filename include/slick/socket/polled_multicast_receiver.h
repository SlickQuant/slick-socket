// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <cstddef>
#include <cstdint>
#include <slick/socket/logger.h>
#include <slick/socket/polled_socket.h>
#include <span>
#include <string>
#include <utility>

namespace slick::socket
{

struct PolledMulticastReceiverConfig
{
    std::string multicast_address = "224.0.0.1";  // Multicast group to join
    uint16_t port = 5000;
    std::string interface_address = "0.0.0.0";    // Interface to receive on (0.0.0.0 = any)
    bool reuse_address = true;                    // Allow multiple receivers on same port
    // On Unix the receiver binds its group, which keeps out unicast and broadcast sent to the port.
    // Set false to bind 0.0.0.0 and get them too. On Windows it always binds 0.0.0.0, since Winsock
    // can't bind a multicast address.
    bool bind_to_group = true;
    // SO_RCVBUF, the amount the kernel holds between two receive() calls. The kernel drops a burst
    // larger than this, so size it for the feed instead of a single packet. 0 keeps the system default.
    int receive_buffer_size = 0;
};

// A multicast receiver driven by its caller. It owns no thread and makes no callbacks. The caller's
// own loop calls receive(), which reads one datagram into the caller's buffer without blocking.
// There's no vector per packet, no sender-address string, and no hand-off from a receiver thread.
// One thread can poll any number of groups, in whatever order it chooses.
//
// Not thread-safe. Use every member from the one thread that owns the object. For a receiver with
// its own thread and a callback, see MulticastReceiverBase (multicast_receiver.h).
class PolledMulticastReceiver : private detail::SocketRuntime
{
public:
    explicit PolledMulticastReceiver(std::string name,
                                     const PolledMulticastReceiverConfig& config = PolledMulticastReceiverConfig())
        : name_(std::move(name)), config_(config)
    {
    }

    ~PolledMulticastReceiver() { close(); }

    // Not copyable or movable, because the object owns a socket
    PolledMulticastReceiver(const PolledMulticastReceiver&) = delete;
    PolledMulticastReceiver& operator=(const PolledMulticastReceiver&) = delete;
    PolledMulticastReceiver(PolledMulticastReceiver&&) = delete;
    PolledMulticastReceiver& operator=(PolledMulticastReceiver&&) = delete;

    // Binds to the port and joins the group. Returns false, with nothing left open, when it cannot.
    bool open()
    {
        if (is_open())
        {
            return true;
        }
        if (!detail::resolve_membership(name_, config_.multicast_address, config_.interface_address, membership_))
        {
            return false;
        }

        socket_ = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (socket_ == detail::invalid_polled_socket)
        {
            LOG_ERROR("{}: failed to create socket: error {}", name_, detail::last_socket_error());
            return false;
        }
        if (!detail::set_non_blocking(socket_))
        {
            LOG_ERROR("{}: failed to make socket non-blocking: error {}", name_, detail::last_socket_error());
            release_socket();
            return false;
        }
        if (config_.receive_buffer_size > 0 &&
            !detail::set_socket_option(socket_, SOL_SOCKET, SO_RCVBUF, config_.receive_buffer_size))
        {
            LOG_WARN("{}: failed to set receive buffer size: error {}", name_, detail::last_socket_error());
        }
        if (config_.reuse_address)
        {
            if (!detail::set_socket_option(socket_, SOL_SOCKET, SO_REUSEADDR, int{1}))
            {
                LOG_WARN("{}: failed to set SO_REUSEADDR: error {}", name_, detail::last_socket_error());
            }
#ifdef SO_REUSEPORT
            // BSD-derived systems (macOS, FreeBSD) need SO_REUSEPORT as well for two sockets on one port
            if (!detail::set_socket_option(socket_, SOL_SOCKET, SO_REUSEPORT, int{1}))
            {
                LOG_WARN("{}: failed to set SO_REUSEPORT: error {}", name_, detail::last_socket_error());
            }
#endif
        }

        // The socket hears no other group on this port, whichever address it binds
        if (!detail::bind_multicast_receiver(socket_, membership_.imr_multiaddr, config_.port, config_.bind_to_group))
        {
            LOG_ERROR("{}: failed to bind to port {}: error {}", name_, config_.port, detail::last_socket_error());
            release_socket();
            return false;
        }
        if (!detail::set_socket_option(socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, membership_))
        {
            LOG_ERROR("{}: failed to join multicast group {}: error {}", name_, config_.multicast_address,
                      detail::last_socket_error());
            release_socket();
            return false;
        }

        LOG_INFO("{} joined {}:{}", name_, config_.multicast_address, config_.port);
        return true;
    }

    void close() noexcept
    {
        if (!is_open())
        {
            return;
        }
        if (!detail::set_socket_option(socket_, IPPROTO_IP, IP_DROP_MEMBERSHIP, membership_))
        {
            LOG_WARN("{}: failed to leave multicast group {}: error {}", name_, config_.multicast_address,
                     detail::last_socket_error());
        }
        release_socket();
        LOG_INFO("{} closed", name_);
    }

    bool is_open() const noexcept { return socket_ != detail::invalid_polled_socket; }

    // Reads one datagram into `buffer` without blocking. Returns its size, 0 when none is waiting, or
    // -1 on an error. Errors are counted in get_receive_errors(), and the receiver stays open. A
    // datagram longer than the buffer is cut to fit, returns buffer.size(), and is counted in
    // get_packets_truncated().
    std::ptrdiff_t receive(std::span<uint8_t> buffer) noexcept
    {
        bool truncated = false;
        const std::ptrdiff_t received = detail::polled_recv_datagram(socket_, buffer.data(), buffer.size(), truncated);
        if (received < 0)
        {
            const int error = detail::last_socket_error();
            if (detail::is_would_block(error) || detail::is_interrupted(error))
            {
                return 0;
            }
            ++receive_errors_;
            return -1;
        }
        if (truncated)
        {
            ++packets_truncated_;
        }
        ++packets_received_;
        bytes_received_ += static_cast<uint64_t>(received);
        return received;
    }

    // Statistics. Plain counters: read them from the thread that calls receive().
    uint64_t get_packets_received() const noexcept { return packets_received_; }
    uint64_t get_bytes_received() const noexcept { return bytes_received_; }
    uint64_t get_receive_errors() const noexcept { return receive_errors_; }
    uint64_t get_packets_truncated() const noexcept { return packets_truncated_; }

private:
    void release_socket() noexcept
    {
        if (socket_ != detail::invalid_polled_socket)
        {
            detail::close_polled_socket(socket_);
            socket_ = detail::invalid_polled_socket;
        }
    }

    std::string name_;
    PolledMulticastReceiverConfig config_;
    detail::PolledSocket socket_ = detail::invalid_polled_socket;
    ip_mreq membership_{};

    uint64_t packets_received_ = 0;
    uint64_t bytes_received_ = 0;
    uint64_t receive_errors_ = 0;
    uint64_t packets_truncated_ = 0;
};

} // namespace slick::socket
