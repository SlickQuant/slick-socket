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
    // SO_RCVBUF: what the kernel holds between two receive() calls. A burst larger than this is
    // dropped by the kernel, so size it for the feed, not for one packet. 0 keeps the system default.
    int receive_buffer_size = 0;
};

// A multicast receiver driven by its caller: it owns no thread and makes no callbacks. The caller's
// own loop calls receive(), which reads one datagram into the caller's buffer without blocking - no
// vector per packet, no sender-address string, and no hand-off from a receiver thread. One thread can
// poll any number of groups, in whatever order it chooses.
//
// Not thread-safe: every member is for the one thread that owns the object. For a receiver with its
// own thread and a callback, see MulticastReceiverBase (multicast_receiver.h).
class PolledMulticastReceiver : private detail::SocketRuntime
{
public:
    explicit PolledMulticastReceiver(std::string name,
                                     const PolledMulticastReceiverConfig& config = PolledMulticastReceiverConfig())
        : name_(std::move(name)), config_(config)
    {
    }

    ~PolledMulticastReceiver() { close(); }

    // Neither copyable nor movable: the object owns a socket
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
        if (inet_pton(AF_INET, config_.multicast_address.c_str(), &membership_.imr_multiaddr) != 1)
        {
            LOG_ERROR("{}: invalid multicast address {}", name_, config_.multicast_address);
            return false;
        }
        if (inet_pton(AF_INET, config_.interface_address.c_str(), &membership_.imr_interface) != 1)
        {
            LOG_WARN("{}: invalid interface address {}, using any interface", name_, config_.interface_address);
            membership_.imr_interface.s_addr = htonl(INADDR_ANY);
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

        sockaddr_in bind_addr{};
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_port = htons(config_.port);
        bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (::bind(socket_, reinterpret_cast<const sockaddr*>(&bind_addr), sizeof(bind_addr)) != 0)
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
    // -1 on an error (counted in get_receive_errors(); the receiver stays open). A datagram longer
    // than the buffer is cut to it - buffer.size() is returned - and counted in
    // get_packets_truncated().
    std::ptrdiff_t receive(std::span<uint8_t> buffer) noexcept
    {
#if defined(_WIN32) || defined(_WIN64)
        std::ptrdiff_t received = detail::polled_recv(socket_, buffer.data(), buffer.size());
#else
        // MSG_TRUNC: the datagram's real length, so one that did not fit is recognised
        std::ptrdiff_t received = ::recv(socket_, buffer.data(), buffer.size(), MSG_TRUNC);
#endif
        if (received < 0)
        {
            const int error = detail::last_socket_error();
            if (detail::is_would_block(error) || detail::is_interrupted(error))
            {
                return 0;
            }
            if (!detail::is_truncated(error))
            {
                ++receive_errors_;
                return -1;
            }
            received = static_cast<std::ptrdiff_t>(buffer.size()) + 1;
        }
        if (received > static_cast<std::ptrdiff_t>(buffer.size()))
        {
            ++packets_truncated_;
            received = static_cast<std::ptrdiff_t>(buffer.size());
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
