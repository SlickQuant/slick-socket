// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if !defined(_WIN32) && !defined(_WIN64)

#include "multicast_sender.h"
#include <utility>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <fcntl.h>
#include <poll.h>

namespace slick::socket
{

inline MulticastSender::MulticastSender(std::string name, const MulticastSenderConfig& config)
    : name_(std::move(name)), config_(config)
{
    LOG_DEBUG("MulticastSender {} created with address {}:{}", name_, config_.multicast_address, config_.port);
}

inline MulticastSender::~MulticastSender()
{
    if (running_.load(std::memory_order_relaxed))
    {
        stop();
    }
}

inline bool MulticastSender::start()
{
    if (running_.load(std::memory_order_relaxed))
    {
        LOG_WARN("{} is already running", name_);
        return true;
    }

    LOG_INFO("Starting {} on {}:{}...", name_, config_.multicast_address, config_.port);

    if (!initialize_socket())
    {
        return false;
    }

    if (!setup_multicast_options())
    {
        cleanup_socket();
        return false;
    }

    send_gate_.open();
    running_.store(true, std::memory_order_release);
    LOG_INFO("{} started successfully", name_);
    return true;
}

inline void MulticastSender::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel))
    {
        return;
    }

    LOG_INFO("Stopping {}...", name_);

    // Waits for send_data() calls on other threads to leave the socket before closing it
    send_gate_.close();
    cleanup_socket();

    LOG_INFO("{} stopped", name_);
}

inline bool MulticastSender::send_data(const std::vector<uint8_t>& data)
{
    // Holds the socket open for the duration of the send, even if another thread stops the sender
    detail::SendGate::Pass pass(send_gate_);
    if (!pass)
    {
        LOG_WARN("Cannot send data: {} is not running", name_);
        return false;
    }

    if (data.empty())
    {
        LOG_WARN("Cannot send empty data");
        return false;
    }

    // Create destination address
    sockaddr_in dest_addr{};
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(config_.port);
    
    int result = inet_pton(AF_INET, config_.multicast_address.c_str(), &dest_addr.sin_addr);
    if (result != 1)
    {
        LOG_ERROR("Invalid multicast address: {}", config_.multicast_address);
        send_errors_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    // The socket is non-blocking: a full send buffer is waited out in short slices, so a stop() on
    // another thread is noticed instead of this call staying blocked in sendto()
    ssize_t bytes_sent;
    while ((bytes_sent = sendto(socket_, data.data(), data.size(), 0,
                                reinterpret_cast<const sockaddr*>(&dest_addr), sizeof(dest_addr))) < 0 &&
           (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
    {
        if (!running_.load(std::memory_order_relaxed))
        {
            LOG_WARN("Cannot send data: {} stopped while waiting for send buffer space", name_);
            return false;
        }
        wait_writable();
    }

    if (bytes_sent < 0)
    {
        int error = errno;
        LOG_ERROR("Failed to send multicast data. error={} ({})", error, strerror(error));
        send_errors_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    if (static_cast<size_t>(bytes_sent) != data.size())
    {
        LOG_WARN("Partial send: {} bytes sent out of {}", bytes_sent, data.size());
    }

    packets_sent_.fetch_add(1, std::memory_order_relaxed);
    bytes_sent_.fetch_add(static_cast<uint64_t>(bytes_sent), std::memory_order_relaxed);

    LOG_TRACE("Sent {} bytes to multicast group {}:{}", bytes_sent, config_.multicast_address, config_.port);
    return true;
}

inline bool MulticastSender::initialize_socket()
{
    // Create UDP socket
    socket_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_ == invalid_socket)
    {
        int error = errno;
        LOG_ERROR("Failed to create socket. error={} ({})", error, strerror(error));
        return false;
    }

    // Set socket buffer size
    int buffer_size = config_.send_buffer_size;
    if (setsockopt(socket_, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size)) < 0)
    {
        int error = errno;
        LOG_WARN("Failed to set send buffer size. error={} ({})", error, strerror(error));
    }

    // Non-blocking, so send_data() can give up when stop() is called instead of blocking in sendto()
    int flags = fcntl(socket_, F_GETFL, 0);
    if (flags < 0 || fcntl(socket_, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        int error = errno;
        LOG_ERROR("Failed to make socket non-blocking. error={} ({})", error, strerror(error));
        cleanup_socket();
        return false;
    }

    return true;
}

inline void MulticastSender::wait_writable() const
{
    pollfd pfd{socket_, POLLOUT, 0};
    ::poll(&pfd, 1, poll_interval_ms);
}

inline void MulticastSender::cleanup_socket()
{
    if (socket_ != invalid_socket)
    {
        close(socket_);
        socket_ = invalid_socket;
    }
}

inline bool MulticastSender::setup_multicast_options()
{
    // Bind to local address for sending (required on some platforms like macOS)
    sockaddr_in local_addr{};
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = 0; // Let OS choose ephemeral port
    local_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(socket_, reinterpret_cast<const sockaddr*>(&local_addr), sizeof(local_addr)) < 0)
    {
        int error = errno;
        LOG_WARN("Failed to bind socket to local address. error={} ({})", error, strerror(error));
        // Don't fail on this, as it might work without binding
    }

    // Set TTL for multicast packets
    int ttl = config_.ttl;
    if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) < 0)
    {
        int error = errno;
        LOG_WARN("Failed to set multicast TTL. error={} ({})", error, strerror(error));
        // Don't fail - TTL might not be settable on all platforms
    }

    // Set multicast loopback
    int loopback = config_.enable_loopback ? 1 : 0;
    if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &loopback, sizeof(loopback)) < 0)
    {
        int error = errno;
        LOG_WARN("Failed to set multicast loopback. error={} ({})", error, strerror(error));
        // Don't fail - loopback might not be settable on all platforms
    }

    // Set multicast interface if specified
    if (config_.interface_address != "0.0.0.0")
    {
        in_addr interface_addr{};
        int result = inet_pton(AF_INET, config_.interface_address.c_str(), &interface_addr);
        if (result == 1)
        {
            if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_IF, &interface_addr, sizeof(interface_addr)) < 0)
            {
                int error = errno;
                LOG_WARN("Failed to set multicast interface. error={} ({})", error, strerror(error));
                // Don't fail - interface might not be settable on all platforms
            }
        }
        else
        {
            LOG_WARN("Invalid interface address: {}, using default", config_.interface_address);
        }
    }

    return true;
}

} // namespace slick::socket

#endif // !defined(_WIN32) && !defined(_WIN64)
