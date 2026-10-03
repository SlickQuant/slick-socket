// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if defined(_WIN32) || defined(_WIN64)

#include "logger.h"
#include "multicast_sender.h"
#include <utility>
#include <winsock2.h>
#include <ws2tcpip.h>

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

    // Initialize Winsock if not already done
    WSADATA wsaData;
    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (result != 0)
    {
        LOG_ERROR("WSAStartup failed: {}", result);
        return false;
    }

    if (!initialize_socket())
    {
        WSACleanup();
        return false;
    }

    if (!setup_multicast_options())
    {
        cleanup_socket();
        WSACleanup();
        return false;
    }

    resolve_destination();
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
    WSACleanup();

    LOG_INFO("{} stopped", name_);
}

inline bool MulticastSender::send_data(const uint8_t* data, size_t size)
{
    // Holds the socket open for the duration of the send, even if another thread stops the sender
    detail::SendGate::Pass pass(send_gate_);
    if (!pass)
    {
        LOG_WARN("Cannot send data: {} is not running", name_);
        return false;
    }

    if (size == 0)
    {
        LOG_WARN("Cannot send empty data");
        return false;
    }

    if (!dest_addr_valid_)
    {
        send_errors_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    // The socket is non-blocking: a full send buffer is waited out in short slices, so a stop() on
    // another thread is noticed instead of this call staying blocked in sendto()
    int bytes_sent;
    while ((bytes_sent = sendto(socket_, reinterpret_cast<const char*>(data), static_cast<int>(size),
                                0, reinterpret_cast<const sockaddr*>(&dest_addr_), sizeof(dest_addr_))) == SOCKET_ERROR &&
           WSAGetLastError() == WSAEWOULDBLOCK)
    {
        if (!running_.load(std::memory_order_relaxed))
        {
            LOG_WARN("Cannot send data: {} stopped while waiting for send buffer space", name_);
            return false;
        }
        wait_writable();
    }

    if (bytes_sent == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_ERROR("Failed to send multicast data. error={}", error);
        send_errors_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    if (static_cast<size_t>(bytes_sent) != size)
    {
        LOG_WARN("Partial send: {} bytes sent out of {}", bytes_sent, size);
    }

    packets_sent_.fetch_add(1, std::memory_order_relaxed);
    bytes_sent_.fetch_add(static_cast<uint64_t>(bytes_sent), std::memory_order_relaxed);

    LOG_TRACE("Sent {} bytes to multicast group {}:{}", bytes_sent, config_.multicast_address, config_.port);
    return true;
}

inline bool MulticastSender::initialize_socket()
{
    // Create UDP socket
    socket_ = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (socket_ == invalid_socket)
    {
        int error = WSAGetLastError();
        LOG_ERROR("Failed to create socket. error={}", error);
        return false;
    }

    // Set socket buffer size
    int buffer_size = config_.send_buffer_size;
    if (setsockopt(socket_, SOL_SOCKET, SO_SNDBUF, 
                   reinterpret_cast<const char*>(&buffer_size), sizeof(buffer_size)) == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_WARN("Failed to set send buffer size. error={}", error);
    }

    // Non-blocking, so send_data() can give up when stop() is called instead of blocking in sendto()
    u_long non_blocking = 1;
    if (ioctlsocket(socket_, FIONBIO, &non_blocking) != 0)
    {
        LOG_ERROR("Failed to make socket non-blocking. error={}", WSAGetLastError());
        cleanup_socket();
        return false;
    }

    return true;
}

inline void MulticastSender::wait_writable() const
{
    WSAPOLLFD pfd{socket_, POLLOUT, 0};
    WSAPoll(&pfd, 1, poll_interval_ms);
}

inline void MulticastSender::cleanup_socket()
{
    if (socket_ != invalid_socket)
    {
        closesocket(socket_);
        socket_ = invalid_socket;
    }
}

inline bool MulticastSender::setup_multicast_options()
{
    // Set TTL for multicast packets
    DWORD ttl = static_cast<DWORD>(config_.ttl);
    if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_TTL,
                   reinterpret_cast<const char*>(&ttl), sizeof(ttl)) == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_ERROR("Failed to set multicast TTL. error={}", error);
        return false;
    }

    // Set multicast loopback
    DWORD loopback = config_.enable_loopback ? 1 : 0;
    if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_LOOP,
                   reinterpret_cast<const char*>(&loopback), sizeof(loopback)) == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_ERROR("Failed to set multicast loopback. error={}", error);
        return false;
    }

    // Set multicast interface if specified
    if (config_.interface_address != "0.0.0.0")
    {
        in_addr interface_addr{};
        int result = inet_pton(AF_INET, config_.interface_address.c_str(), &interface_addr);
        if (result == 1)
        {
            if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_IF,
                          reinterpret_cast<const char*>(&interface_addr), sizeof(interface_addr)) == SOCKET_ERROR)
            {
                int error = WSAGetLastError();
                LOG_ERROR("Failed to set multicast interface. error={}", error);
                return false;
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

#endif // defined(_WIN32) || defined(_WIN64)
