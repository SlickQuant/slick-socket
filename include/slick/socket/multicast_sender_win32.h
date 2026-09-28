// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include "logger.h"
#include "multicast_sender.h"
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

    running_.store(true, std::memory_order_relaxed);
    LOG_INFO("{} started successfully", name_);
    return true;
}

inline void MulticastSender::stop()
{
    if (!running_.load(std::memory_order_relaxed))
    {
        return;
    }

    LOG_INFO("Stopping {}...", name_);
    running_.store(false, std::memory_order_relaxed);

    cleanup_socket();
    WSACleanup();

    LOG_INFO("{} stopped", name_);
}

inline bool MulticastSender::send_data(const std::vector<uint8_t>& data)
{
    if (!running_.load(std::memory_order_relaxed))
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

    // Send the data
    int bytes_sent = sendto(socket_, 
                           reinterpret_cast<const char*>(data.data()), 
                           static_cast<int>(data.size()),
                           0,
                           reinterpret_cast<const sockaddr*>(&dest_addr),
                           sizeof(dest_addr));

    if (bytes_sent == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_ERROR("Failed to send multicast data. error={}", error);
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

    return true;
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