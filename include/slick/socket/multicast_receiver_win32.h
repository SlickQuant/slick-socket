// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include "logger.h"
#include "multicast_receiver.h"
#include <winsock2.h>
#include <ws2tcpip.h>

namespace slick::socket
{

template<typename DerivedT>
MulticastReceiverBase<DerivedT>::MulticastReceiverBase(std::string name, const MulticastReceiverConfig& config)
    : name_(std::move(name)), config_(config)
{
    LOG_DEBUG("MulticastReceiver {} created for group {}:{}", name_, config_.multicast_address, config_.port);
}

template<typename DerivedT>
MulticastReceiverBase<DerivedT>::~MulticastReceiverBase()
{
    if (!detail::stopped_before_destroy(receiver_thread_, name_))
    {
        SLICK_SOCKET_ON_UNSAFE_DESTROY();
    }

    // Also joins a receiver thread that was stopped from within the callback
    stop();
}

template<typename DerivedT>
bool MulticastReceiverBase<DerivedT>::start()
{
    if (running_.load(std::memory_order_relaxed))
    {
        LOG_WARN("{} is already running", name_);
        return true;
    }

    // Reap a receiver thread that was stopped from within the callback
    if (!join_receiver_thread())
    {
        LOG_ERROR("Cannot restart {} from within the receive callback", name_);
        return false;
    }

    LOG_INFO("Starting {} for group {}:{}...", name_, config_.multicast_address, config_.port);

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

    if (!join_multicast_group())
    {
        cleanup_socket();
        WSACleanup();
        return false;
    }

    running_.store(true, std::memory_order_relaxed);

    // Start receiver thread
    receiver_thread_ = std::thread(&MulticastReceiverBase::receiver_loop, this);

    LOG_INFO("{} started successfully", name_);
    return true;
}

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::receiver_loop()
{
    detail::WorkerThreadId::Scope worker_scope(receiver_thread_id_);

    std::vector<uint8_t> buffer(config_.receive_buffer_size);
    sockaddr_in sender_addr{};
    int sender_addr_len = sizeof(sender_addr);

    LOG_DEBUG("Receiver loop started for {}", name_);

    // Receive timeout bounds how long stop() waits; set it once rather than per packet
    DWORD timeout = static_cast<DWORD>(config_.receive_timeout.count());
    if (setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO,
                   reinterpret_cast<const char*>(&timeout), sizeof(timeout)) == SOCKET_ERROR)
    {
        LOG_WARN("Failed to set receive timeout");
    }

    while (running_.load(std::memory_order_relaxed))
    {
        sender_addr_len = sizeof(sender_addr);
        int bytes_received = recvfrom(socket_,
                                     reinterpret_cast<char*>(buffer.data()),
                                     static_cast<int>(buffer.size()),
                                     0,
                                     reinterpret_cast<sockaddr*>(&sender_addr),
                                     &sender_addr_len);

        if (bytes_received == SOCKET_ERROR)
        {
            int error = WSAGetLastError();
            if (error == WSAETIMEDOUT)
            {
                // Timeout is normal, continue loop
                continue;
            }
            else if (running_.load(std::memory_order_relaxed))
            {
                LOG_ERROR("Failed to receive multicast data. error={}", error);
                receive_errors_.fetch_add(1, std::memory_order_relaxed);
            }
            continue;
        }

        if (bytes_received > 0)
        {
            packets_received_.fetch_add(1, std::memory_order_relaxed);
            bytes_received_.fetch_add(static_cast<uint64_t>(bytes_received), std::memory_order_relaxed);

            // Get sender address as string
            char sender_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &sender_addr.sin_addr, sender_ip, INET_ADDRSTRLEN);
            std::string sender_address = sender_ip;

            LOG_TRACE("Received {} bytes from {}", bytes_received, sender_address);

            // Resize buffer to actual data size and call handler
            buffer.resize(static_cast<size_t>(bytes_received));
            derived().handle_multicast_data(buffer, sender_address);
            buffer.resize(config_.receive_buffer_size); // Reset buffer size
        }
    }

    // Release here so a stop() issued from the callback also cleans up
    release_socket();
    LOG_DEBUG("Receiver loop ended for {}", name_);
}

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::release_socket()
{
    if (socket_ == invalid_socket)
    {
        return;
    }
    leave_multicast_group();
    cleanup_socket();
    WSACleanup();  // pairs with the WSAStartup in a successful start()
}

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::handle_multicast_data(const std::vector<uint8_t>& data, const std::string& sender_address)
{
    // Default implementation - derived classes should override this
    LOG_TRACE("Received {} bytes from {} (no handler implemented)", data.size(), sender_address);
}

template<typename DerivedT>
bool MulticastReceiverBase<DerivedT>::initialize_socket()
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
    int buffer_size = config_.receive_buffer_size;
    if (setsockopt(socket_, SOL_SOCKET, SO_RCVBUF, 
                   reinterpret_cast<const char*>(&buffer_size), sizeof(buffer_size)) == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_WARN("Failed to set receive buffer size. error={}", error);
    }

    return true;
}

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::cleanup_socket()
{
    if (socket_ != invalid_socket)
    {
        closesocket(socket_);
        socket_ = invalid_socket;
    }
}

template<typename DerivedT>
bool MulticastReceiverBase<DerivedT>::setup_multicast_options()
{
    // Enable address reuse
    if (config_.reuse_address)
    {
        DWORD reuse = 1;
        if (setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR,
                       reinterpret_cast<const char*>(&reuse), sizeof(reuse)) == SOCKET_ERROR)
        {
            int error = WSAGetLastError();
            LOG_WARN("Failed to set address reuse. error={}", error);
        }
    }

    // Bind to the multicast port
    sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(config_.port);
    bind_addr.sin_addr.s_addr = INADDR_ANY; // Bind to any interface

    if (bind(socket_, reinterpret_cast<const sockaddr*>(&bind_addr), sizeof(bind_addr)) == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_ERROR("Failed to bind socket to port {}. error={}", config_.port, error);
        return false;
    }

    return true;
}

template<typename DerivedT>
bool MulticastReceiverBase<DerivedT>::join_multicast_group()
{
    ip_mreq mreq{};
    
    // Set multicast group address
    int result = inet_pton(AF_INET, config_.multicast_address.c_str(), &mreq.imr_multiaddr);
    if (result != 1)
    {
        LOG_ERROR("Invalid multicast address: {}", config_.multicast_address);
        return false;
    }

    // Set interface address
    if (config_.interface_address == "0.0.0.0")
    {
        mreq.imr_interface.s_addr = INADDR_ANY;
    }
    else
    {
        result = inet_pton(AF_INET, config_.interface_address.c_str(), &mreq.imr_interface);
        if (result != 1)
        {
            LOG_WARN("Invalid interface address: {}, using any interface", config_.interface_address);
            mreq.imr_interface.s_addr = INADDR_ANY;
        }
    }

    // Join multicast group
    if (setsockopt(socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                   reinterpret_cast<const char*>(&mreq), sizeof(mreq)) == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        LOG_ERROR("Failed to join multicast group {}. error={}", config_.multicast_address, error);
        return false;
    }

    LOG_DEBUG("Joined multicast group {}", config_.multicast_address);
    return true;
}

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::leave_multicast_group()
{
    if (socket_ == invalid_socket)
        return;

    ip_mreq mreq{};
    
    // Set multicast group address
    int result = inet_pton(AF_INET, config_.multicast_address.c_str(), &mreq.imr_multiaddr);
    if (result == 1)
    {
        // Set interface address
        if (config_.interface_address == "0.0.0.0")
        {
            mreq.imr_interface.s_addr = INADDR_ANY;
        }
        else
        {
            inet_pton(AF_INET, config_.interface_address.c_str(), &mreq.imr_interface);
        }

        // Leave multicast group
        if (setsockopt(socket_, IPPROTO_IP, IP_DROP_MEMBERSHIP,
                       reinterpret_cast<const char*>(&mreq), sizeof(mreq)) == SOCKET_ERROR)
        {
            int error = WSAGetLastError();
            LOG_WARN("Failed to leave multicast group {}. error={}", config_.multicast_address, error);
        }
        else
        {
            LOG_DEBUG("Left multicast group {}", config_.multicast_address);
        }
    }
}

} // namespace slick::socket