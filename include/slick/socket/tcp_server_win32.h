// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if defined(_WIN32) || defined(_WIN64)

#include "tcp_server.h"
#include <ws2tcpip.h>
#include <windows.h>
#include "wepoll.h"
#include <queue>
#include <algorithm>

#pragma comment(lib, "ws2_32.lib")

namespace slick::socket
{

template<typename DrivedT>
inline TCPServerBase<DrivedT>::TCPServerBase(std::string name, const TCPServerConfig& config)
    : name_(std::move(name)), config_(config)
{
    WSADATA wsa_data;
    int result = WSAStartup(MAKEWORD(2, 2), &wsa_data);
    if (result != 0)
    {
        throw std::runtime_error("WSAStartup failed: " + std::to_string(result));
    }
}

template<typename DrivedT>
inline TCPServerBase<DrivedT>::~TCPServerBase()
{
    stop();
    release_resources();
    WSACleanup();
}

template<typename DrivedT>
inline bool TCPServerBase<DrivedT>::start()
{
    if (running_.load(std::memory_order_relaxed))
    {
        return true;
    }

    LOG_INFO("Starting {}, lisening on: {}...", name_, config_.port);
    // Create server socket
    server_socket_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_socket_ == INVALID_SOCKET)
    {
        LOG_ERROR("Failed to create server socket");
        return false;
    }

    // Make server socket non-blocking
    u_long mode = 1; // non-blocking mode
    if (ioctlsocket(server_socket_, FIONBIO, &mode) != 0)
    {
        LOG_WARN("Failed to make server socket non-blocking");
    }

    // Set socket options
    if (config_.reuse_address)
    {
        int opt = 1;
        setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt));
    }

    // Bind socket
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_socket_, (sockaddr*)&server_addr, sizeof(server_addr)) == SOCKET_ERROR)
    {
        LOG_ERROR("Failed to bind socket");
        closesocket(server_socket_);
        server_socket_ = INVALID_SOCKET;
        return false;
    }

    // Listen for connections
    if (listen(server_socket_, SOMAXCONN) == SOCKET_ERROR)
    {
        LOG_ERROR("Failed to listen on socket");
        closesocket(server_socket_);
        server_socket_ = INVALID_SOCKET;
        return false;
    }

    // Record the actual port (resolves port 0 to the OS-assigned port)
    int addr_len = sizeof(server_addr);
    if (getsockname(server_socket_, (sockaddr*)&server_addr, &addr_len) == 0)
    {
        config_.port = ntohs(server_addr.sin_port);
    }

    running_.store(true, std::memory_order_release);

    // Start single-threaded server loop
    server_thread_ = std::thread(&TCPServerBase<DrivedT>::server_loop, this);

    LOG_INFO("{} started", name_);
    return true;
}

template<typename DrivedT>
inline void TCPServerBase<DrivedT>::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel))
    {
        return;
    }

    LOG_INFO("Stopping {}...", name_);

    // The server thread owns the sockets and connection maps while it runs,
    // so it must finish before anything is released
    if (server_thread_.joinable())
    {
        server_thread_.join();
    }

    release_resources();

    LOG_INFO("{} stopped", name_);
}

template<typename DrivedT>
inline void TCPServerBase<DrivedT>::release_resources()
{
    if (server_socket_ != INVALID_SOCKET)
    {
        closesocket(server_socket_);
        server_socket_ = INVALID_SOCKET;
    }

    for (auto& [client_id, client_info] : clients_)
    {
        closesocket(client_info.socket);
    }
    clients_.clear();
    socket_to_client_id_.clear();

    if (epoll_fd_ != nullptr)
    {
        epoll_close(epoll_fd_);
        epoll_fd_ = nullptr;
    }
}

template<typename DrivedT>
inline bool TCPServerBase<DrivedT>::send_data(int client_id, const std::vector<uint8_t>& data)
{
    auto it = clients_.find(client_id);
    if (it == clients_.end())
    {
        return false;
    }

    size_t total_sent = 0;
    size_t data_size = data.size();
    const char* buffer = reinterpret_cast<const char*>(data.data());

    // Keep sending until all data is sent
    while (total_sent < data_size)
    {
        int sent = send(it->second.socket, buffer + total_sent, static_cast<int>(data_size - total_sent), 0);
        if (sent == SOCKET_ERROR)
        {
            int error = WSAGetLastError();

            // Check for non-blocking specific errors
            if (error == WSAEWOULDBLOCK)
            {
                // Socket buffer is full, retry immediately
                continue;
            }

            LOG_ERROR("Failed to send data to client {}: error {}", client_id, error);

            // Check if connection is broken
            if (error == WSAECONNRESET || error == WSAECONNABORTED || error == WSAENOTCONN)
            {
                LOG_INFO("Connection lost during send to client {}, disconnecting", client_id);
                disconnect_client(client_id);
            }
            return false;
        }

        total_sent += sent;
        
        if (sent > 0 && total_sent < data_size)
        {
            LOG_TRACE("Partial send to client {}: sent {} bytes, {} remaining", 
                           client_id, sent, data_size - total_sent);
        }
    }

    LOG_TRACE("Successfully sent {} bytes to client {}", total_sent, client_id);
    return true;
}

template<typename DrivedT>
inline void TCPServerBase<DrivedT>::close_socket(SocketT socket)
{
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, socket, nullptr);
    socket_to_client_id_.erase(socket);
    closesocket(socket);
}

template<typename DrivedT>
inline void TCPServerBase<DrivedT>::disconnect_client(int client_id)
{
    auto it = clients_.find(client_id);
    if (it != clients_.end())
    {
        close_socket(it->second.socket);
        clients_.erase(it);
    }
}

template<typename DrivedT>
void TCPServerBase<DrivedT>::server_loop()
{
    // Set CPU affinity if specified
    if (config_.cpu_affinity >= 0)
    {
        DWORD_PTR mask = 1ULL << config_.cpu_affinity;
        HANDLE thread = GetCurrentThread();
        DWORD_PTR result = SetThreadAffinityMask(thread, mask);
        if (result == 0)
        {
            LOG_WARN("Failed to set CPU affinity to core {}: error {}", 
                             config_.cpu_affinity, GetLastError());
        }
        else
        {
            LOG_INFO("Server thread pinned to CPU core {}", config_.cpu_affinity);
        }
    }

    // Create epoll instance using wepoll
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == nullptr)
    {
        LOG_ERROR("Failed to create epoll instance");
        return;
    }

    // Add server socket to epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = (int)(intptr_t)server_socket_;  // Cast SOCKET to int for wepoll
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, (SOCKET)server_socket_, &ev) < 0)
    {
        LOG_ERROR("Failed to add server socket to epoll");
        epoll_close(epoll_fd_);
        epoll_fd_ = nullptr;
        return;
    }

    const int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];
    std::vector<uint8_t> buffer(config_.receive_buffer_size);

    int timeout = 0;
    if (config_.cpu_affinity < 0)
    {
        // CPU isn't pinned wait for 1 ms
        timeout = 1;
    }

    while (running_.load(std::memory_order_relaxed))
    {
        int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout);
        if (num_events < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            LOG_ERROR("epoll_wait failed: {}", WSAGetLastError());
            break;
        }

        for (int i = 0; i < num_events; i++)
        {
            SOCKET sock = (SOCKET)(intptr_t)events[i].data.fd;
            
            if (sock == server_socket_)
            {
                // New connection on server socket
                accept_new_client();
            }
            else
            {
                // Data from client socket - O(1) lookup using socket_to_client_id_ map
                auto it = socket_to_client_id_.find(sock);
                if (it != socket_to_client_id_.end())
                {
                    handle_client_data(it->second, buffer);
                }
            }
        }
    }

    // Clean up
    if (epoll_fd_ != nullptr)
    {
        epoll_close(epoll_fd_);
        epoll_fd_ = nullptr;
    }
}

template<typename DrivedT>
void TCPServerBase<DrivedT>::accept_new_client()
{
    sockaddr_in client_addr{};
    int addr_len = sizeof(client_addr);

    SOCKET client_socket = accept(server_socket_, (sockaddr*)&client_addr, &addr_len);
    if (client_socket == INVALID_SOCKET)
    {
        int error = WSAGetLastError();
        if (error != WSAEWOULDBLOCK && error != WSAENOTSOCK)
        {
            LOG_ERROR("Failed to accept client. error={}", error);
        }
        return;
    }

    if (at_connection_limit())
    {
        LOG_WARN("Rejecting client: max_connections ({}) reached", config_.max_connections);
        closesocket(client_socket);
        return;
    }

    // Make client socket non-blocking
    u_long mode = 1; // non-blocking mode
    if (ioctlsocket(client_socket, FIONBIO, &mode) != 0)
    {
        LOG_WARN("Failed to make client socket non-blocking");
        closesocket(client_socket);
        return;
    }

    // Add client socket to epoll
    struct epoll_event ev;
    // Read readiness only: an idle socket is always writable, so EPOLLOUT would wake the loop continuously
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.fd = (int)(intptr_t)client_socket;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_socket, &ev) < 0)
    {
        LOG_ERROR("Failed to add client socket to epoll: {}", WSAGetLastError());
        closesocket(client_socket);
        return;
    }

    // Get client address
    char addr_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, addr_str, INET_ADDRSTRLEN);

    int client_id = next_client_id_.fetch_add(1);
    std::string client_address = addr_str;

    // Add client to maps
    clients_[client_id] = {client_socket, client_address};
    socket_to_client_id_[client_socket] = client_id;

    // Notify about new client
    derived().onClientConnected(client_id, client_address);
}

template<typename DrivedT>
void TCPServerBase<DrivedT>::handle_client_data(int client_id, std::vector<uint8_t>& buffer)
{
    auto it = clients_.find(client_id);
    if (it == clients_.end())
    {
        return;
    }

    SOCKET socket = it->second.socket;
    int received = recv(socket, (char*)buffer.data(), (int)buffer.size(), 0);

    if (received > 0)
    {
        derived().onClientData(client_id, buffer.data(), received);
    }
    else if (received == 0)
    {
        // Client disconnected
        close_socket(socket);
        clients_.erase(it);
        // Notify about client disconnection
        derived().onClientDisconnected(client_id);
    }
    else
    {
        // Error
        int error = WSAGetLastError();
        if (error != WSAEWOULDBLOCK)
        {
            LOG_ERROR("Receive error for client ID={}", client_id);
            close_socket(socket);
            clients_.erase(it);
            derived().onClientDisconnected(client_id);
        }
    }
}

} // namespace slick::socket

#endif // _WIN32
