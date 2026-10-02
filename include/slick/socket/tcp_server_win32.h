// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if defined(_WIN32) || defined(_WIN64)

#include "tcp_server.h"
#include <cerrno>
#include <stdexcept>
#include <utility>
#include <ws2tcpip.h>
#include <windows.h>
#include "wepoll.h"
#include <queue>
#include <algorithm>

#pragma comment(lib, "ws2_32.lib")

namespace slick::socket
{

template<typename DerivedT>
inline TCPServerBase<DerivedT>::TCPServerBase(std::string name, const TCPServerConfig& config)
    : name_(std::move(name)), config_(config)
{
    WSADATA wsa_data;
    int result = WSAStartup(MAKEWORD(2, 2), &wsa_data);
    if (result != 0)
    {
        throw std::runtime_error("WSAStartup failed: " + std::to_string(result));
    }
}

template<typename DerivedT>
inline TCPServerBase<DerivedT>::~TCPServerBase()
{
    if (!detail::stopped_before_destroy(server_thread_, name_))
    {
        SLICK_SOCKET_ON_UNSAFE_DESTROY();
    }
    stop();
    WSACleanup();
}

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::start()
{
    if (running_.load(std::memory_order_relaxed))
    {
        return true;
    }

    // Reap a server thread that was stopped from within a callback
    if (!join_server_thread())
    {
        LOG_ERROR("Cannot restart {} from within a server callback", name_);
        return false;
    }

    LOG_INFO("Starting {}, listening on: {}...", name_, config_.port);
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

    // Set up the event loop here so a failure is reported by start() rather than a dead server thread
    if (!create_event_loop())
    {
        closesocket(server_socket_);
        server_socket_ = INVALID_SOCKET;
        return false;
    }

    running_.store(true, std::memory_order_release);

    // Start single-threaded server loop
    server_thread_ = std::thread(&TCPServerBase<DerivedT>::server_loop, this);

    LOG_INFO("{} started", name_);
    return true;
}

template<typename DerivedT>
inline void TCPServerBase<DerivedT>::release_resources()
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
    disconnected_.clear();
    client_count_.store(0, std::memory_order_relaxed);

    if (epoll_fd_ != nullptr)
    {
        epoll_close(epoll_fd_);
        epoll_fd_ = nullptr;
    }
}

template<typename DerivedT>
inline auto TCPServerBase<DerivedT>::write_some(SocketT socket, const uint8_t* data, size_t size, size_t& sent) -> SendStatus
{
    while (sent < size)
    {
        int result = ::send(socket, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
        if (result != SOCKET_ERROR)
        {
            sent += static_cast<size_t>(result);
            continue;
        }
        int error = WSAGetLastError();
        if (error == WSAEINTR)
        {
            continue;
        }
        if (error == WSAEWOULDBLOCK)
        {
            return SendStatus::would_block;
        }
        LOG_ERROR("Failed to send data: error {}", error);
        return SendStatus::failed;
    }
    return SendStatus::complete;
}

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::set_write_interest(SocketT socket, bool enable)
{
    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP | (enable ? static_cast<uint32_t>(EPOLLOUT) : 0u);
    ev.data.sock = socket;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, socket, &ev) == 0;
}

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::create_event_loop()
{
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == nullptr)
    {
        LOG_ERROR("Failed to create epoll instance");
        return false;
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.sock = server_socket_;  // Full-width SOCKET; data.fd would truncate 64-bit handles
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_socket_, &ev) < 0)
    {
        LOG_ERROR("Failed to add server socket to epoll");
        epoll_close(epoll_fd_);
        epoll_fd_ = nullptr;
        return false;
    }
    return true;
}

template<typename DerivedT>
inline void TCPServerBase<DerivedT>::close_socket(SocketT socket)
{
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, socket, nullptr);
    socket_to_client_id_.erase(socket);
    closesocket(socket);
}

template<typename DerivedT>
void TCPServerBase<DerivedT>::server_loop()
{
    detail::WorkerThreadId::Scope worker_scope(server_thread_id_);

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

        begin_iteration();

        // Stop dispatching as soon as a callback calls stop()
        for (int i = 0; i < num_events && running_.load(std::memory_order_relaxed); i++)
        {
            SOCKET sock = events[i].data.sock;

            if (sock == server_socket_)
            {
                // New connection on server socket
                accept_new_client();
            }
            else
            {
                const uint32_t flags = events[i].events;
                dispatch_client_event(sock, (flags & EPOLLOUT) != 0, (flags & ~EPOLLOUT) != 0, buffer);
            }
            after_event();
        }

        end_iteration();
    }

    // The loop owns the sockets; release them here so a stop() issued from a callback also cleans up.
    // Also covers an event-loop failure, so is_running() does not report a dead server.
    running_.store(false, std::memory_order_release);
    release_resources();
}

template<typename DerivedT>
void TCPServerBase<DerivedT>::accept_new_client()
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
    ev.data.sock = client_socket;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_socket, &ev) < 0)
    {
        LOG_ERROR("Failed to add client socket to epoll: {}", WSAGetLastError());
        closesocket(client_socket);
        return;
    }

    // Get client address
    char addr_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, addr_str, INET_ADDRSTRLEN);

    register_client(client_socket, addr_str);
}

template<typename DerivedT>
void TCPServerBase<DerivedT>::handle_client_data(int client_id, std::vector<uint8_t>& buffer)
{
    auto it = clients_.find(client_id);
    if (it == clients_.end())
    {
        return;
    }

    int received = recv(it->second.socket, (char*)buffer.data(), (int)buffer.size(), 0);

    if (received > 0)
    {
        it->second.last_activity = loop_now_;  // before the callback, which may remove the client
        derived().onClientData(client_id, buffer.data(), received);
    }
    else if (received == 0)
    {
        // Client disconnected
        remove_client(it);
    }
    else
    {
        // Error
        int error = WSAGetLastError();
        if (error != WSAEWOULDBLOCK)
        {
            LOG_ERROR("Receive error for client ID={}", client_id);
            remove_client(it);
        }
    }
}

} // namespace slick::socket

#endif // _WIN32
