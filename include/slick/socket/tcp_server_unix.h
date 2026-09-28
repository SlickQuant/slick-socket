// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if !defined(_WIN32) && !defined(_WIN64)

#include "tcp_server.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <csignal>
#include <algorithm>
#include <cstring>
#include <pthread.h>

#ifdef __APPLE__
#include <sys/event.h>
#include <sys/time.h>
#else
#include <sys/epoll.h>
#endif

namespace slick::socket
{

template<typename DerivedT>
inline TCPServerBase<DerivedT>::TCPServerBase(std::string name, const TCPServerConfig& config)
    : name_(std::move(name)), config_(config)
{
    // Ignore SIGPIPE to prevent crashes when writing to closed sockets
    std::signal(SIGPIPE, SIG_IGN);
}

template<typename DerivedT>
inline TCPServerBase<DerivedT>::~TCPServerBase()
{
    stop();
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

    LOG_INFO("Starting {}, lisening on: {}...", name_, config_.port);
    // Create server socket
    server_socket_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_ < 0)
    {
        LOG_ERROR("Failed to create server socket");
        return false;
    }

    // Set socket options
    if (config_.reuse_address)
    {
        int opt = 1;
        if (setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
        {
            LOG_WARN("Failed to set SO_REUSEADDR");
        }
    }

    // Make socket non-blocking for select() usage
    int flags = fcntl(server_socket_, F_GETFL, 0);
    if (flags < 0 || fcntl(server_socket_, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        LOG_WARN("Failed to make server socket non-blocking");
    }

    // Bind socket
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_socket_, (sockaddr*)&server_addr, sizeof(server_addr)) < 0)
    {
        LOG_ERROR("Failed to bind socket");
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }

    // Listen for connections
    if (listen(server_socket_, SOMAXCONN) < 0)
    {
        LOG_ERROR("Failed to listen on socket");
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }

    // Record the actual port (resolves port 0 to the OS-assigned port)
    socklen_t addr_len = sizeof(server_addr);
    if (getsockname(server_socket_, (sockaddr*)&server_addr, &addr_len) == 0)
    {
        config_.port = ntohs(server_addr.sin_port);
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
    if (server_socket_ >= 0)
    {
        close(server_socket_);
        server_socket_ = -1;
    }

    for (auto& [id, client] : clients_)
    {
        close(client.socket);
    }
    clients_.clear();
    socket_to_client_id_.clear();
    client_count_.store(0, std::memory_order_relaxed);

    if (epoll_fd_ >= 0)
    {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::send_data(int client_id, const std::vector<uint8_t>& data)
{
    auto it = clients_.find(client_id);
    if (it == clients_.end())
    {
        return false;
    }

    size_t total_sent = 0;
    size_t data_size = data.size();
    const uint8_t* buffer = data.data();

    // Keep sending until all data is sent
    while (total_sent < data_size)
    {
        ssize_t sent = send(it->second.socket, buffer + total_sent, data_size - total_sent, MSG_NOSIGNAL);
        if (sent < 0)
        {
            // Check for non-blocking specific errors
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // Socket buffer is full, retry immediately
                continue;
            }

            LOG_ERROR("Failed to send data to client {}: {}", client_id, std::strerror(errno));

            // Check if connection is broken
            if (errno == ECONNRESET || errno == EPIPE || errno == ENOTCONN)
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

template<typename DerivedT>
inline void TCPServerBase<DerivedT>::close_socket(SocketT socket)
{
#ifdef __APPLE__
    struct kevent ev;
    EV_SET(&ev, socket, EVFILT_READ, EV_DELETE, 0, 0, 0);
    kevent(epoll_fd_, &ev, 1, nullptr, 0, nullptr);
#else
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, socket, nullptr);
#endif
    socket_to_client_id_.erase(socket);
    close(socket);
}

template<typename DerivedT>
inline void TCPServerBase<DerivedT>::disconnect_client(int client_id)
{
    auto it = clients_.find(client_id);
    if (it != clients_.end())
    {
        remove_client(it);
    }
}

template<typename DerivedT>
void TCPServerBase<DerivedT>::server_loop()
{
    // Set CPU affinity if specified
    if (config_.cpu_affinity >= 0)
    {
#ifndef __APPLE__
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(config_.cpu_affinity, &cpuset);

        pthread_t thread = pthread_self();
        int result = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
        if (result != 0)
        {
            LOG_WARN("Failed to set CPU affinity to core {}: {}",
                             config_.cpu_affinity, std::strerror(result));
        }
        else
        {
            LOG_INFO("Server thread pinned to CPU core {}", config_.cpu_affinity);
        }
#else
        LOG_WARN("CPU affinity not supported on macOS");
#endif
    }

#ifdef __APPLE__
    // macOS: Use kqueue
    epoll_fd_ = kqueue();
    if (epoll_fd_ < 0)
    {
        LOG_ERROR("Failed to create kqueue instance: {}", std::strerror(errno));
        return;
    }

    // Add server socket to kqueue
    struct kevent ev;
    EV_SET(&ev, server_socket_, EVFILT_READ, EV_ADD, 0, 0, 0);
    if (kevent(epoll_fd_, &ev, 1, nullptr, 0, nullptr) < 0)
    {
        LOG_ERROR("Failed to add server socket to kqueue: {}", std::strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
        return;
    }

    const int MAX_EVENTS = 64;
    struct kevent events[MAX_EVENTS];
    std::vector<uint8_t> buffer(config_.receive_buffer_size);

    // Set timeout to 1us to allow checking running_ flag
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 1000; // 1us

    if (config_.cpu_affinity < 0)
    {
        timeout.tv_nsec = 1000000;  // 1ms
    }

    while (running_.load())
    {
        int num_events = kevent(epoll_fd_, nullptr, 0, events, MAX_EVENTS, &timeout);
        if (num_events < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            LOG_ERROR("kevent failed: {}", std::strerror(errno));
            break;
        }

        // Stop dispatching as soon as a callback calls stop()
        for (int i = 0; i < num_events && running_.load(std::memory_order_relaxed); i++)
        {
            int fd = static_cast<int>(events[i].ident);
            if (fd == server_socket_)
            {
                // New connection on server socket
                accept_new_client();
            }
            else
            {
                auto it = socket_to_client_id_.find(fd);
                if (it != socket_to_client_id_.end())
                {
                    handle_client_data(it->second, buffer);
                }
            }
        }
    }

    // The loop owns the sockets; release them here so a stop() issued from a callback also cleans up
    release_resources();
#else
    // Linux: Use epoll
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0)
    {
        LOG_ERROR("Failed to create epoll instance: {}", std::strerror(errno));
        return;
    }

    // Add server socket to epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = server_socket_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_socket_, &ev) < 0)
    {
        LOG_ERROR("Failed to add server socket to epoll: {}", std::strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
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

    while (running_.load())
    {
        int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout);
        if (num_events < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            LOG_ERROR("epoll_wait failed: {}", std::strerror(errno));
            break;
        }

        // Stop dispatching as soon as a callback calls stop()
        for (int i = 0; i < num_events && running_.load(std::memory_order_relaxed); i++)
        {
            if (events[i].data.fd == server_socket_)
            {
                // New connection on server socket
                accept_new_client();
            }
            else
            {
                auto it = socket_to_client_id_.find(events[i].data.fd);
                if (it != socket_to_client_id_.end())
                {
                    handle_client_data(it->second, buffer);
                }
            }
        }
    }

    // The loop owns the sockets; release them here so a stop() issued from a callback also cleans up
    release_resources();
#endif
}

template<typename DerivedT>
void TCPServerBase<DerivedT>::accept_new_client()
{
    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);

    int client_socket = accept(server_socket_, (sockaddr*)&client_addr, &addr_len);
    if (client_socket < 0)
    {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            LOG_ERROR("Failed to accept client");
        }
        return;
    }

    if (at_connection_limit())
    {
        LOG_WARN("Rejecting client: max_connections ({}) reached", config_.max_connections);
        close(client_socket);
        return;
    }

    // Make client socket non-blocking
    int flags = fcntl(client_socket, F_GETFL, 0);
    if (flags >= 0)
    {
        fcntl(client_socket, F_SETFL, flags | O_NONBLOCK);
    }

    // Add client socket to event system
#ifdef __APPLE__
    struct kevent ev;
    EV_SET(&ev, client_socket, EVFILT_READ, EV_ADD, 0, 0, 0);
    if (kevent(epoll_fd_, &ev, 1, nullptr, 0, nullptr) < 0)
    {
        LOG_ERROR("Failed to add client socket to kqueue: {}", std::strerror(errno));
        close(client_socket);
        return;
    }
#else
    struct epoll_event ev;
    ev.events = EPOLLIN;  // Level-triggered: data left over after one recv() is reported again
    ev.data.fd = client_socket;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_socket, &ev) < 0)
    {
        LOG_ERROR("Failed to add client socket to epoll: {}", std::strerror(errno));
        close(client_socket);
        return;
    }
#endif

    // Get client address
    char addr_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, addr_str, INET_ADDRSTRLEN);

    uint32_t client_id = next_client_id_.fetch_add(1);
    std::string client_address = addr_str;

    // Add client to maps
    clients_[client_id] = {client_socket, client_address};
    socket_to_client_id_[client_socket] = client_id;
    client_count_.store(clients_.size(), std::memory_order_relaxed);

    // Notify about new client
    derived().onClientConnected(client_id, client_address);
}

template<typename DerivedT>
void TCPServerBase<DerivedT>::handle_client_data(int client_id, std::vector<uint8_t>& buffer)
{
    auto it = clients_.find(client_id);
    if (it == clients_.end())
    {
        return;
    }

    ssize_t received = recv(it->second.socket, buffer.data(), buffer.size(), 0);

    if (received > 0)
    {
        // Process received data
        derived().onClientData(client_id, buffer.data(), received);
    }
    else if (received == 0)
    {
        // Client disconnected
        remove_client(it);
        // Notify about client disconnection
        derived().onClientDisconnected(client_id);
    }
    else
    {
        // Error
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            LOG_ERROR("Receive error for client ID={}", client_id);
            remove_client(it);
            derived().onClientDisconnected(client_id);
        }
    }
}

} // namespace slick::socket

#endif // !_WIN32 && !_WIN64
