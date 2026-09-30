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
    if (!detail::stopped_before_destroy(server_thread_, name_))
    {
        SLICK_SOCKET_ON_UNSAFE_DESTROY();
    }
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

    // Set up the event loop here so a failure is reported by start() rather than a dead server thread
    if (!create_event_loop())
    {
        close(server_socket_);
        server_socket_ = -1;
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
    disconnected_.clear();
    client_count_.store(0, std::memory_order_relaxed);

    if (epoll_fd_ >= 0)
    {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

template<typename DerivedT>
inline auto TCPServerBase<DerivedT>::write_some(SocketT socket, const uint8_t* data, size_t size, size_t& sent) -> SendStatus
{
    while (sent < size)
    {
        ssize_t result = ::send(socket, data + sent, size - sent, MSG_NOSIGNAL);
        if (result >= 0)
        {
            sent += static_cast<size_t>(result);
            continue;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return SendStatus::would_block;
        }
        LOG_ERROR("Failed to send data: {}", std::strerror(errno));
        return SendStatus::failed;
    }
    return SendStatus::complete;
}

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::set_write_interest(SocketT socket, bool enable)
{
#ifdef __APPLE__
    struct kevent ev;
    EV_SET(&ev, socket, EVFILT_WRITE, enable ? EV_ADD : EV_DELETE, 0, 0, 0);
    return kevent(epoll_fd_, &ev, 1, nullptr, 0, nullptr) == 0;
#else
    struct epoll_event ev{};
    ev.events = EPOLLIN | (enable ? EPOLLOUT : 0);
    ev.data.fd = socket;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, socket, &ev) == 0;
#endif
}

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::create_event_loop()
{
#ifdef __APPLE__
    // macOS: Use kqueue
    epoll_fd_ = kqueue();
    if (epoll_fd_ < 0)
    {
        LOG_ERROR("Failed to create kqueue instance: {}", std::strerror(errno));
        return false;
    }

    struct kevent ev;
    EV_SET(&ev, server_socket_, EVFILT_READ, EV_ADD, 0, 0, 0);
    if (kevent(epoll_fd_, &ev, 1, nullptr, 0, nullptr) < 0)
    {
        LOG_ERROR("Failed to add server socket to kqueue: {}", std::strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }
#else
    // Linux: Use epoll
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0)
    {
        LOG_ERROR("Failed to create epoll instance: {}", std::strerror(errno));
        return false;
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_socket_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_socket_, &ev) < 0)
    {
        LOG_ERROR("Failed to add server socket to epoll: {}", std::strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }
#endif
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
void TCPServerBase<DerivedT>::server_loop()
{
    detail::WorkerThreadId::Scope worker_scope(server_thread_id_);

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

        begin_iteration();

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
                const bool writable = events[i].filter == EVFILT_WRITE;
                dispatch_client_event(fd, writable, !writable, buffer);
            }
            after_event();
        }

        end_iteration();
    }

    // The loop owns the sockets; release them here so a stop() issued from a callback also cleans up.
    // Also covers an event-loop failure, so is_running() does not report a dead server.
    running_.store(false, std::memory_order_release);
    release_resources();
#else
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

        begin_iteration();

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
                const uint32_t flags = events[i].events;
                dispatch_client_event(events[i].data.fd, (flags & EPOLLOUT) != 0, (flags & ~EPOLLOUT) != 0, buffer);
            }
            after_event();
        }

        end_iteration();
    }

    // The loop owns the sockets; release them here so a stop() issued from a callback also cleans up.
    // Also covers an event-loop failure, so is_running() does not report a dead server.
    running_.store(false, std::memory_order_release);
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

    ssize_t received = recv(it->second.socket, buffer.data(), buffer.size(), 0);

    if (received > 0)
    {
        it->second.last_activity = loop_now_;  // before the callback, which may remove the client
        // Process received data
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
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            LOG_ERROR("Receive error for client ID={}", client_id);
            remove_client(it);
        }
    }
}

} // namespace slick::socket

#endif // !_WIN32 && !_WIN64
