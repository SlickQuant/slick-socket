// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if !defined(_WIN32) && !defined(_WIN64)

#include "tcp_client.h"
#include "socket_options_unix.h"
#include <cerrno>
#include <utility>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <pthread.h>
#include <poll.h>

namespace slick::socket
{

template<typename DerivedT>
inline TCPClientBase<DerivedT>::TCPClientBase(std::string name, const TCPClientConfig& config)
    : name_(std::move(name)),config_(config)
{
}

template<typename DerivedT>
inline TCPClientBase<DerivedT>::~TCPClientBase()
{
    if (!detail::stopped_before_destroy(client_thread_, name_))
    {
        SLICK_SOCKET_ON_UNSAFE_DESTROY();
    }

    // The derived object is already destroyed here, so the loop must not call back into it
    destroying_.store(true, std::memory_order_relaxed);
    disconnect();
}

template<typename DerivedT>
inline bool TCPClientBase<DerivedT>::connect()
{
    if (connected_.load(std::memory_order_relaxed))
    {
        return true;
    }

    // Reap a previous connection that ended on its own (e.g. server closed it)
    if (!release_connection())
    {
        LOG_ERROR("Cannot reconnect from within the client thread");
        return false;
    }

    // Create socket
    socket_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket_ == invalid_socket)
    {
        LOG_ERROR("Failed to create socket: {}", std::strerror(errno));
        return false;
    }
    detail::suppress_sigpipe(socket_);

    // Make socket non-blocking
    int flags = fcntl(socket_, F_GETFL, 0);
    if (flags < 0 || fcntl(socket_, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        LOG_WARN("Failed to make socket non-blocking: {}", std::strerror(errno));
        close(socket_);
        socket_ = invalid_socket;
        return false;
    }

    // Set up server address
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.server_port);

    // Resolve server address
    if (!resolve_server_address(server_addr.sin_addr))
    {
        LOG_ERROR("Failed to resolve server address: {}", config_.server_address);
        close(socket_);
        socket_ = invalid_socket;
        return false;
    }

    LOG_INFO("Attempting to connect to {}:{}", config_.server_address, config_.server_port);

    // Attempt to connect (non-blocking)
    int result = ::connect(socket_, (sockaddr*)&server_addr, sizeof(server_addr));
    if (result < 0 && errno != EINPROGRESS)
    {
        LOG_WARN("Failed to connect to server: {}", std::strerror(errno));
        close(socket_);
        socket_ = invalid_socket;
        return false;
    }

    // Wait for connection to complete
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(socket_, &write_fds);

    struct timeval timeout;
    timeout.tv_sec = config_.connection_timeout.count() / 1000;
    timeout.tv_usec = (config_.connection_timeout.count() % 1000) * 1000;

    result = select(socket_ + 1, nullptr, &write_fds, nullptr, &timeout);
    if (result <= 0)
    {
        LOG_WARN("Connection timeout or failed");
        close(socket_);
        socket_ = invalid_socket;
        return false;
    }

    // Check if connection was successful
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(socket_, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0)
    {
        LOG_WARN("Connection failed: {}", std::strerror(error));
        close(socket_);
        socket_ = invalid_socket;
        return false;
    }

    return finish_connect();
}

template<typename DerivedT>
inline bool TCPClientBase<DerivedT>::release_connection()
{
    if (!detail::join_unless_current(client_thread_, client_thread_id_))
    {
        // Called from a callback; the loop exits on its own and is joined later
        return false;
    }

    // Only close after the client thread is gone so it never reads a closed/reused descriptor
    // ...and after every send_data() on another thread has left it. Senders blocked on a full socket
    // re-check connected_ every poll_interval_ms, which is already false here, so this wait is short.
    send_gate_.close();
    if (socket_ != invalid_socket)
    {
        close(socket_);
        socket_ = invalid_socket;
    }
    return true;
}

template<typename DerivedT>
inline bool TCPClientBase<DerivedT>::wait_socket(short events, int timeout_ms) const
{
    pollfd pfd{socket_, events, 0};
    return ::poll(&pfd, 1, timeout_ms) != 0;
}

template<typename DerivedT>
inline void TCPClientBase<DerivedT>::client_loop()
{
    detail::WorkerThreadId::Scope worker_scope(client_thread_id_);

    LOG_DEBUG("Client loop started");

    // Set CPU affinity if specified
#ifndef __APPLE__
    if (config_.cpu_affinity >= 0)
    {
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
            LOG_INFO("Client thread pinned to CPU core {}", config_.cpu_affinity);
        }
    }
#else
    if (config_.cpu_affinity >= 0)
    {
        LOG_WARN("CPU affinity not supported on macOS");
    }
#endif

    // Connection established - handle server communication
    std::vector<uint8_t> buffer(config_.receive_buffer_size);

    // Pinned to a core: spin on recv() for the lowest latency. Otherwise block in poll() until data
    // arrives; the timeout only bounds how long disconnect() waits for this loop to notice.
    const bool busy_poll = config_.cpu_affinity >= 0;

    while (connected_.load(std::memory_order_relaxed))
    {
        if (!busy_poll && !wait_socket(POLLIN, poll_interval_ms))
        {
            continue;
        }

        // Check for incoming data (non-blocking)
        ssize_t received = recv(socket_, (char*)buffer.data(), buffer.size(), 0);

        if (received > 0)
        {
            // Process received data
            derived().onData(buffer.data(), received);
            continue;
        }
        else if (received == 0)
        {
            // Server closed connection
            LOG_INFO("Server closed connection");
            connected_.store(false, std::memory_order_release);
            break;
        }
        else
        {
            // Error or would block
            if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                LOG_ERROR("Receive error: {}", std::strerror(errno));
                connected_.store(false, std::memory_order_release);
                break;
            }
        }
    }

    // The acquire load pairs with the exchange in disconnect(), making destroying_ visible
    if (!connected_.load(std::memory_order_acquire) && !destroying_.load(std::memory_order_relaxed))
    {
        derived().onDisconnected();
    }

    // The socket is closed by release_connection() once this thread has been joined
    LOG_DEBUG("Client loop ended");
}

template<typename DerivedT>
inline void TCPClientBase<DerivedT>::handle_server_data(std::vector<uint8_t>& buffer)
{
    // Default implementation - derived classes should override this
    LOG_DEBUG("Received {} bytes from server", buffer.size());
}

template<typename DerivedT>
inline bool TCPClientBase<DerivedT>::send_data(const std::vector<uint8_t>& data)
{
    // Holds the socket open for the duration of the send, even if another thread disconnects
    detail::SendGate::Pass pass(send_gate_);
    if (!pass || !connected_.load(std::memory_order_relaxed))
    {
        LOG_WARN("Cannot send data: client not connected");
        return false;
    }

    if (data.empty())
    {
        LOG_WARN("Cannot send empty data");
        return false;
    }

    size_t total_sent = 0;
    size_t data_size = data.size();
    const uint8_t* buffer = data.data();

    // Keep sending until all data is sent
    while (total_sent < data_size)
    {
        ssize_t sent = send(socket_, buffer + total_sent, data_size - total_sent, MSG_NOSIGNAL);
        if (sent < 0)
        {
            // Check for non-blocking specific errors
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            {
                // Socket buffer is full: block until the server drains it instead of spinning
                if (!connected_.load(std::memory_order_relaxed))
                {
                    LOG_WARN("Connection closed while sending");
                    return false;
                }
                wait_socket(POLLOUT, poll_interval_ms);
                continue;
            }

            LOG_ERROR("Failed to send data: {}", std::strerror(errno));

            // Check if connection is broken
            if (errno == ECONNRESET || errno == EPIPE || errno == ENOTCONN)
            {
                // Ends the connection the way a server-side close does: the client thread reports
                // onDisconnected() and exits, and the next disconnect()/connect() releases the socket.
                // Joining or closing here instead would race a disconnect() on another thread.
                LOG_INFO("Connection lost during send");
                connected_.store(false, std::memory_order_release);
            }
            return false;
        }

        total_sent += sent;
        
        if (sent > 0 && total_sent < data_size)
        {
            LOG_TRACE("Partial send: sent {} bytes, {} remaining", sent, data_size - total_sent);
        }
    }

    LOG_TRACE("Successfully sent {} bytes to server", total_sent);
    return true;
}

} // namespace slick::socket

#endif // !defined(_WIN32) && !defined(_WIN64)
