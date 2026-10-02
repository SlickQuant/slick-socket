// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if defined(_WIN32) || defined(_WIN64)

#include "tcp_client.h"
#include <stdexcept>
#include <utility>
#include <ws2tcpip.h>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")
namespace slick::socket
{

template<typename DerivedT>
inline TCPClientBase<DerivedT>::TCPClientBase(std::string name, const TCPClientConfig& config)
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
inline TCPClientBase<DerivedT>::~TCPClientBase()
{
    if (!detail::stopped_before_destroy(client_thread_, name_))
    {
        SLICK_SOCKET_ON_UNSAFE_DESTROY();
    }

    // The derived object is already destroyed here, so the loop must not call back into it
    destroying_.store(true, std::memory_order_relaxed);
    disconnect();
    WSACleanup();
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
        LOG_ERROR("Failed to create socket");
        return false;
    }

    // Make socket non-blocking
    u_long mode = 1; // non-blocking mode
    if (ioctlsocket(socket_, FIONBIO, &mode) != 0)
    {
        LOG_WARN("Failed to make socket non-blocking");
        closesocket(socket_);
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
        closesocket(socket_);
        socket_ = invalid_socket;
        return false;
    }

    LOG_INFO("{} attempting to connect to {}:{}", name_, config_.server_address, config_.server_port);

    // Attempt to connect (non-blocking)
    int result = ::connect(socket_, (sockaddr*)&server_addr, sizeof(server_addr));
    if (result == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        if (error != WSAEWOULDBLOCK && error != WSAEINPROGRESS)
        {
            LOG_WARN("Failed to connect to server: error {}", error);
            closesocket(socket_);
            socket_ = invalid_socket;
            return false;
        }
    }

    // Wait for connection to complete. Winsock reports a failed connect in the except set.
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(socket_, &write_fds);
    fd_set except_fds;
    FD_ZERO(&except_fds);
    FD_SET(socket_, &except_fds);

    struct timeval timeout;
    timeout.tv_sec = static_cast<long>(config_.connection_timeout.count() / 1000);
    timeout.tv_usec = static_cast<long>((config_.connection_timeout.count() % 1000) * 1000);

    result = select(static_cast<int>(socket_) + 1, nullptr, &write_fds, &except_fds, &timeout);
    if (result <= 0)
    {
        LOG_WARN("Connection timeout or failed");
        closesocket(socket_);
        socket_ = invalid_socket;
        return false;
    }

    // Check if connection was successful
    int error = 0;
    int len = sizeof(error);
    if (FD_ISSET(socket_, &except_fds) ||
        getsockopt(socket_, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &len) == SOCKET_ERROR ||
        error != 0)
    {
        LOG_WARN("Connection failed: error {}", error);
        closesocket(socket_);
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

    // Only close after the client thread is gone so it never reads a closed/reused socket
    // ...and after every send_data() on another thread has left it. Senders blocked on a full socket
    // re-check connected_ every poll_interval_ms, which is already false here, so this wait is short.
    send_gate_.close();
    if (socket_ != invalid_socket)
    {
        closesocket(socket_);
        socket_ = invalid_socket;
    }
    return true;
}

template<typename DerivedT>
inline bool TCPClientBase<DerivedT>::wait_socket(short events, int timeout_ms) const
{
    WSAPOLLFD pfd{socket_, events, 0};
    return WSAPoll(&pfd, 1, timeout_ms) != 0;
}

template<typename DerivedT>
inline void TCPClientBase<DerivedT>::client_loop()
{
    detail::WorkerThreadId::Scope worker_scope(client_thread_id_);

    LOG_DEBUG("Client loop started");

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
            LOG_INFO("Client thread pinned to CPU core {}", config_.cpu_affinity);
        }
    }

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
        int received = recv(socket_, (char*)buffer.data(), (int)buffer.size(), 0);
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
            int error = WSAGetLastError();
            if (error != WSAEWOULDBLOCK && error != WSAEINPROGRESS)
            {
                LOG_ERROR("Receive error: {}", error);
                connected_.store(false, std::memory_order_release);
                break;
            }
        }

        if (busy_poll)
        {
            std::this_thread::yield();
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
    const char* buffer = reinterpret_cast<const char*>(data.data());

    // Keep sending until all data is sent
    while (total_sent < data_size)
    {
        int sent = send(socket_, buffer + total_sent, static_cast<int>(data_size - total_sent), 0);
        if (sent == SOCKET_ERROR)
        {
            int error = WSAGetLastError();

            // Check for non-blocking specific errors
            if (error == WSAEWOULDBLOCK || error == WSAEINTR)
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

            LOG_ERROR("Failed to send data: error {}", error);

            // Check if connection is broken
            if (error == WSAECONNRESET || error == WSAECONNABORTED || error == WSAENOTCONN)
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

#endif // defined(_WIN32) || defined(_WIN64)
