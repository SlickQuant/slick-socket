// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <functional>
#include <memory>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <string>
#include <slick/socket/logger.h>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#endif

namespace slick::socket
{

struct TCPServerConfig
{
    uint16_t port = 5000;
    int max_connections = 100;  // connections beyond this are accepted and closed immediately; <= 0 means unlimited
    bool reuse_address = true;
    int receive_buffer_size = 4096;
    std::chrono::milliseconds connection_timeout{30000};
    int cpu_affinity = -1;  // -1 means no affinity, otherwise specify CPU core index
};

template<typename DerivedT>
class TCPServerBase
{
public:
    explicit TCPServerBase(std::string name, const TCPServerConfig& config = TCPServerConfig());
    virtual ~TCPServerBase();

    // Delete copy operations
    TCPServerBase(const TCPServerBase&) = delete;
    TCPServerBase& operator=(const TCPServerBase&) = delete;

    // Move operations
    TCPServerBase(TCPServerBase&& other) noexcept = default;
    TCPServerBase& operator=(TCPServerBase&& other) noexcept = default;

    // Server control
    bool start();
    void stop();

    bool is_running() const noexcept
    {
        return running_.load(std::memory_order_relaxed);
    }

    // Listening port. When configured with port 0, this is the OS-assigned port after start().
    uint16_t port() const noexcept
    {
        return config_.port;
    }

protected:
    DerivedT& derived() { return static_cast<DerivedT&>(*this); }
    const DerivedT& derived() const { return static_cast<const DerivedT&>(*this); }

    void server_loop();
    void accept_new_client();
    void handle_client_data(int client_id, std::vector<uint8_t>& buffer);

    // Send data to client
    bool send_data(int client_id, const std::vector<uint8_t>& data);
    bool send_data(int client_id, const std::string& data)
    {
        std::vector<uint8_t> buffer(data.begin(), data.end());
        return send_data(client_id, buffer);
    }

    // Connection management
    void disconnect_client(int client_id);

    // Safe to call from any thread: the count is maintained by the server thread
    size_t get_connected_client_count() const noexcept
    {
        return client_count_.load(std::memory_order_relaxed);
    }

#if defined(_WIN32) || defined(_WIN64)
    using SocketT = SOCKET;
    static constexpr SocketT invalid_socket = INVALID_SOCKET;
#else
    using SocketT = int;
    static constexpr SocketT invalid_socket = -1;
#endif

    void close_socket(SocketT socket);

    // Closes the listening socket, all client sockets and the event loop handle.
    // Must only be called while the server thread is not running.
    void release_resources();

    bool at_connection_limit() const noexcept
    {
        return config_.max_connections > 0 && clients_.size() >= static_cast<size_t>(config_.max_connections);
    }

    // Joins the server thread unless called from it (i.e. from a server callback).
    // Returns false in that case; the loop exits once the callback returns and releases resources itself.
    bool join_server_thread()
    {
        if (server_thread_.joinable())
        {
            if (server_thread_.get_id() == std::this_thread::get_id())
            {
                return false;
            }
            server_thread_.join();
        }
        return true;
    }

protected:

    struct ClientInfo
    {
        SocketT socket;
        std::string address;
    };
    using ClientMap = std::unordered_map<int, ClientInfo>;

    // Closes the client's socket and removes it from the connection maps (server thread only)
    void remove_client(typename ClientMap::iterator it)
    {
        close_socket(it->second.socket);
        clients_.erase(it);
        client_count_.store(clients_.size(), std::memory_order_relaxed);
    }

    std::string name_;
    TCPServerConfig config_;
    std::atomic_bool running_{false};

    std::thread server_thread_;
    SocketT server_socket_ = invalid_socket;

#if !defined(_WIN32) && !defined(_WIN64)
    int epoll_fd_ = -1;  // epoll file descriptor for Unix/Linux
#else
    HANDLE epoll_fd_ = nullptr;  // wepoll handle for Windows (epoll-like API)
#endif

    ClientMap clients_;
    std::unordered_map<SocketT, int> socket_to_client_id_;
    std::atomic<size_t> client_count_{0};  // mirrors clients_.size() for readers on other threads
    std::atomic<int> next_client_id_{1};
};

template<typename DerivedT>
inline void TCPServerBase<DerivedT>::stop()
{
    const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running)
    {
        LOG_INFO("Stopping {}...", name_);
    }

    // The server thread owns the sockets and connection maps while it runs,
    // so it must finish before anything is released
    if (!join_server_thread())
    {
        // Called from a server callback; the loop exits after it returns and cleans up
        return;
    }

    release_resources();

    if (was_running)
    {
        LOG_INFO("{} stopped", name_);
    }
}

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "tcp_server_win32.h"
#else
#include "tcp_server_unix.h"
#endif
