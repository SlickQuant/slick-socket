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
#include <slick/socket/worker_thread.h>

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
    // Per-client cap on data queued while the peer is not reading. send_data() rejects a message
    // (without sending any of it) if the queue could exceed this, so a single message larger than
    // this is always rejected. 0 means unlimited.
    size_t max_pending_send_bytes = 16 * 1024 * 1024;
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

    // Send data to client. Must be called on the server thread (i.e. from a server callback).
    // Never blocks: data the socket cannot take right away is queued and flushed when it becomes writable.
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

    // Creates the event loop handle and registers the listening socket
    bool create_event_loop();

    enum class SendStatus { complete, would_block, failed };

    // Sends data[sent, size) until done or the socket would block; `sent` is advanced
    SendStatus write_some(SocketT socket, const uint8_t* data, size_t size, size_t& sent);

    // Enables/disables writability notifications for a client socket
    bool set_write_interest(SocketT socket, bool enable);

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
        return detail::join_unless_current(server_thread_, server_thread_id_);
    }

protected:

    struct ClientInfo
    {
        SocketT socket;
        std::string address;
        std::vector<uint8_t> pending;   // data waiting for the socket to become writable
        size_t pending_offset = 0;      // bytes of `pending` already sent

        bool has_pending() const noexcept { return pending_offset < pending.size(); }
        size_t pending_size() const noexcept { return pending.size() - pending_offset; }
    };
    using ClientMap = std::unordered_map<int, ClientInfo>;

    bool queue_pending(typename ClientMap::iterator it, const uint8_t* data, size_t size);
    void flush_pending(int client_id);

    // Routes an event-loop notification for a client socket (server thread only)
    void dispatch_client_event(SocketT socket, bool writable, bool readable, std::vector<uint8_t>& buffer)
    {
        auto it = socket_to_client_id_.find(socket);
        if (it == socket_to_client_id_.end())
        {
            return;
        }

        const int client_id = it->second;  // copied: flushing may remove the client
        if (writable)
        {
            flush_pending(client_id);
        }
        if (readable)
        {
            handle_client_data(client_id, buffer);
        }
    }

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
    detail::WorkerThreadId server_thread_id_;
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

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::send_data(int client_id, const std::vector<uint8_t>& data)
{
    auto it = clients_.find(client_id);
    if (it == clients_.end())
    {
        return false;
    }

    ClientInfo& client = it->second;

    // Checked before the direct write: once the kernel takes part of a message, the rest must be
    // queued whatever its size, so admission can only be all-or-nothing up front
    if (config_.max_pending_send_bytes > 0 &&
        client.pending_size() + data.size() > config_.max_pending_send_bytes)
    {
        LOG_WARN("Send queue for client {} is full ({} bytes pending), dropping {} bytes",
                 client_id, client.pending_size(), data.size());
        return false;
    }

    size_t sent = 0;
    if (!client.has_pending())
    {
        // Nothing queued, so ordering allows writing straight to the socket
        switch (write_some(client.socket, data.data(), data.size(), sent))
        {
        case SendStatus::complete:
            LOG_TRACE("Successfully sent {} bytes to client {}", sent, client_id);
            return true;
        case SendStatus::failed:
            LOG_INFO("Connection lost during send to client {}, disconnecting", client_id);
            remove_client(it);
            return false;
        case SendStatus::would_block:
            break;
        }
    }

    // The peer is not keeping up: queue the rest instead of spinning on the event thread
    LOG_TRACE("Queued {} bytes for client {}", data.size() - sent, client_id);
    return queue_pending(it, data.data() + sent, data.size() - sent);
}

template<typename DerivedT>
inline bool TCPServerBase<DerivedT>::queue_pending(typename ClientMap::iterator it, const uint8_t* data, size_t size)
{
    ClientInfo& client = it->second;
    const bool was_empty = !client.has_pending();
    if (was_empty)
    {
        client.pending.clear();
        client.pending_offset = 0;
    }
    else if (client.pending_offset > client.pending.size() / 2)
    {
        // Drop the already-sent prefix before growing the buffer
        client.pending.erase(client.pending.begin(), client.pending.begin() + client.pending_offset);
        client.pending_offset = 0;
    }
    client.pending.insert(client.pending.end(), data, data + size);

    if (was_empty && !set_write_interest(client.socket, true))
    {
        LOG_ERROR("Failed to watch client {} for writability, disconnecting", it->first);
        remove_client(it);
        return false;
    }
    return true;
}

template<typename DerivedT>
inline void TCPServerBase<DerivedT>::flush_pending(int client_id)
{
    auto it = clients_.find(client_id);
    if (it == clients_.end())
    {
        return;
    }

    ClientInfo& client = it->second;
    switch (write_some(client.socket, client.pending.data(), client.pending.size(), client.pending_offset))
    {
    case SendStatus::would_block:
        return;
    case SendStatus::complete:
        client.pending.clear();
        client.pending_offset = 0;
        set_write_interest(client.socket, false);
        return;
    case SendStatus::failed:
        remove_client(it);
        derived().onClientDisconnected(client_id);
        return;
    }
}

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "tcp_server_win32.h"
#else
#include "tcp_server_unix.h"
#endif
