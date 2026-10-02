// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <cstddef>
#include <cstdint>
#include <algorithm>
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
    // Disconnects a client after this long with no traffic in either direction: nothing received and
    // no send progress. Reported through onClientDisconnected(). 0 (the default) disables it.
    std::chrono::milliseconds idle_timeout{0};
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

    // Neither copyable nor movable: the object owns sockets, and its worker thread holds `this`
    TCPServerBase(const TCPServerBase&) = delete;
    TCPServerBase& operator=(const TCPServerBase&) = delete;
    TCPServerBase(TCPServerBase&&) = delete;
    TCPServerBase& operator=(TCPServerBase&&) = delete;

    // Server control
    // start()/stop() must not run concurrently with each other; call them from one thread at a time.
    // They replace the socket and worker thread and are not synchronized against each other. The one
    // supported overlap is stop() from this object's own callback while another thread calls stop():
    // on the worker thread, stop() only flags the shutdown.
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

    // Closes the client's connection. Must be called on the server thread (i.e. from a server callback).
    // onClientDisconnected() fires once the current callback returns.
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

    using Clock = std::chrono::steady_clock;

    struct ClientInfo
    {
        SocketT socket;
        std::string address;
        Clock::time_point last_activity;  // last receive or send progress; only kept with idle_timeout on
        std::vector<uint8_t> pending;     // data waiting for the socket to become writable
        size_t pending_offset = 0;        // bytes of `pending` already sent

        bool has_pending() const noexcept { return pending_offset < pending.size(); }
        size_t pending_size() const noexcept { return pending.size() - pending_offset; }
    };
    using ClientMap = std::unordered_map<int, ClientInfo>;

    bool queue_pending(typename ClientMap::iterator it, const uint8_t* data, size_t size);
    void flush_pending(int client_id);

    // Adds an accepted, event-loop-registered socket to the connection maps and notifies the derived class
    void register_client(SocketT socket, std::string address)
    {
        const int client_id = next_client_id_.fetch_add(1, std::memory_order_relaxed);
        ClientInfo& client = clients_[client_id];
        client.socket = socket;
        client.address = address;
        client.last_activity = loop_now_;
        socket_to_client_id_[socket] = client_id;
        client_count_.store(clients_.size(), std::memory_order_relaxed);

        // Passes the local copy: the callback may disconnect the client, destroying its ClientInfo
        derived().onClientConnected(client_id, address);
    }

    bool idle_timeout_enabled() const noexcept
    {
        return config_.idle_timeout.count() > 0;
    }

    // Runs after the event wait returns, before dispatching. The clock is only read with idle_timeout on,
    // once per iteration, and every activity stamp in the iteration reuses it.
    void begin_iteration()
    {
        if (idle_timeout_enabled())
        {
            loop_now_ = Clock::now();
        }
    }

    // Runs after an iteration's events are dispatched
    void end_iteration()
    {
        if (idle_timeout_enabled() && loop_now_ >= next_idle_check_ && running_.load(std::memory_order_relaxed))
        {
            reap_idle_clients();
            after_event();
        }
    }

    void reap_idle_clients();

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

    // Closes the client's socket and removes it from the connection maps (server thread only).
    // Every removal goes through here and is reported once by notify_disconnected().
    // Returns the iterator following the removed client.
    typename ClientMap::iterator remove_client(typename ClientMap::iterator it)
    {
        close_socket(it->second.socket);
        disconnected_.push_back(it->first);
        auto next = clients_.erase(it);
        client_count_.store(clients_.size(), std::memory_order_relaxed);
        return next;
    }

    // Fires onClientDisconnected() for clients removed since the last call. Called by the server loop
    // after each event, so a send_data()/disconnect_client() issued from a callback never re-enters
    // user code (e.g. while it iterates its own client list).
    void notify_disconnected()
    {
        // Index loop: a callback may remove more clients, which are reported in the same pass
        for (size_t i = 0; i < disconnected_.size(); ++i)
        {
            const int client_id = disconnected_[i];  // copied: the callback may grow the vector
            derived().onClientDisconnected(client_id);
        }
        disconnected_.clear();
    }

    // Runs after every dispatched event
    void after_event()
    {
        if (!disconnected_.empty()) [[unlikely]]
        {
            notify_disconnected();
        }
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
    std::vector<int> disconnected_;  // removed clients awaiting onClientDisconnected() (server thread only)
    Clock::time_point loop_now_{};         // time the current loop iteration started (idle_timeout on only)
    Clock::time_point next_idle_check_{};  // earliest time reap_idle_clients() can find an idle client
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
        const SendStatus status = write_some(client.socket, data.data(), data.size(), sent);
        if (sent != 0)
        {
            client.last_activity = loop_now_;
        }
        switch (status)
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
    const size_t offset_before = client.pending_offset;
    const SendStatus status = write_some(client.socket, client.pending.data(), client.pending.size(), client.pending_offset);
    if (client.pending_offset != offset_before)
    {
        client.last_activity = loop_now_;
    }
    switch (status)
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
        return;
    }
}

template<typename DerivedT>
inline void TCPServerBase<DerivedT>::reap_idle_clients()
{
    const auto timeout = std::chrono::duration_cast<Clock::duration>(config_.idle_timeout);
    auto next_expiry = loop_now_ + timeout;
    for (auto it = clients_.begin(); it != clients_.end();)
    {
        const auto expiry = it->second.last_activity + timeout;
        if (expiry <= loop_now_)
        {
            LOG_INFO("Client {} idle for {} ms, disconnecting", it->first, config_.idle_timeout.count());
            it = remove_client(it);
        }
        else
        {
            next_expiry = (std::min)(next_expiry, expiry);
            ++it;
        }
    }

    // Sleep until the earliest survivor could expire, but rescan at most 8 times per timeout period,
    // so clients that keep pushing their expiry back cannot make the O(n) scan run every iteration
    next_idle_check_ = (std::max)(next_expiry, loop_now_ + timeout / 8);
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

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "tcp_server_win32.h"
#else
#include "tcp_server_unix.h"
#endif
