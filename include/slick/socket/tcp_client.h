// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <slick/socket/logger.h>
#include <slick/socket/worker_thread.h>
#include <slick/socket/send_gate.h>
#include <vector>
#include <thread>
#include <string>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#endif

namespace slick::socket
{

struct TCPClientConfig
{
    std::string server_address = "localhost";   // IPv4 literal or hostname
    uint16_t server_port = 5000;
    int receive_buffer_size = 4096;
    std::chrono::milliseconds connection_timeout{30000};
    int cpu_affinity = -1;  // -1 means no affinity, otherwise specify CPU core index
};

template<typename DerivedT>
class TCPClientBase
{
public:
    explicit TCPClientBase(std::string name, const TCPClientConfig& config = TCPClientConfig());
    virtual ~TCPClientBase();

    // Neither copyable nor movable: the object owns sockets, and its worker thread holds `this`
    TCPClientBase(const TCPClientBase&) = delete;
    TCPClientBase& operator=(const TCPClientBase&) = delete;
    TCPClientBase(TCPClientBase&&) = delete;
    TCPClientBase& operator=(TCPClientBase&&) = delete;

    // connect()/disconnect() must not run concurrently with each other; call them from one thread at a
    // time. They replace the socket and client thread and are not synchronized against each other. The
    // one supported overlap is disconnect() from a callback while another thread calls disconnect(): on
    // the client thread, disconnect() only flags the shutdown. send_data() may run on any thread,
    // concurrently with both.
    // From the client's callbacks (the client thread), disconnect() is allowed but connect() always fails:
    // it must first join the previous connection's client thread, which cannot join itself. To reconnect
    // from onDisconnected(), signal another thread to call connect().
    bool connect();
    void disconnect();
    
    bool is_connected() const noexcept
    {
        return connected_.load(std::memory_order_relaxed);
    }

    // Safe to call from any number of threads, including concurrently with disconnect(): the socket is
    // never closed while a send is using it. Blocks until the data is sent or the connection is lost.
    // Concurrent calls each send their own data in order, but a large message can be split into partial
    // writes, so callers that rely on message boundaries must not send concurrently.
    // A broken connection makes it return false and ends the connection, as a server-side close does.
    bool send_data(const std::vector<uint8_t>& data);
    bool send_data(const std::string& data)
    {
        std::vector<uint8_t> buffer(data.begin(), data.end());
        return send_data(buffer);
    }

protected:
#if defined(_WIN32) || defined(_WIN64)
    using SocketT = SOCKET;
    static constexpr SocketT invalid_socket = INVALID_SOCKET;
#else
    using SocketT = int;
    static constexpr SocketT invalid_socket = -1;
#endif

    DerivedT& derived() { return static_cast<DerivedT&>(*this); }
    const DerivedT& derived() const { return static_cast<const DerivedT&>(*this); }

    void client_loop();

    // Final step of a successful connect(), shared by both platforms: reports the connection, then
    // starts the client thread. onConnected() runs first, on the calling thread, so the caller's
    // post-connect setup completes before any onData()/onDisconnected(); data the server sends
    // meanwhile waits in the socket. Returns connect()'s result.
    bool finish_connect();
    void handle_server_data(std::vector<uint8_t>& buffer);

    // Joins the client thread (unless called from it) and closes the socket.
    // Returns false when called from the client thread, in which case cleanup is deferred.
    bool release_connection();

    // Waits up to timeout_ms for `events` (POLLIN/POLLOUT) on the socket.
    // Returns false on timeout; true when ready or on error (reported by the following recv/send).
    bool wait_socket(short events, int timeout_ms) const;

    // How long a blocked receive/send waits before re-checking connected_
    static constexpr int poll_interval_ms = 1;

    // Resolves config_.server_address (IPv4 literal or hostname) to an IPv4 address
    bool resolve_server_address(in_addr& addr) const
    {
        // Fast path: numeric IPv4 address, no resolver round trip
        if (inet_pton(AF_INET, config_.server_address.c_str(), &addr) == 1)
        {
            return true;
        }

        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        addrinfo* result = nullptr;
        if (getaddrinfo(config_.server_address.c_str(), nullptr, &hints, &result) != 0 || result == nullptr)
        {
            return false;
        }

        addr = reinterpret_cast<const sockaddr_in*>(result->ai_addr)->sin_addr;
        freeaddrinfo(result);
        return true;
    }

    std::string name_;
    TCPClientConfig config_;
    std::atomic_bool connected_{false};
    std::atomic_bool destroying_{false};  // set by the base destructor to suppress callbacks
    std::thread client_thread_;
    detail::WorkerThreadId client_thread_id_;
    SocketT socket_ = invalid_socket;
    detail::SendGate send_gate_;  // keeps socket_ open while send_data() uses it from other threads
};

template<typename DerivedT>
inline bool TCPClientBase<DerivedT>::finish_connect()
{
    send_gate_.open();
    connected_.store(true, std::memory_order_release);
    derived().onConnected();

    // onConnected() may have changed the connection before the client thread exists
    if (client_thread_.joinable())
    {
        // It reconnected (disconnect() then connect()); that connect() started the client thread
        return connected_.load(std::memory_order_relaxed);
    }
    if (socket_ == invalid_socket)
    {
        // It called disconnect(), which closed the socket and reported onDisconnected()
        return false;
    }

    // Started even if a send in onConnected() already found the connection broken: the loop then
    // sees connected_ cleared, reports onDisconnected() and exits, as for any lost connection
    client_thread_ = std::thread(&TCPClientBase::client_loop, this);
    return connected_.load(std::memory_order_relaxed);
}

template<typename DerivedT>
inline void TCPClientBase<DerivedT>::disconnect()
{
    // The client thread may already have cleared connected_ (server closed the connection),
    // so the thread and socket are released regardless of the previous state.
    const bool was_connected = connected_.exchange(false, std::memory_order_acq_rel);
    if (was_connected)
    {
        LOG_INFO("Disconnecting from {}:{}...", config_.server_address, config_.server_port);
    }

    // Connected without a client thread only happens inside onConnected(), before connect() starts the
    // thread. No thread will report this disconnect then, so it is reported here.
    const bool report_here = was_connected && !client_thread_.joinable();

    if (release_connection() && was_connected)
    {
        LOG_INFO("Disconnected");
        if (report_here && !destroying_.load(std::memory_order_relaxed))
        {
            derived().onDisconnected();
        }
    }
}

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "tcp_client_win32.h"
#else
#include "tcp_client_unix.h"
#endif
