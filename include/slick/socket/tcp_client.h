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

#include <slick/socket/logger.h>
#include <slick/socket/worker_thread.h>
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

    // Delete copy operations
    TCPClientBase(const TCPClientBase&) = delete;
    TCPClientBase& operator=(const TCPClientBase&) = delete;

    // Move operations
    TCPClientBase(TCPClientBase&& other) noexcept = default;
    TCPClientBase& operator=(TCPClientBase&& other) noexcept = default;

    bool connect();
    void disconnect();
    
    bool is_connected() const noexcept
    {
        return connected_.load(std::memory_order_relaxed);
    }

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
};

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "tcp_client_win32.h"
#else
#include "tcp_client_unix.h"
#endif
