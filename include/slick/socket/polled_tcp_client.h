// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <slick/socket/logger.h>
#include <slick/socket/polled_socket.h>
#include <span>
#include <string>
#include <utility>

namespace slick::socket
{

struct PolledTCPClientConfig
{
    std::string server_address = "localhost";   // IPv4 literal or hostname
    uint16_t server_port = 5000;
    std::chrono::milliseconds connection_timeout{30000};
    // How long send() waits for a peer that has stopped reading before it gives the connection up
    std::chrono::milliseconds send_timeout{1000};
    // Sets TCP_NODELAY, so each send() goes out at once instead of waiting to be coalesced with the next
    bool no_delay = true;
    int receive_buffer_size = 0;  // SO_RCVBUF; 0 keeps the system default
    int send_buffer_size = 0;     // SO_SNDBUF; 0 keeps the system default
};

// A TCP client driven by its caller. It owns no thread and makes no callbacks. The caller's own loop
// calls poll() while a connection is being made and receive() to read. A busy-polling thread then
// reads the bytes itself, with no hand-off from a receiver thread, and several connections can share
// one thread. Nothing here allocates once the object is built.
//
// Not thread-safe. Use every member from the one thread that owns the object. For a client with its
// own thread and callbacks, see TCPClientBase (tcp_client.h).
class PolledTCPClient : private detail::SocketRuntime
{
public:
    enum class State : uint8_t
    {
        Disconnected,
        Connecting,
        Connected,
    };

    explicit PolledTCPClient(std::string name, const PolledTCPClientConfig& config = PolledTCPClientConfig())
        : name_(std::move(name)), config_(config)
    {
    }

    ~PolledTCPClient() { disconnect(); }

    // Not copyable or movable, because the object owns a socket
    PolledTCPClient(const PolledTCPClient&) = delete;
    PolledTCPClient& operator=(const PolledTCPClient&) = delete;
    PolledTCPClient(PolledTCPClient&&) = delete;
    PolledTCPClient& operator=(PolledTCPClient&&) = delete;

    // Starts connecting and returns at once. poll() reports when the connection is made or has failed.
    // Returns false if the attempt couldn't even be started. Resolving a hostname here blocks, but an
    // IPv4 literal doesn't.
    bool connect()
    {
        if (state_ != State::Disconnected)
        {
            return true;
        }

        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(config_.server_port);
        if (!detail::resolve_ipv4(config_.server_address, server_addr.sin_addr))
        {
            LOG_ERROR("{}: failed to resolve server address {}", name_, config_.server_address);
            return false;
        }

        socket_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (socket_ == detail::invalid_polled_socket)
        {
            LOG_ERROR("{}: failed to create socket: error {}", name_, detail::last_socket_error());
            return false;
        }
#if !defined(_WIN32) && !defined(_WIN64)
        detail::suppress_sigpipe(socket_);
#endif
        if (!detail::set_non_blocking(socket_))
        {
            LOG_ERROR("{}: failed to make socket non-blocking: error {}", name_, detail::last_socket_error());
            release_socket();
            return false;
        }
        apply_options();

        LOG_INFO("{} connecting to {}:{}", name_, config_.server_address, config_.server_port);
        if (::connect(socket_, reinterpret_cast<const sockaddr*>(&server_addr), sizeof(server_addr)) != 0)
        {
            const int error = detail::last_socket_error();
            if (!detail::is_in_progress(error))
            {
                LOG_WARN("{}: failed to connect: error {}", name_, error);
                release_socket();
                return false;
            }
            state_ = State::Connecting;
            connect_deadline_ = std::chrono::steady_clock::now() + config_.connection_timeout;
            return true;
        }

        // A loopback connect can complete at once
        state_ = State::Connected;
        LOG_INFO("{} connected", name_);
        return true;
    }

    // Completes a pending connect() without blocking. Call it from the caller's loop while the state is
    // Connecting. In any other state it just returns the state.
    State poll()
    {
        if (state_ != State::Connecting)
        {
            return state_;
        }

        if (!detail::connect_finished(socket_))
        {
            if (std::chrono::steady_clock::now() >= connect_deadline_)
            {
                LOG_WARN("{}: connection timed out", name_);
                release_socket();
            }
            return state_;
        }

        // Made or failed. The pending error says which.
        const int error = detail::pending_socket_error(socket_);
        if (error != 0)
        {
            LOG_WARN("{}: connection failed: error {}", name_, error);
            release_socket();
            return state_;
        }
        state_ = State::Connected;
        LOG_INFO("{} connected", name_);
        return state_;
    }

    void disconnect() noexcept
    {
        if (socket_ != detail::invalid_polled_socket)
        {
            LOG_INFO("{} disconnecting", name_);
            release_socket();
        }
    }

    State state() const noexcept { return state_; }
    bool is_connected() const noexcept { return state_ == State::Connected; }

    // Reads what has arrived, up to buffer.size() bytes, without blocking. Returns the number of bytes
    // read, 0 when nothing is waiting, or -1 when the connection is gone (closed by the server, or
    // failed). After -1 the state is Disconnected.
    std::ptrdiff_t receive(std::span<uint8_t> buffer) noexcept
    {
        if (state_ != State::Connected)
        {
            return state_ == State::Connecting ? 0 : -1;
        }

        const std::ptrdiff_t received = detail::polled_recv(socket_, buffer.data(), buffer.size());
        if (received > 0)
        {
            return received;
        }
        if (received == 0)
        {
            LOG_INFO("{}: server closed the connection", name_);
            release_socket();
            return -1;
        }

        const int error = detail::last_socket_error();
        if (detail::is_would_block(error) || detail::is_interrupted(error))
        {
            return 0;
        }
        LOG_ERROR("{}: receive error {}", name_, error);
        release_socket();
        return -1;
    }

    // Sends all of `data`, straight from the caller's buffer. If the socket buffer is full because the
    // server isn't reading, it waits up to send_timeout. Returns false, with the connection closed,
    // when the data couldn't all be sent. Part of a message may have gone out by then, so the stream
    // can't be resumed.
    bool send(std::span<const uint8_t> data) noexcept
    {
        if (state_ != State::Connected)
        {
            return false;
        }

        size_t total_sent = 0;
        while (total_sent < data.size())
        {
            const std::ptrdiff_t sent =
                detail::polled_send(socket_, data.data() + total_sent, data.size() - total_sent);
            if (sent >= 0)
            {
                total_sent += static_cast<size_t>(sent);
                continue;
            }

            const int error = detail::last_socket_error();
            if (detail::is_interrupted(error))
            {
                continue;
            }
            if (!detail::is_would_block(error) ||
                detail::wait_polled_socket(socket_, POLLOUT, static_cast<int>(config_.send_timeout.count())) == 0)
            {
                LOG_ERROR("{}: failed to send {} bytes ({} sent): error {}", name_, data.size(), total_sent, error);
                release_socket();
                return false;
            }
        }
        return true;
    }

private:
    void apply_options() noexcept
    {
        if (config_.no_delay && !detail::set_socket_option(socket_, IPPROTO_TCP, TCP_NODELAY, int{1}))
        {
            LOG_WARN("{}: failed to set TCP_NODELAY: error {}", name_, detail::last_socket_error());
        }
        if (config_.receive_buffer_size > 0 &&
            !detail::set_socket_option(socket_, SOL_SOCKET, SO_RCVBUF, config_.receive_buffer_size))
        {
            LOG_WARN("{}: failed to set receive buffer size: error {}", name_, detail::last_socket_error());
        }
        if (config_.send_buffer_size > 0 &&
            !detail::set_socket_option(socket_, SOL_SOCKET, SO_SNDBUF, config_.send_buffer_size))
        {
            LOG_WARN("{}: failed to set send buffer size: error {}", name_, detail::last_socket_error());
        }
    }

    void release_socket() noexcept
    {
        if (socket_ != detail::invalid_polled_socket)
        {
            detail::close_polled_socket(socket_);
            socket_ = detail::invalid_polled_socket;
        }
        state_ = State::Disconnected;
    }

    std::string name_;
    PolledTCPClientConfig config_;
    detail::PolledSocket socket_ = detail::invalid_polled_socket;
    State state_ = State::Disconnected;
    std::chrono::steady_clock::time_point connect_deadline_{};
};

} // namespace slick::socket
