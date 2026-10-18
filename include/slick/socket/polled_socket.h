// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <slick/socket/socket_options_unix.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

// Code shared by the caller-driven sockets (polled_tcp_client.h, polled_multicast_receiver.h). It
// holds the few calls that are spelled differently on Winsock and on BSD sockets.
namespace slick::socket::detail
{

#if defined(_WIN32) || defined(_WIN64)

using PolledSocket = SOCKET;
inline constexpr PolledSocket invalid_polled_socket = INVALID_SOCKET;

// Winsock is reference counted per process, and every object holds one reference for its lifetime.
// The polled sockets use this as a private base, so it's set up before their members and torn down
// after them.
class SocketRuntime
{
public:
    SocketRuntime()
    {
        WSADATA wsa_data;
        const int result = WSAStartup(MAKEWORD(2, 2), &wsa_data);
        if (result != 0)
        {
            throw std::runtime_error("WSAStartup failed: " + std::to_string(result));
        }
    }

    ~SocketRuntime() { WSACleanup(); }

    SocketRuntime(const SocketRuntime&) = delete;
    SocketRuntime& operator=(const SocketRuntime&) = delete;
};

inline int last_socket_error() noexcept { return WSAGetLastError(); }
inline bool is_would_block(int error) noexcept { return error == WSAEWOULDBLOCK; }
inline bool is_in_progress(int error) noexcept { return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS; }
inline bool is_interrupted(int error) noexcept { return error == WSAEINTR; }

inline void close_polled_socket(PolledSocket socket) noexcept { closesocket(socket); }

inline bool set_non_blocking(PolledSocket socket) noexcept
{
    u_long mode = 1;
    return ioctlsocket(socket, FIONBIO, &mode) == 0;
}

// Waits up to timeout_ms for `events` (POLLIN/POLLOUT). Returns the events that are ready, or 0 on
// timeout. POLLERR and POLLHUP are always reported.
inline short wait_polled_socket(PolledSocket socket, short events, int timeout_ms) noexcept
{
    WSAPOLLFD pfd{socket, events, 0};
    return WSAPoll(&pfd, 1, timeout_ms) > 0 ? pfd.revents : short{0};
}

inline std::ptrdiff_t polled_recv(PolledSocket socket, uint8_t* buffer, size_t size) noexcept
{
    return recv(socket, reinterpret_cast<char*>(buffer), static_cast<int>(size), 0);
}

// Reads one datagram, up to `size` bytes of it, and sets `truncated` when it was longer. Returns the
// bytes stored, or -1 with the error in last_socket_error().
inline std::ptrdiff_t polled_recv_datagram(PolledSocket socket, uint8_t* buffer, size_t size, bool& truncated) noexcept
{
    const int received = recv(socket, reinterpret_cast<char*>(buffer), static_cast<int>(size), 0);
    // Winsock fills the buffer with what fits and reports WSAEMSGSIZE instead of a length
    truncated = received == SOCKET_ERROR && WSAGetLastError() == WSAEMSGSIZE;
    return truncated ? static_cast<std::ptrdiff_t>(size) : received;
}

inline std::ptrdiff_t polled_send(PolledSocket socket, const uint8_t* data, size_t size) noexcept
{
    return send(socket, reinterpret_cast<const char*>(data), static_cast<int>(size), 0);
}

#else

using PolledSocket = int;
inline constexpr PolledSocket invalid_polled_socket = -1;

// Nothing to set up. As an empty base it takes no space.
class SocketRuntime
{
};

inline int last_socket_error() noexcept { return errno; }
inline bool is_would_block(int error) noexcept { return error == EAGAIN || error == EWOULDBLOCK; }
inline bool is_in_progress(int error) noexcept { return error == EINPROGRESS; }
inline bool is_interrupted(int error) noexcept { return error == EINTR; }

inline void close_polled_socket(PolledSocket socket) noexcept { ::close(socket); }

inline bool set_non_blocking(PolledSocket socket) noexcept
{
    const int flags = fcntl(socket, F_GETFL, 0);
    return flags >= 0 && fcntl(socket, F_SETFL, flags | O_NONBLOCK) == 0;
}

inline short wait_polled_socket(PolledSocket socket, short events, int timeout_ms) noexcept
{
    pollfd pfd{socket, events, 0};
    return ::poll(&pfd, 1, timeout_ms) > 0 ? pfd.revents : short{0};
}

inline std::ptrdiff_t polled_recv(PolledSocket socket, uint8_t* buffer, size_t size) noexcept
{
    return ::recv(socket, buffer, size, 0);
}

// Reads one datagram, up to `size` bytes of it, and sets `truncated` when it was longer. Returns the
// bytes stored, or -1 with the error in last_socket_error().
inline std::ptrdiff_t polled_recv_datagram(PolledSocket socket, uint8_t* buffer, size_t size, bool& truncated) noexcept
{
#if defined(__linux__)
    // MSG_TRUNC makes Linux return the datagram's real length, which is the cheapest way to see a cut
    const std::ptrdiff_t received = ::recv(socket, buffer, size, MSG_TRUNC);
    truncated = received > static_cast<std::ptrdiff_t>(size);
    return truncated ? static_cast<std::ptrdiff_t>(size) : received;
#else
    // Only Linux returns the real length for MSG_TRUNC (macOS's recv() doesn't document it). recvmsg()
    // reports a cut datagram in msg_flags on every POSIX system.

    iovec part{};
    part.iov_base = buffer;
    part.iov_len = size;
    msghdr message{};
    message.msg_iov = &part;
    message.msg_iovlen = 1;
    const std::ptrdiff_t received = ::recvmsg(socket, &message, 0);
    truncated = received >= 0 && (message.msg_flags & MSG_TRUNC) != 0;
    return received;
#endif
}

inline std::ptrdiff_t polled_send(PolledSocket socket, const uint8_t* data, size_t size) noexcept
{
    // With MSG_NOSIGNAL, a send on a peer-closed socket fails with EPIPE instead of raising SIGPIPE
    return ::send(socket, data, size, MSG_NOSIGNAL);
}

#endif

template<typename T>
inline bool set_socket_option(PolledSocket socket, int level, int name, const T& value) noexcept
{
#if defined(_WIN32) || defined(_WIN64)
    return setsockopt(socket, level, name, reinterpret_cast<const char*>(&value), static_cast<int>(sizeof(T))) == 0;
#else
    return setsockopt(socket, level, name, &value, sizeof(T)) == 0;
#endif
}

// Whether a non-blocking connect() has finished, either made or failed. pending_socket_error() tells
// which.
inline bool connect_finished(PolledSocket socket) noexcept
{
#if defined(_WIN32) || defined(_WIN64)
    // Winsock reports a failed connect in the except set. Before Windows 10 2004, WSAPoll() didn't
    // report it at all.
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(socket, &write_fds);
    fd_set except_fds;
    FD_ZERO(&except_fds);
    FD_SET(socket, &except_fds);
    timeval no_wait{0, 0};
    return select(0, nullptr, &write_fds, &except_fds, &no_wait) > 0;
#else
    return wait_polled_socket(socket, POLLOUT, 0) != 0;
#endif
}

// The pending error of a socket whose non-blocking connect() has finished, 0 when it succeeded
inline int pending_socket_error(PolledSocket socket) noexcept
{
    int error = 0;
#if defined(_WIN32) || defined(_WIN64)
    int length = sizeof(error);
    if (getsockopt(socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &length) != 0)
#else
    socklen_t length = sizeof(error);
    if (getsockopt(socket, SOL_SOCKET, SO_ERROR, &error, &length) != 0)
#endif
    {
        return last_socket_error();
    }
    return error;
}

// Resolves an IPv4 literal or a hostname. A hostname costs a resolver round trip, which blocks.
inline bool resolve_ipv4(const std::string& address, in_addr& out)
{
    if (inet_pton(AF_INET, address.c_str(), &out) == 1)
    {
        return true;
    }

    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* result = nullptr;
    if (getaddrinfo(address.c_str(), nullptr, &hints, &result) != 0 || result == nullptr)
    {
        return false;
    }

    out = reinterpret_cast<const sockaddr_in*>(result->ai_addr)->sin_addr;
    freeaddrinfo(result);
    return true;
}

} // namespace slick::socket::detail
