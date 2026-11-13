// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if !defined(_WIN32) && !defined(_WIN64)

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <netinet/in.h>
#include <slick/socket/logger.h>
#include <sys/socket.h>

namespace slick::socket::detail
{

// A write to a peer-closed TCP socket must fail with EPIPE instead of raising SIGPIPE. The
// process-wide signal disposition belongs to the embedding application, so it's left alone. Every
// send passes MSG_NOSIGNAL, and where the platform also offers the per-socket SO_NOSIGPIPE
// (macOS/BSD), it's set too.
inline void suppress_sigpipe([[maybe_unused]] int socket) noexcept
{
#ifdef SO_NOSIGPIPE
    int on = 1;
    setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
}

// Binds a multicast receiver to its group and port, or to 0.0.0.0 when `bind_to_group` is false.
// Either way it turns off IP_MULTICAST_ALL where the OS has it. Linux has that on by default, which
// hands a socket every group joined by any socket on the host, as long as the port matches. With only
// the group bound, its datagrams would still arrive from an interface where only another socket joined it.
// Returns false with the error in errno.
inline bool bind_multicast_receiver(int socket, in_addr group, uint16_t port, bool bind_to_group) noexcept
{
#ifdef IP_MULTICAST_ALL
    int all = 0;
    if (setsockopt(socket, IPPROTO_IP, IP_MULTICAST_ALL, &all, sizeof(all)) != 0)  // pre-2.6.31 lacks it
    {
        LOG_WARN("Failed to turn off IP_MULTICAST_ALL, other groups on port {} may get in. error={}", port, errno);
    }
#endif
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = bind_to_group ? group.s_addr : htonl(INADDR_ANY);
    return ::bind(socket, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0;
}

} // namespace slick::socket::detail

#endif // !defined(_WIN32) && !defined(_WIN64)
