// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if !defined(_WIN32) && !defined(_WIN64)

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

} // namespace slick::socket::detail

#endif // !defined(_WIN32) && !defined(_WIN64)
