// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#if !defined(_WIN32) && !defined(_WIN64)

#include <sys/socket.h>

namespace slick::socket::detail
{

// Writes to a peer-closed TCP socket must fail with EPIPE rather than raise SIGPIPE, without touching
// the process-wide signal disposition, which belongs to the embedding application. Every send passes
// MSG_NOSIGNAL; where the platform also offers the per-socket SO_NOSIGPIPE (macOS/BSD), it is set too.
inline void suppress_sigpipe([[maybe_unused]] int socket) noexcept
{
#ifdef SO_NOSIGPIPE
    int on = 1;
    setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
}

} // namespace slick::socket::detail

#endif // !defined(_WIN32) && !defined(_WIN64)
