// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

namespace slick::socket::detail
{
// Swallows log arguments when no logger is configured
template<typename... Args>
constexpr void discard_log_args(const Args&...) noexcept {}
}

// Default no-op logging. The arguments are referenced in a dead branch, never evaluated, so variables
// that exist only to be logged do not trigger unused-variable warnings, and the call costs nothing.
#define SLICK_SOCKET_LOG_NOOP(...) do { if (false) { ::slick::socket::detail::discard_log_args(__VA_ARGS__); } } while (0)

// Logging function placeholders
// User can assign their own log functions by defining these macros before including this file
#ifndef LOG_DEBUG
#define LOG_DEBUG(...) SLICK_SOCKET_LOG_NOOP(__VA_ARGS__)
#endif
#ifndef LOG_INFO
#define LOG_INFO(...) SLICK_SOCKET_LOG_NOOP(__VA_ARGS__)
#endif
#ifndef LOG_WARN
#define LOG_WARN(...) SLICK_SOCKET_LOG_NOOP(__VA_ARGS__)
#endif
#ifndef LOG_ERROR
#define LOG_ERROR(...) SLICK_SOCKET_LOG_NOOP(__VA_ARGS__)
#endif
#ifndef LOG_TRACE
#define LOG_TRACE(...) SLICK_SOCKET_LOG_NOOP(__VA_ARGS__)
#endif
