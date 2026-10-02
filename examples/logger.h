#pragma once

// NOTE: This is a simple example logger for demonstration purposes.
// For production use, consider using a proper logging library like spdlog or slick_logger.

#include <iostream>
#include <chrono>
#include <format>

namespace {
    // Single function using vformat handles both cases: with and without arguments
    template<typename... Args>
    void log_impl(const std::string& level, const std::string& fmt, Args&&... args)
    {
        std::cout << std::format("{:%Y-%m-%d %H:%M:%S} ", std::chrono::system_clock::now())
                  << "[" << level << "] " << std::vformat(fmt, std::make_format_args(args...)) << std::endl;
    }
}

// The format string is the first variadic argument, so no GNU `, ##__VA_ARGS__` is needed
#define LOG_DEBUG(...) log_impl("DEBUG", __VA_ARGS__)
#define LOG_INFO(...) log_impl("INFO", __VA_ARGS__)
#define LOG_WARN(...) log_impl("WARNING", __VA_ARGS__)
#define LOG_ERROR(...) log_impl("ERROR", __VA_ARGS__)
#define LOG_TRACE(...) log_impl("TRACE", __VA_ARGS__)
