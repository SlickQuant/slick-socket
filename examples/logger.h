#pragma once

// A simple logger for the examples. For production, use a real logging library such as spdlog or
// slick_logger.

#include <iostream>
#include <chrono>
#include <format>

namespace {
    // One function built on vformat handles calls with and without arguments
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
