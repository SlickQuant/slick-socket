# slick-socket

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/SlickQuant/slick-socket/actions/workflows/ci.yml/badge.svg)](https://github.com/SlickQuant/slick-socket/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/SlickQuant/slick-socket)](https://github.com/SlickQuant/slick-socket/releases)

A header-only C++20 networking library providing cross-platform TCP and UDP multicast communication.

## Features

- **Cross-platform**: Windows and Unix/Linux support
- **Header-only**: No separate compilation required
- **Modern C++**: C++20 design with CRTP for most components
- **Asynchronous**: Non-blocking socket operations with timeout handling
- **TCP Communication**: Client and server implementations
- **UDP Multicast**: One-to-many communication support
- **Logging**: Template-based logger interface with console output

## Dependencies

- **Windows**: Requires [wepoll](https://github.com/piscisaureus/wepoll) for epoll-like functionality
  - Automatically fetched via CMake FetchContent if not installed
  - Or install via vcpkg: `vcpkg install wepoll`
- **Unix/Linux/macOS**: No external dependencies

## Installation

### Using vcpkg (Recommended for Windows users)

If you're using vcpkg, install wepoll first:

```bash
vcpkg install wepoll
```

Then use CMake with the vcpkg toolchain:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake
```

### Using FetchContent

The easiest way to use slick-socket is to fetch it directly in your CMakeLists.txt:

```cmake
include(FetchContent)

# Disable slick-socket examples and tests
set(BUILD_SLICK_SOCKET_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_SLICK_SOCKET_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    slick-socket
    GIT_REPOSITORY https://github.com/SlickQuant/slick-socket.git
    GIT_TAG v1.0.6  # Use the desired version
)

FetchContent_MakeAvailable(slick-socket)

# Link against slick-socket (automatically links ws2_32 and wepoll on Windows)
target_link_libraries(your_target PRIVATE slick::socket)
```

**Note**: On Windows, if wepoll is not found, CMake will automatically fetch and build it from GitHub.

### Using find_package

If you have slick-socket installed, you can use it with `find_package`:

```cmake
find_package(slick-socket REQUIRED)
target_link_libraries(your_target PRIVATE slick::socket)
```

### From Source

#### Prerequisites

- C++20 compatible compiler (GCC 11+, Clang 12+, MSVC 2022+)
- CMake 3.25 or higher
- **Windows only**: wepoll (automatically fetched if not found)

#### Unix/Linux/macOS

1. **Configure the build**:
   ```bash
   cmake -S . -B build
   ```

2. **Build the library**:
   ```bash
   cmake --build build --config Release
   ```

3. **Copy to your project**:
   ```bash
   cp -r build/dist/include/slick /path/to/your/project/include/
   ```

   The library is header-only on Unix/Linux/macOS platforms, so only headers are needed.

#### Windows (Visual Studio)

1. **(Optional) Install wepoll via vcpkg**:
   ```bash
   vcpkg install wepoll
   ```

2. **Configure the build**:
   ```bash
   # With vcpkg
   cmake -S . -B build -G "Visual Studio 17 2022" -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake

   # Without vcpkg (wepoll will be fetched automatically)
   cmake -S . -B build -G "Visual Studio 17 2022"
   ```

3. **Build the library**:
   ```bash
   cmake --build build --config Release
   ```

4. **Install (optional)**:
   ```bash
   cmake --install build --prefix /path/to/install
   ```

   Then in your project:
   ```cmake
   find_package(slick-socket REQUIRED)
   target_link_libraries(your_target PRIVATE slick::socket)
   ```

## Usage

### Basic Example

Include the headers you need in your project:

```cpp
#include <slick/socket/tcp_server.h>
#include <slick/socket/tcp_client.h>
#include <slick/socket/multicast_sender.h>
#include <slick/socket/multicast_receiver.h>
```

### Creating a TCP Server

```cpp
#include <slick/socket/tcp_server.h>

class MyServer : public slick::socket::TCPServerBase<MyServer>
{
public:
    MyServer() : TCPServerBase("MyServer", {/*.port = 5000*/}) {}

    void onClientConnected(int client_id, const std::string& address)
    {
        std::cout << "Client " << client_id << " connected from " << address << std::endl;
    }

    void onClientData(int client_id, const uint8_t* data, size_t size)
    {
        // Echo back to client
        send_data(client_id, std::vector<uint8_t>(data, data + size));
    }

    void onClientDisconnected(int client_id)
    {
        std::cout << "Client " << client_id << " disconnected" << std::endl;
    }
};

int main()
{
    MyServer server;
    server.start();
    // ... server runs in background thread
    server.stop();
    return 0;
}
```

Server configuration notes:

- Set `TCPServerConfig::port = 0` to let the OS pick a free port; `port()` returns the bound port after `start()`.
- `TCPServerConfig::max_connections` caps concurrent clients. Connections beyond the limit are accepted and closed immediately (the client sees a disconnect). A value `<= 0` means unlimited.

### Creating a TCP Client

```cpp
#include <slick/socket/tcp_client.h>

class MyClient : public slick::socket::TCPClientBase<MyClient>
{
public:
    MyClient(const slick::socket::TCPClientConfig& config)
        : TCPClientBase("MyClient", config) {}

    void onConnected()
    {
        std::cout << "Connected to server" << std::endl;
    }

    void onDisconnected()
    {
        std::cout << "Disconnected from server" << std::endl;
    }

    void onData(const uint8_t* data, size_t length)
    {
        std::string received_data((const char*)data, length);
        std::cout << "Received: " << received_data << std::endl;
    }
};

int main()
{
    slick::socket::TCPClientConfig config;
    config.server_address = "127.0.0.1";   // IPv4 address or hostname (default: "localhost")
    config.server_port = 5000;

    MyClient client(config);
    client.connect();

    if (client.is_connected())
    {
        client.send_data("Hello Server!");
        // ... process responses
        client.disconnect();
    }

    return 0;
}
```

If the server closes the connection, `onDisconnected()` is called and `is_connected()` becomes false. Calling `connect()` again reconnects the same client object.

> **Lifetime:** call `stop()` / `disconnect()` before a derived server or client is destroyed. The base destructor also stops the worker thread, but by then the derived object's members are already destroyed, so callbacks such as `onDisconnected()` must not run against them.

### Creating a Multicast Sender

```cpp
#include <slick/socket/multicast_sender.h>

int main()
{
    slick::socket::MulticastSenderConfig config;
    config.multicast_address = "224.0.0.100";
    config.port = 12345;
    config.ttl = 1; // Local network only

    slick::socket::MulticastSender sender("MySender", config);

    if (!sender.start())
    {
        std::cerr << "Failed to start sender" << std::endl;
        return -1;
    }

    // Send data to multicast group
    sender.send_data("Hello Multicast World!");

    // Check statistics
    std::cout << "Packets sent: " << sender.get_packets_sent() << std::endl;

    sender.stop();
    return 0;
}
```

### Creating a Multicast Receiver

```cpp
#include <slick/socket/multicast_receiver.h>

class MyReceiver : public slick::socket::MulticastReceiverBase<MyReceiver>
{
public:
    MyReceiver(const slick::socket::MulticastReceiverConfig& config)
        : MulticastReceiverBase("MyReceiver", config) {}

    void handle_multicast_data(const std::vector<uint8_t>& data, const std::string& sender_address)
    {
        std::string message(data.begin(), data.end());
        std::cout << "Received from " << sender_address << ": " << message << std::endl;
    }
};

int main()
{
    slick::socket::MulticastReceiverConfig config;
    config.multicast_address = "224.0.0.100";
    config.port = 12345;
    config.reuse_address = true; // Allow multiple receivers

    MyReceiver receiver(config);

    if (!receiver.start())
    {
        std::cerr << "Failed to start receiver" << std::endl;
        return -1;
    }

    // ... receiver runs in background thread
    std::this_thread::sleep_for(std::chrono::seconds(30));

    receiver.stop();
    return 0;
}
```

For more examples, see the [examples/](examples/) directory.

## Testing

Run the complete test suite:

```bash
cd build && ctest --output-on-failure -C Debug
```

Run specific tests:

```bash
cd build && ctest -R TCPServerTest -C Debug
```

Enable verbose test output:

```bash
cd build && ctest -V -C Debug
```

## Development

### Build Options

#### AddressSanitizer

Enable AddressSanitizer for debugging memory issues:

```bash
cmake -S . -B build -DENABLE_ASAN=ON
cmake --build build --config Debug
```

#### Release Build with Optimization

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

### Project Structure

```
slick-socket/
├── include/slick/socket/     # Public headers
│   ├── tcp_server.h          # TCP server base class
│   ├── tcp_client.h          # TCP client base class
│   ├── multicast_sender.h    # UDP multicast sender
│   ├── multicast_receiver.h  # UDP multicast receiver
│   └── logger.h              # Logger interface
├── src/                       # Implementation files (Windows-specific)
├── examples/                  # Usage examples
├── tests/                     # Unit and integration tests
└── CMakeLists.txt
```

## Architecture

The library uses a three-file pattern for cross-platform support:

- `component.h` - Base class with platform-independent interface
- `component_win32.h` - Windows implementation
- `component_unix.h` - Unix/Linux implementation

Most components use CRTP (Curiously Recurring Template Pattern) for compile-time polymorphism without virtual function overhead. `MulticastSender` is implemented as a regular class without CRTP for simpler usage.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**