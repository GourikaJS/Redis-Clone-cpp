cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp)

# Set the C++ standard to 23 as requested
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find all source files recursively
file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

# 1. Find necessary packages
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(asio CONFIG REQUIRED)
find_package(OpenSSL REQUIRED) # Locate OpenSSL for SHA256 hashing

# 2. Define the executable
add_executable(redis ${SOURCE_FILES})

# 3. Link libraries using modern CMake targets
# OpenSSL::Crypto handles both the library (-lcrypto) and the include paths automatically.
target_link_libraries(redis PRIVATE 
    asio 
    asio::asio 
    Threads::Threads 
    OpenSSL::Crypto
)

# Optional: Ensure the compiler can find OpenSSL headers if targets aren't enough
target_include_directories(redis PRIVATE ${OPENSSL_INCLUDE_DIR})