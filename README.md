# Redis Clone (C++)

A lightweight Redis-inspired in-memory key-value database server implemented in C++. The project supports core Redis data structures, client-server communication over TCP, replication, persistence through RDB loading, and several commonly used Redis commands.

## Features

- TCP server with support for multiple client connections
- RESP (Redis Serialization Protocol) request parsing
- In-memory storage with optional key expiration (TTL)
- RDB file loading during startup
- Master-replica replication support
- Basic Pub/Sub messaging
- Transaction support
- Authentication and ACL-based user management

## Supported Data Structures

- Strings
- Lists
- Streams
- Sorted Sets
- Geospatial data (Geo)
- Pub/Sub channels

## Supported Commands

- **Strings:** `PING`, `ECHO`, `SET`, `GET`, `INCR`, `TYPE`
- **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `LLEN`, `BLPOP`
- **Streams:** `XADD`, `XRANGE`, `XREAD`
- **Sorted Sets:** `ZADD`, `ZRANGE`, `ZRANK`, `ZCARD`, `ZSCORE`, `ZREM`
- **Geo:** `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`
- **Replication & Server:** `INFO`, `REPLCONF`, `PSYNC`, `WAIT`, `CONFIG GET`, `KEYS`
- **Pub/Sub:** `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`
- **Authentication:** `AUTH`, `ACL WHOAMI`, `ACL SETUSER`, `ACL GETUSER`

## Tech Stack

- C++23
- CMake
- OpenSSL (SHA-256)

## Build

```bash
mkdir build
cd build
cmake ..
cmake --build .
```

## Run

### Start as a standalone server

```bash
./redis-server --port 6379
```

### Start as a replica

```bash
./redis-server --port 6380 --replicaof <master_host> <master_port>
```

## Project Structure

```
.
├── src/
│   └── main.cpp
├── CMakeLists.txt
├── vcpkg.json
└── README.md
```