#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <unordered_map>
#include <string>
#include <mutex>
#include <vector>
#include <chrono>



struct Value {
    std::string data;
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry;
};

std::unordered_map<std::string, Value> store;
std::mutex store_mutex;


std::vector<std::string> parse_resp(const std::string& input) {
    std::vector<std::string> result;
    size_t i = 0;

    // Skip "*<count>\r\n"
    if (input[i] != '*') return result;
    i = input.find("\r\n", i) + 2;

    while (i < input.size()) {
        if (input[i] != '$') break;

        // Read length
        size_t len_end = input.find("\r\n", i);
        int len = std::stoi(input.substr(i + 1, len_end - i - 1));

        // Read string
        i = len_end + 2;
        result.push_back(input.substr(i, len));

        i += len + 2; // skip string + \r\n
    }

    return result;
}

struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
};

std::unordered_map<std::string, std::vector<StreamEntry>> streams;

bool is_valid_id(const std::string& id) {
    return id.find('-') != std::string::npos;
}

bool parse_id(const std::string& id, long long& ms, long long& seq) {
    size_t dash = id.find('-');
    if (dash == std::string::npos) return false;

    try {
        ms  = std::stoll(id.substr(0, dash));
        seq = std::stoll(id.substr(dash + 1));
    } catch (...) {
        return false;
    }

    return ms >= 0 && seq >= 0;
}



void handle_client(int client_fd) {
    char buffer[1024];

    while (true) {
        int bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) break;

        buffer[bytes_read] = '\0';
        std::string input(buffer);

        auto tokens = parse_resp(input);
        if (tokens.empty()) continue;

        std::string command = tokens[0];

        // Normalize command to uppercase
        for (auto& c : command) c = toupper(c);

        // ---------- PING ----------
        if (command == "PING") {
            const char* pong = "+PONG\r\n";
            send(client_fd, pong, strlen(pong), 0);
        }

        // ---------- ECHO ----------
        else if (command == "ECHO" && tokens.size() == 2) {
            std::string& msg = tokens[1];
            std::string response =
                "$" + std::to_string(msg.size()) + "\r\n" +
                msg + "\r\n";

            send(client_fd, response.c_str(), response.size(), 0);
        }

        // ---------- SET ----------
        else if (command == "SET" && (tokens.size() == 3 || tokens.size() == 5)) {
    std::lock_guard<std::mutex> lock(store_mutex);

    Value val;
    val.data = tokens[2];
    val.has_expiry = false;

    if (tokens.size() == 5) {
        std::string option = tokens[3];
        for (auto& c : option) c = toupper(c);

        if (option == "PX") {
            int ttl_ms = std::stoi(tokens[4]);
            val.expiry = std::chrono::steady_clock::now()
                       + std::chrono::milliseconds(ttl_ms);
            val.has_expiry = true;
        }
    }

    store[tokens[1]] = val;

    const char* ok = "+OK\r\n";
    send(client_fd, ok, strlen(ok), 0);
}


        // ---------- GET ----------
        else if (command == "GET" && tokens.size() == 2) {
    std::lock_guard<std::mutex> lock(store_mutex);

    auto it = store.find(tokens[1]);
    if (it == store.end()) {
        const char* nil = "$-1\r\n";
        send(client_fd, nil, strlen(nil), 0);
        continue;
    }

    Value& val = it->second;

    if (val.has_expiry &&
        std::chrono::steady_clock::now() > val.expiry) {
        store.erase(it);
        const char* nil = "$-1\r\n";
        send(client_fd, nil, strlen(nil), 0);
        continue;
    }

    std::string response =
        "$" + std::to_string(val.data.size()) + "\r\n" +
        val.data + "\r\n";

    send(client_fd, response.c_str(), response.size(), 0);
}

// -------- TYPE COMMAND LOGIC --------------

else if (command == "TYPE" && tokens.size() == 2) {
    std::lock_guard<std::mutex> lock(store_mutex);

    const std::string& key = tokens[1];

    // 1️⃣ Check strings
    auto sit = store.find(key);
    if (sit != store.end()) {
        Value& val = sit->second;

        // Expiry check
        if (val.has_expiry &&
            std::chrono::steady_clock::now() > val.expiry) {
            store.erase(sit);
        } else {
            const char* type = "+string\r\n";
            send(client_fd, type, strlen(type), 0);
            continue;
        }
    }

    // 2️⃣ Check streams
    if (streams.find(key) != streams.end()) {
        const char* type = "+stream\r\n";
        send(client_fd, type, strlen(type), 0);
        continue;
    }

    // 3️⃣ Not found
    const char* none = "+none\r\n";
    send(client_fd, none, strlen(none), 0);
}


// ------------- XADD ----------
else if (command == "XADD" && tokens.size() >= 5) {
    std::string stream_key = tokens[1];
    std::string entry_id   = tokens[2];

    // ===============================
    // ENTRY ID VALIDATION STARTS HERE
    // ===============================

   long long new_ms, new_seq;
if (!parse_id(entry_id, new_ms, new_seq)) {
    const char* err = "-ERR Invalid stream ID\r\n";
    send(client_fd, err, strlen(err), 0);
    continue;
}

auto it = streams.find(stream_key);

// 🔹 Case 1: stream does NOT exist yet
if (it == streams.end()) {
    // First entry must be > 0-0
    if (new_ms == 0 && new_seq == 0) {
        const char* err =
            "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        send(client_fd, err, strlen(err), 0);
        continue;
    }
}
// 🔹 Case 2: stream exists → must be strictly increasing
else {
    auto& stream = it->second;

    long long last_ms, last_seq;
    parse_id(stream.back().id, last_ms, last_seq);

    if (new_ms < last_ms ||
        (new_ms == last_ms && new_seq <= last_seq)) {
        const char* err =
            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        send(client_fd, err, strlen(err), 0);
        continue;
    }
}


    // ===============================
    // ENTRY ID VALIDATION ENDS HERE
    // ===============================

    // Build entry (ONLY AFTER validation)
    StreamEntry entry;
entry.id = entry_id;

for (size_t i = 3; i < tokens.size(); i += 2) {
    entry.fields.push_back({tokens[i], tokens[i + 1]});
}

streams[stream_key].push_back(entry);
    

    // Return entry ID
    std::string response =
        "$" + std::to_string(entry_id.size()) + "\r\n" +
        entry_id + "\r\n";

    send(client_fd, response.c_str(), response.size(), 0);
}
    }

    close(client_fd);
}


  int main(int argc, char **argv) {
   std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // 1. Create server socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    // 2. Allow reuse
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    // 3. Bind
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind\n";
        return 1;
    }

    // 4. Listen
    listen(server_fd, 5);

    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);

    while (true) {
        int client_fd = accept(
            server_fd,
            (struct sockaddr*)&client_addr,
            (socklen_t*)&client_addr_len
        );

        std::thread t(handle_client, client_fd);
        t.detach();
    }

    return 0; // never reached
}


 