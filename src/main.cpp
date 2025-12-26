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

void handle_client(int client_fd) {
    char buffer[1024];

    while (true) {
        int bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            break;
        }

        buffer[bytes_read] = '\0';
        std::string input(buffer);

        // Handle PING
        if (input.find("PING") != std::string::npos) {
            const char* pong = "+PONG\r\n";
            send(client_fd, pong, strlen(pong), 0);
        }

        // Handle ECHO
        else if (input.find("ECHO") != std::string::npos) {
            // Very simple extraction: take last bulk string
            size_t last_dollar = input.rfind('$');
            size_t last_crlf = input.find("\r\n", last_dollar);

            int len = std::stoi(input.substr(last_dollar + 1,
                              last_crlf - last_dollar - 1));

            std::string message =
                input.substr(last_crlf + 2, len);

            std::string response =
                "$" + std::to_string(message.size()) +
                "\r\n" + message + "\r\n";

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


 