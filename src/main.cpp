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
        int bytes_read = read(client_fd, buffer, sizeof(buffer));
        if (bytes_read <= 0) {
            break; // client disconnected
        }

        const char* response = "+PONG\r\n";
        send(client_fd, response, strlen(response), 0);
    }

    close(client_fd);
}


int main(int argc, char **argv) {
  int main(int argc, char **argv) {
    // socket(), setsockopt(), bind(), listen() code above

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


  return 0;
}
