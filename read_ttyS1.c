#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>
#include <string.h>
#include <errno.h>

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}




int main(void) {
    const char *portname = "/dev/ttyS1";

    // Open serial port (read-only, not controlling terminal)
    int fd = open(portname, O_RDONLY | O_NOCTTY);
    if (fd < 0) {
        die("open");
    }

    // Configure serial port
    struct termios tty;
    if (tcgetattr(fd, &tty) != 0) {
        die("tcgetattr");
    }

    cfmakeraw(&tty);                // raw mode (no translations)

    cfsetispeed(&tty, B9600);
    cfsetospeed(&tty, B9600);

    // 8N1, no flow control
    tty.c_cflag &= ~PARENB;         // no parity
    tty.c_cflag &= ~CSTOPB;         // 1 stop bit
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;             // 8 data bits
    tty.c_cflag &= ~CRTSCTS;        // no HW flow control
    tty.c_cflag |= (CLOCAL | CREAD);

    tty.c_iflag &= ~(IXON | IXOFF | IXANY); // no SW flow control

    tty.c_cc[VMIN]  = 1;            // at least 1 byte to return from read()
    tty.c_cc[VTIME] = 0;            // no timeout

    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        die("tcsetattr");
    }

    printf("Listening on %s (9600 8N1)...\n", portname);
    fflush(stdout);

    unsigned char buf[256];

    while (1) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n < 0) {
            // If interrupted by signal, continue
            if (errno == EINTR) continue;
            die("read");
        } else if (n == 0) {
            // EOF (shouldn't normally happen on a tty)
            continue;
        }

        printf("Read %zd bytes: ", n);
        for (ssize_t i = 0; i < n; i++) {
            printf("%02X ", buf[i]);
        }
        printf("\n");
        fflush(stdout);
    }

    close(fd);
    return 0;
}
