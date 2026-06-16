CC = gcc
CFLAGS = -Wall -O2 -D_GNU_SOURCE
LDFLAGS = -libverbs

# Default target
all: server client

# Rule to build the server executable
server: bw_template.c
	$(CC) $(CFLAGS) -o server bw_template.c $(LDFLAGS)

# Rule to create the client symbolic link pointing to the server
client: server
	ln -sf server client

# Rule to clean up the workspace
clean:
	rm -f server client

.PHONY: all clean