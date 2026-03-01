# Makefile for Mosquitto MongoDB Logger Plugin

# Compiler and flags
CC = gcc
CFLAGS = -fPIC -Wall -Wextra -Werror $(shell pkg-config --cflags libmongoc-1.0)
LDFLAGS = -shared $(shell pkg-config --libs libmongoc-1.0)

# Target and source
TARGET = mosquitto_logger_plugin.so
SOURCE = mosquitto_logger_plugin.c

# Default target
all: $(TARGET)

# Build the shared library
$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

# Install target (requires root privileges)
install: $(TARGET)
	install -D -m 755 $(TARGET) /usr/lib/mosquitto/$(TARGET)
	@echo "Plugin installed to /usr/lib/mosquitto/$(TARGET)"
	@echo "Configure it in /etc/mosquitto/mosquitto.conf"

# Clean build artifacts
clean:
	rm -f $(TARGET)

# Check if dependencies are installed
check-deps:
	@echo "Checking for required dependencies..."
	@pkg-config --exists libmongoc-1.0 && echo "✓ libmongoc-1.0 found" || echo "✗ libmongoc-1.0 not found"
	@pkg-config --exists libbson-1.0 && echo "✓ libbson-1.0 found" || echo "✗ libbson-1.0 not found"
	@which mosquitto > /dev/null 2>&1 && echo "✓ mosquitto found" || echo "✗ mosquitto not found"

.PHONY: all clean install check-deps
