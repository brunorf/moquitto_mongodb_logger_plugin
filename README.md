# Mosquitto MQTT Broker MongoDB Logger Plugin

This is a [Mosquitto](https://mosquitto.org/) MQTT broker plugin that logs every received publication to a MongoDB database.

## Features

- Logs all MQTT messages to MongoDB
- Each publication is stored in a collection named after the topic
- Automatically detects payload type (integer, float, or string) and stores accordingly
- Includes timestamp for each message
- Configurable MongoDB database name

## Requirements

- Mosquitto broker (version 2.0 or later)
- MongoDB C driver (libmongoc-1.0)
- BSON library (libbson-1.0)

## Building

### Dependencies

Install the required development packages:

```bash
# Debian/Ubuntu
sudo apt-get install libmosquitto-dev libmongoc-dev libbson-dev

# Fedora/RHEL
sudo dnf install mosquitto-devel mongo-c-driver-devel

# macOS (with Homebrew)
brew install mosquitto mongo-c-driver
```

### Compilation

Compile the plugin using gcc:

```bash
gcc -fPIC -shared -o mosquitto_logger_plugin.so mosquitto_logger_plugin.c \
    $(pkg-config --cflags --libs libmongoc-1.0)
```

Or create a Makefile:

```makefile
CFLAGS = -fPIC -Wall -Wextra $(shell pkg-config --cflags libmongoc-1.0)
LDFLAGS = -shared $(shell pkg-config --libs libmongoc-1.0)

mosquitto_logger_plugin.so: mosquitto_logger_plugin.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

clean:
	rm -f mosquitto_logger_plugin.so

.PHONY: clean
```

## Configuration

Add the plugin to your Mosquitto configuration file (typically `/etc/mosquitto/mosquitto.conf` or `/etc/mosquitto/conf.d/logger.conf`):

```conf
# Load the MongoDB logger plugin
plugin /path/to/mosquitto_logger_plugin.so

# MongoDB connection URI (required)
plugin_opt_mongodb_uri mongodb://localhost:27017

# MongoDB database name (optional, defaults to "mqtt_data")
plugin_opt_mongodb_database my_mqtt_logs
```

### Configuration Options

- `mongodb_uri` (required): MongoDB connection string
  - Examples:
    - `mongodb://localhost:27017`
    - `mongodb://user:password@localhost:27017`
    - `mongodb://host1:27017,host2:27017/?replicaSet=myReplSet`
- `mongodb_database` (optional): Database name to use
  - Default: `mqtt_data`

## Data Format

Each message is stored as a document with the following structure:

```json
{
  "_id": ObjectId("..."),
  "payload": <value>,
  "timestamp": ISODate("...")
}
```

The `payload` field type is automatically detected:
- **Integer**: Stored as Int32 (e.g., `42`, `-10`)
- **Float**: Stored as Double (e.g., `3.14`, `-0.5`)
- **String**: Stored as UTF-8 string (e.g., `"hello"`, `"sensor_data"`)

## Example Usage

1. Start MongoDB:
   ```bash
   mongod --dbpath /var/lib/mongodb
   ```

2. Configure and restart Mosquitto:
   ```bash
   sudo systemctl restart mosquitto
   ```

3. Publish a test message:
   ```bash
   mosquitto_pub -t "temperature/sensor1" -m "23.5"
   ```

4. Check MongoDB for the logged message:
   ```bash
   mongosh
   use mqtt_data
   db['temperature/sensor1'].find()
   ```

## License

This project is licensed under the GNU General Public License v2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
