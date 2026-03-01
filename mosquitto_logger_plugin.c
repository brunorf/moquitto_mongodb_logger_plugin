#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mosquitto_broker.h"
#include "mosquitto_plugin.h"
#include "mosquitto.h"
#include "mqtt_protocol.h"
#include "unistd.h"
#include <time.h>

#include <bson/bson.h>
#include <mongoc/mongoc.h>

#include <regex.h>

#define UNUSED(A) (void)(A)

static mosquitto_plugin_id_t *mosq_pid = NULL;
static char *mongodb_uri = NULL;
static char *mongodb_database = NULL;
static mongoc_client_t *client = NULL;


/**
 * @brief Saves publication to MongoDB database
 * @description Saves the publication to a MongoDB database, considering
 *              the data type in the payload.
 *              Currently checks if payload is float, int or string and saves with appropriate format.
 *              Also saves a timestamp for when the publication was handled by the MQTT broker.
 *
 * @param event Event type
 * @param event_data Event data containing the message
 * @param userdata User data (unused)
 * @return int MOSQ_ERR_SUCCESS on success
 */
static int callback_message(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_message *ed = event_data;
	mongoc_collection_t *collection = NULL;
	bson_error_t error;
	bson_oid_t oid;
	bson_t *doc = NULL;
	regex_t regex_letters;
	regex_t regex_float;
	regex_t regex_int;
	int regex_letters_compiled = 0;
	int regex_float_compiled = 0;
	int regex_int_compiled = 0;
	time_t rawtime;
	int result = MOSQ_ERR_SUCCESS;
	const char *db_name = mongodb_database ? mongodb_database : "mqtt_data";

	UNUSED(event);
	UNUSED(userdata);

	if (!client) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "MongoDB client not initialized");
		return MOSQ_ERR_UNKNOWN;
	}

	if (!ed || !ed->topic || !ed->payload) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Invalid event data");
		return MOSQ_ERR_INVAL;
	}

	// Get UTC datetime
	time(&rawtime);

	collection = mongoc_client_get_collection(client, db_name, ed->topic);
	if (!collection) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to get MongoDB collection");
		return MOSQ_ERR_UNKNOWN;
	}

	doc = bson_new();
	if (!doc) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to create BSON document");
		mongoc_collection_destroy(collection);
		return MOSQ_ERR_NOMEM;
	}

	bson_oid_init(&oid, NULL);
	BSON_APPEND_OID(doc, "_id", &oid);

	// Compile regex patterns
	if (regcomp(&regex_letters, "[a-z]+", REG_EXTENDED | REG_ICASE) != 0) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to compile letters regex");
		result = MOSQ_ERR_UNKNOWN;
		goto cleanup;
	}
	regex_letters_compiled = 1;

	if (regcomp(&regex_float, "^[+-]?[0-9]*\\.[0-9]+$", REG_EXTENDED) != 0) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to compile float regex");
		result = MOSQ_ERR_UNKNOWN;
		goto cleanup;
	}
	regex_float_compiled = 1;

	if (regcomp(&regex_int, "^[+-]?[0-9]+$", REG_EXTENDED) != 0) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to compile int regex");
		result = MOSQ_ERR_UNKNOWN;
		goto cleanup;
	}
	regex_int_compiled = 1;

	// If payload contains letters, store as string
	if (!regexec(&regex_letters, ed->payload, 0, NULL, 0)) {
		BSON_APPEND_UTF8(doc, "payload", ed->payload);
	}
	// If it has float format, store as double
	else if (!regexec(&regex_float, ed->payload, 0, NULL, 0)) {
		BSON_APPEND_DOUBLE(doc, "payload", atof(ed->payload));
	}
	// If it has int format, store as int
	else if (!regexec(&regex_int, ed->payload, 0, NULL, 0)) {
		BSON_APPEND_INT32(doc, "payload", atoi(ed->payload));
	}
	// Otherwise store as string
	else {
		BSON_APPEND_UTF8(doc, "payload", ed->payload);
	}

	BSON_APPEND_TIME_T(doc, "timestamp", rawtime);

	if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "MongoDB insert failed: %s", error.message);
		result = MOSQ_ERR_UNKNOWN;
	}

cleanup:
	// Free regex patterns
	if (regex_letters_compiled) {
		regfree(&regex_letters);
	}
	if (regex_float_compiled) {
		regfree(&regex_float);
	}
	if (regex_int_compiled) {
		regfree(&regex_int);
	}

	if (doc) {
		bson_destroy(doc);
	}
	if (collection) {
		mongoc_collection_destroy(collection);
	}

	return result;
}

int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
	int i;

	for (i = 0; i < supported_version_count; i++)
	{
		if (supported_versions[i] == 5)
		{
			return 5;
		}
	}
	return -1;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data, struct mosquitto_opt *opts, int opt_count)
{
	int i;

	UNUSED(user_data);

	mosq_pid = identifier;

	mongoc_init();

	// Parse configuration options
	for (i = 0; i < opt_count; i++) {
		if (!strcasecmp(opts[i].key, "mongodb_uri")) {
			mongodb_uri = mosquitto_strdup(opts[i].value);
			if (mongodb_uri == NULL) {
				mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to allocate memory for mongodb_uri");
				return MOSQ_ERR_NOMEM;
			}
		} else if (!strcasecmp(opts[i].key, "mongodb_database")) {
			mongodb_database = mosquitto_strdup(opts[i].value);
			if (mongodb_database == NULL) {
				mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to allocate memory for mongodb_database");
				if (mongodb_uri) {
					mosquitto_free(mongodb_uri);
					mongodb_uri = NULL;
				}
				return MOSQ_ERR_NOMEM;
			}
		}
	}

	if (mongodb_uri == NULL) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Error: MongoDB URI not configured, plugin cannot start");
		return MOSQ_ERR_INVAL;
	}

	client = mongoc_client_new(mongodb_uri);
	if (!client) {
		mosquitto_log_printf(MOSQ_LOG_ERR, "Failed to create MongoDB client");
		if (mongodb_uri) {
			mosquitto_free(mongodb_uri);
			mongodb_uri = NULL;
		}
		if (mongodb_database) {
			mosquitto_free(mongodb_database);
			mongodb_database = NULL;
		}
		return MOSQ_ERR_UNKNOWN;
	}

	mosquitto_log_printf(MOSQ_LOG_INFO, "MongoDB logger plugin initialized successfully");
	if (mongodb_database) {
		mosquitto_log_printf(MOSQ_LOG_INFO, "Using database: %s", mongodb_database);
	} else {
		mosquitto_log_printf(MOSQ_LOG_INFO, "Using default database: mqtt_data");
	}

	return mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL, NULL);
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *opts, int opt_count)
{
	UNUSED(user_data);
	UNUSED(opts);
	UNUSED(opt_count);

	if (client) {
		mongoc_client_destroy(client);
		client = NULL;
	}

	if (mongodb_uri) {
		mosquitto_free(mongodb_uri);
		mongodb_uri = NULL;
	}

	if (mongodb_database) {
		mosquitto_free(mongodb_database);
		mongodb_database = NULL;
	}

	mongoc_cleanup();

	mosquitto_log_printf(MOSQ_LOG_INFO, "MongoDB logger plugin cleanup completed");

	return mosquitto_callback_unregister(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL);
}
