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
mongoc_client_t *client;


/**
 * @brief Salva a publicação em um banco de dados mongodb
 * @description Salva a publicação em um banco de dados mongodb, levando em consideração
 *              o tipo de dados carregado no payload
 *              No momento, verifica se o payload é um float, int ou string e salva no banco com o formato apropriado
 *              Também salva um timestamp referente ao momento que a publicação foi tratada aqui no broker MQTT
 * 
 * @param event 
 * @param event_data 
 * @param userdata 
 * @return int 
 */
static int callback_message(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_message *ed = event_data;

	UNUSED(event);
	UNUSED(userdata);

	time_t rawtime;

	mongoc_collection_t *collection;
	bson_error_t error;
	bson_oid_t oid;
	bson_t *doc;

	regex_t regex_letras;
	regex_t regex_float;
	regex_t regex_int;

	// informações de datetime em UTC
	time(&rawtime);

	collection = mongoc_client_get_collection(client, "tcc", ed->topic);
	doc = bson_new();
	bson_oid_init(&oid, NULL);
	BSON_APPEND_OID(doc, "_id", &oid);

	regcomp(&regex_letras, "[a-z]+", REG_EXTENDED | REG_ICASE);
	regcomp(&regex_float, "^[+-]?[0-9]*\\.[0-9]+$", REG_EXTENDED);
	regcomp(&regex_int, "^[+-]?[0-9]+$", REG_EXTENDED);

	// se o payload contiver alguma letra, armazena como string
	if (!regexec(&regex_letras, ed->payload, 0, NULL, 0))
	{
		mosquitto_log_printf(MOSQ_LOG_WARNING, "Tem letras");
		BSON_APPEND_UTF8(doc, "payload", ed->payload);
	}
	// se for tiver formato de float, como float
	else if (!regexec(&regex_float, ed->payload, 0, NULL, 0))
	{
		BSON_APPEND_DOUBLE(doc, "payload", atof(ed->payload));
	}
	// se for tiver formato de int, como int
	else if (!regexec(&regex_int, ed->payload, 0, NULL, 0))
	{
		BSON_APPEND_INT32(doc, "payload", atoi(ed->payload));
	}
	// caso contrário armazena como string
	else
	{
		BSON_APPEND_UTF8(doc, "payload", ed->payload);
	}

	BSON_APPEND_TIME_T(doc, "timestamp", rawtime);

	if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error))
	{
		fprintf(stderr, "%s\n", error.message);
	}


	bson_destroy(doc);
	mongoc_collection_destroy(collection);

	return MOSQ_ERR_SUCCESS;
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
	UNUSED(user_data);

	mosq_pid = identifier;

	mongoc_init();


	for (int i = 0; i < opt_count; i++)
	{
		if (!strcasecmp(opts[i].key, "mongodb_uri"))
		{
			mongodb_uri = mosquitto_strdup(opts[i].value);
			if (mongodb_uri == NULL)
			{
				return MOSQ_ERR_NOMEM;
			}
			break;
		}
	}
	if (mongodb_uri == NULL)
	{
		mosquitto_log_printf(MOSQ_LOG_WARNING, "Warning: Não foi definida uma url de configuração para o mongodb, o plugin não pode ser iniciado");
		return MOSQ_ERR_SUCCESS;
	}

	client = mongoc_client_new(mongodb_uri);

	mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL, NULL);

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *opts, int opt_count)
{
	UNUSED(user_data);
	UNUSED(opts);
	UNUSED(opt_count);

	mongoc_client_destroy(client);
	mongoc_cleanup();

	return mosquitto_callback_unregister(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL);
}
