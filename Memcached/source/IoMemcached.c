#include "IoMemcached.h"

#define DATA(self) ((IoMemcachedData*) IoObject_dataPointer(self))

IoTag *IoMemcached_newTag(void *state)
{
	IoTag *tag = IoTag_newWithName_("Memcached");
	IoTag_state_(tag, state);
	IoTag_freeFunc_(tag, (IoTagFreeFunc *)IoMemcached_free);
	IoTag_cloneFunc_(tag, (IoTagCloneFunc *)IoMemcached_rawClone);
	return tag;
}

IoObject *IoMemcached_proto(void *state)
{
	IoMemcached *self = IoObject_new(state);
	IoObject_tag_(self, IoMemcached_newTag(state));

	IoObject_setDataPointer_(self, calloc(1, sizeof(IoMemcachedData)));

	IoState_registerProtoWithFunc_(state, self, IoMemcached_proto);

	{
		IoMethodTable methodTable[] = {
		{"addServer",  IoMemcached_addServer},
		{"set",        IoMemcached_set},
		{"add",        IoMemcached_add},
		{"replace",    IoMemcached_replace},
		{"append",     IoMemcached_append},
		{"prepend",    IoMemcached_prepend},
		{"get",        IoMemcached_get},
		{"getMulti",   IoMemcached_getMulti},
		{"delete",     IoMemcached_delete},
		{"flushAll",   IoMemcached_flushAll},
		{"incr",       IoMemcached_incr},
		{"decr",       IoMemcached_decr},
		{"stats",      IoMemcached_stats},
		{NULL, NULL},
		};
		IoObject_addMethodTable_(self, methodTable);
	}

	return self;
}

IoObject *IoMemcached_rawClone(IoMemcached *proto)
{
	IoObject *self = IoObject_rawClonePrimitive(proto);
	IoObject_setDataPointer_(self, calloc(1, sizeof(IoMemcachedData)));

	DATA(self)->mc = memcached_create(NULL);
	memcached_behavior_set(DATA(self)->mc, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
	memcached_behavior_set(DATA(self)->mc, MEMCACHED_BEHAVIOR_HASH, MEMCACHED_HASH_FNV1A_32);
	memcached_behavior_set(DATA(self)->mc, MEMCACHED_BEHAVIOR_DISTRIBUTION, MEMCACHED_DISTRIBUTION_CONSISTENT);
	memcached_behavior_set(DATA(self)->mc, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 0);

	return self;
}

IoObject *IoMemcached_new(void *state)
{
	IoObject *proto = IoState_protoWithInitFunction_(state, IoMemcached_proto);
	return IOCLONE(proto);
}

void IoMemcached_free(IoMemcached *self)
{
	memcached_free(DATA(self)->mc);
	free(DATA(self));
}

// addServer
IoObject *IoMemcached_addServer(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	memcached_server_st *server;

	server = memcached_servers_parse(IoMessage_locals_cStringArgAt_(m, locals, 0));
	memcached_server_push(DATA(self)->mc, server);

	memcached_server_list_free(server);

	return self;
}

// Storage commands
IoObject *IoMemcached_set(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key   = IoMessage_locals_seqArgAt_(m, locals, 0);
	IoSeq *value = IoMessage_locals_seqArgAt_(m, locals, 1);

	time_t expiration = IoMessage_argCount(m) == 3 ? IoMessage_locals_longArgAt_(m, locals, 2) : 0;

	memcached_return rc;
	rc = memcached_set(DATA(self)->mc,
		CSTRING(key),   IOSEQ_LENGTH(key),
		CSTRING(value), IOSEQ_LENGTH(value),
		expiration, 0
	);

	if(rc != MEMCACHED_SUCCESS)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	return IOSTATE->ioTrue;
}

IoObject *IoMemcached_add(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key   = IoMessage_locals_seqArgAt_(m, locals, 0);
	IoSeq *value = IoMessage_locals_seqArgAt_(m, locals, 1);

	time_t expiration = IoMessage_argCount(m) == 3 ? IoMessage_locals_longArgAt_(m, locals, 2) : 0;

	memcached_return rc;
	rc = memcached_add(DATA(self)->mc,
		CSTRING(key),   IOSEQ_LENGTH(key),
		CSTRING(value), IOSEQ_LENGTH(value),
		expiration, 0
	);

	if(rc != MEMCACHED_SUCCESS && rc != MEMCACHED_NOTSTORED)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	// MEMCACHED_NOTSTORED is a legitmate error in the case of a collision.
	if(rc == MEMCACHED_NOTSTORED)
		return IOSTATE->ioFalse;

	return IOSTATE->ioTrue; // MEMCACHED_SUCCESS
}

IoObject *IoMemcached_replace(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key   = IoMessage_locals_seqArgAt_(m, locals, 0);
	IoSeq *value = IoMessage_locals_seqArgAt_(m, locals, 1);

	time_t expiration = IoMessage_argCount(m) == 3 ? IoMessage_locals_longArgAt_(m, locals, 2) : 0;

	memcached_return rc;
	rc = memcached_replace(DATA(self)->mc,
		CSTRING(key),   IOSEQ_LENGTH(key),
		CSTRING(value), IOSEQ_LENGTH(value),
		expiration, 0
	);

	if(rc != MEMCACHED_SUCCESS && rc != MEMCACHED_NOTSTORED)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	// MEMCACHED_NOTSTORED is a legitmate error in the case of a collision.
	if(rc == MEMCACHED_NOTSTORED)
		return IOSTATE->ioFalse;

	return IOSTATE->ioTrue; // MEMCACHED_SUCCESS
}

// Requires memcached 1.2.4+. Untested.
IoObject *IoMemcached_append(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key   = IoMessage_locals_seqArgAt_(m, locals, 0);
	IoSeq *value = IoMessage_locals_seqArgAt_(m, locals, 1);

	memcached_return rc;
	rc = memcached_append(DATA(self)->mc,
		CSTRING(key),   IOSEQ_LENGTH(key),
		CSTRING(value), IOSEQ_LENGTH(value),
		0, 0
	);

	if(rc != MEMCACHED_SUCCESS)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	return IOSTATE->ioTrue;
}

// Requires memcached 1.2.4+. Untested.
IoObject *IoMemcached_prepend(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key   = IoMessage_locals_seqArgAt_(m, locals, 0);
	IoSeq *value = IoMessage_locals_seqArgAt_(m, locals, 1);

	memcached_return rc;
	rc = memcached_prepend(DATA(self)->mc,
		CSTRING(key),   IOSEQ_LENGTH(key),
		CSTRING(value), IOSEQ_LENGTH(value),
		0, 0
	);

	if(rc != MEMCACHED_SUCCESS)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	return IOSTATE->ioTrue;
}

// Retrieval commands
IoObject *IoMemcached_get(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	size_t size;
	uint32_t flags;
	memcached_return rc;

	IoObject *key = IoMessage_locals_seqArgAt_(m, locals, 0);

	unsigned char *value;
	value = (unsigned char*) memcached_get(DATA(self)->mc,
		CSTRING(key), IOSEQ_LENGTH(key),
		&size, &flags, &rc
	);

	if(value == NULL)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	IoObject *result = IOSEQ(value, size);

	free(value);

	return result;
}

IoObject *IoMemcached_getMulti(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoList *keys_list = IoMessage_locals_listArgAt_(m, locals, 0);
	size_t keys_list_size = IoList_rawSize(keys_list);

	IoObject *results_map = IoMap_new(IOSTATE);

	if(keys_list_size == 0)
		return results_map;

	int i;
	for(i = 0; i < keys_list_size; i++) {
		IoSeq *key = IoList_rawAt_(keys_list, i);
		IOASSERT(ISSEQ(key), "key must be a Sequence");
		IOASSERT(IOSEQ_LENGTH(key) > 0, "key cannot be an empty Sequence");
		IOASSERT(IOSEQ_LENGTH(key) < MEMCACHED_MAX_KEY, "key is too long");
	}

	char **ckeys = (char **) malloc(sizeof(char *) * keys_list_size);
	size_t *ckey_lengths = (size_t *) malloc(sizeof(size_t) * keys_list_size);

	for(i = 0; i < keys_list_size; i++) {
		ckeys[i] = CSTRING(IoList_rawAt_(keys_list, i));
		ckey_lengths[i] = strlen(ckeys[i]);
	}

	memcached_return rc = memcached_mget(DATA(self)->mc, ckeys, ckey_lengths, keys_list_size);

	free(ckeys);
	free(ckey_lengths);

	char returned_key[MEMCACHED_MAX_KEY], *returned_value;
	size_t returned_key_length, returned_value_length;
	uint32_t flags;

	returned_value = memcached_fetch(DATA(self)->mc,
		returned_key, &returned_key_length,
		&returned_value_length, &flags, &rc
	);

	while(returned_value != NULL) {
		IoMap_rawAtPut(results_map,
			IoSeq_newSymbolWithData_length_(IOSTATE, returned_key, returned_key_length),
			IOSEQ((unsigned char *) returned_value, returned_value_length)
		);

		free(returned_value);

		returned_value = memcached_fetch(DATA(self)->mc,
			returned_key, &returned_key_length,
			&returned_value_length, &flags, &rc
		);
	}

	return results_map;
}

// Delete and flushAll
IoObject *IoMemcached_delete(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key = IoMessage_locals_seqArgAt_(m, locals, 0);

	time_t expiration = IoMessage_argCount(m) == 2 ? IoMessage_locals_longArgAt_(m, locals, 1) : 0;

	memcached_return rc;
	rc = memcached_delete(DATA(self)->mc,
		CSTRING(key), IOSEQ_LENGTH(key),
		expiration
	);

	if(rc != MEMCACHED_SUCCESS && rc != MEMCACHED_NOTFOUND)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	if(rc == MEMCACHED_NOTFOUND)
		return IOSTATE->ioFalse;

	return IOSTATE->ioTrue; // MEMCACHED_SUCCESS
}

IoObject *IoMemcached_flushAll(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	time_t expiration = IoMessage_argCount(m) == 1 ? IoMessage_locals_intArgAt_(m, locals, 0) : 0;
	memcached_flush(DATA(self)->mc, expiration); // always returns ok
	return self;
}

// Increment/Decrement
IoObject *IoMemcached_incr(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key = IoMessage_locals_seqArgAt_(m, locals, 0);

	uint32_t offset = IoMessage_argCount(m) == 2 ? IoMessage_locals_intArgAt_(m, locals, 1) : 1;

	uint64_t new_value;

	memcached_return rc;
	rc = memcached_increment(DATA(self)->mc,
		CSTRING(key), IOSEQ_LENGTH(key),
		offset, &new_value
	);

	if(rc != MEMCACHED_SUCCESS)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	return IONUMBER(new_value);
}

IoObject *IoMemcached_decr(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoSeq *key = IoMessage_locals_seqArgAt_(m, locals, 0);

	uint32_t offset = IoMessage_argCount(m) == 2 ? IoMessage_locals_intArgAt_(m, locals, 1) : 1;

	uint64_t new_value;

	memcached_return rc;
	rc = memcached_decrement(DATA(self)->mc,
		CSTRING(key), IOSEQ_LENGTH(key),
		offset, &new_value
	);

	if(rc != MEMCACHED_SUCCESS)
		IoState_error_(IOSTATE, m, memcached_strerror(DATA(self)->mc, rc));

	return IONUMBER(new_value);
}

// Stats
IoObject *IoMemcached_stats(IoMemcached *self, IoObject *locals, IoMessage *m)
{
	IoMap *results_map = IoMap_new(IOSTATE);

	int i;
	for(i = 0; i < memcached_server_list_count(DATA(self)->mc->hosts); i++) {
		memcached_server_st *server = DATA(self)->mc->hosts + i;

		memcached_stat_st stats;
		if(memcached_stat_servername(&stats, "", server->hostname, server->port) != 0)
			continue;

		memcached_return rc;
		char **ckeys = memcached_stat_get_keys(DATA(self)->mc, &stats, &rc);

		int ckeys_count = 0;
		while(ckeys[ckeys_count] != NULL)
			ckeys_count++;

		IoMap *per_server_map = IoMap_new(IOSTATE);
		int k;
		for(k = 0; k < ckeys_count; k++) {
			char *ckey = ckeys[k];
			char *cvalue = memcached_stat_get_value(DATA(self)->mc, &stats, ckey, &rc);
			IoMap_rawAtPut(per_server_map, IOSYMBOL(ckey), IOSYMBOL(cvalue));
			free(cvalue);
		}

		free(ckeys);

		// "127.0.0.1:11211"
		char *server_key = (char *) malloc((strlen(server->hostname) + 1 + 5 + 1) * sizeof(char));
		sprintf(server_key, "%s:%d", server->hostname, server->port);

		IoMap_rawAtPut(results_map, IOSYMBOL(server_key), per_server_map);
		free(server_key);
	}

	return results_map;
}
