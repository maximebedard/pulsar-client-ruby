#include "pulsar_ext.h"

typedef struct pulsar_rb_producer_wrapper_t {
  pulsar_producer_t *producer;
} pulsar_rb_producer_wrapper_t;

static void pulsar_rb_producer_free(void *data) {
  pulsar_producer_free(((pulsar_rb_producer_wrapper_t*)data)->producer);
  free(data);
}

static const rb_data_type_t pulsar_rb_producer_t = {
  .wrap_struct_name = "Pulsar::Producer",
  .function = {
    .dfree = pulsar_rb_producer_free,
  },
  .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

static pulsar_producer_t* pulsar_rb_producer_ptr(VALUE self) {
  pulsar_rb_producer_wrapper_t *rb_producer;
  TypedData_Get_Struct(self, pulsar_rb_producer_wrapper_t, &pulsar_rb_producer_t, rb_producer);
  return rb_producer->producer;
}

static VALUE pulsar_rb_producer_alloc(VALUE klass) {
  pulsar_rb_producer_wrapper_t *rb_producer = malloc(sizeof(*rb_producer));
  rb_producer->producer = NULL;
  return TypedData_Wrap_Struct(klass, &pulsar_rb_producer_t, rb_producer);
}

static VALUE pulsar_rb_producer_initialize(int argc, VALUE* argv, VALUE self) {
  pulsar_rb_producer_wrapper_t *rb_producer;
  TypedData_Get_Struct(self, pulsar_rb_producer_wrapper_t, &pulsar_rb_producer_t, rb_producer);

  VALUE rb_client_v, rb_config_v;
  rb_scan_args_kw(RB_SCAN_ARGS_KEYWORDS, argc, argv, "1:", &rb_client_v, &rb_config_v);

  if (NIL_P(rb_config_v)) {
    rb_config_v = rb_hash_new();
  }

  pulsar_client_t *client = pulsar_rb_client_ptr(rb_client_v);

  VALUE topic = rb_hash_fetch(rb_config_v, ID2SYM(rb_intern("topic")));
  Check_Type(topic, T_STRING);

  VALUE name = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("name")));
  VALUE routing_mode = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("routing_mode")));
  VALUE hashing_scheme = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("hashing_scheme")));
  VALUE compression_type = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("compression_type")));
  VALUE send_timeout_ms = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("send_timeout_ms")));;
  VALUE max_pending_messages = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("max_pending_messages")));;
  VALUE max_pending_messages_across_partitions = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("max_pending_messages_across_partitions")));;
  VALUE block_if_queue_full = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("block_if_queue_full")));;
  VALUE batching_enabled = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("batching_enabled")));;
  VALUE batching_max_publish_delay = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("batching_max_publish_delay")));;
  VALUE batching_max_messages = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("batching_max_messages")));;

  // TODO: schema, properties, message_router

  // TODO: fix memory leak here if there's an error parsing the arguments.
  pulsar_producer_configuration_t *config = pulsar_producer_configuration_create();

  if (RB_TYPE_P(name, T_STRING) && RSTRING_LEN(name) > 0) {
    pulsar_producer_configuration_set_producer_name(config, StringValueCStr(name));
  }

  // is it possible to have constant values to do a switch case instead?
  if (SYMBOL_P(routing_mode)) {
    ID routing_mode_id = SYM2ID(routing_mode);
    if (routing_mode_id == rb_intern("round_robin")) {
      pulsar_producer_configuration_set_partitions_routing_mode(config, pulsar_RoundRobinDistribution);
    } else if (routing_mode_id == rb_intern("single_partition")) {
      pulsar_producer_configuration_set_partitions_routing_mode(config, pulsar_UseSinglePartition);
    } else if (routing_mode_id == rb_intern("custom_partition")) {
      pulsar_producer_configuration_set_partitions_routing_mode(config, pulsar_CustomPartition);
    }
  }

  // is it possible to have constant values to do a switch case instead?
  if (SYMBOL_P(hashing_scheme)) {
    ID hashing_scheme_id = SYM2ID(hashing_scheme);
    if (hashing_scheme_id == rb_intern("java_string")) {
      pulsar_producer_configuration_set_hashing_scheme(config, pulsar_JavaStringHash);
    } else if (hashing_scheme_id == rb_intern("murmur32")) {
      pulsar_producer_configuration_set_hashing_scheme(config, pulsar_Murmur3_32Hash);
    } else if (hashing_scheme_id == rb_intern("boost")) {
      pulsar_producer_configuration_set_hashing_scheme(config, pulsar_BoostHash);
    }
  }

  // is it possible to have constant values to do a switch case instead?
  if (SYMBOL_P(compression_type)) {
    ID compression_type_id = SYM2ID(compression_type);
    if (compression_type_id == rb_intern("lz4")) {
      pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionLZ4);
    } else if (compression_type_id == rb_intern("zlib")) {
      pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionZLib);
    } else if (compression_type_id == rb_intern("zstd")) {
      // TODO: zstd compression
      // pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionZstd);
    } else if (compression_type_id == rb_intern("snappy")) {
      // TODO: snappy compression
      // pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionSnappy);
    }
  }

  if (FIXNUM_P(send_timeout_ms)) {
    pulsar_producer_configuration_set_send_timeout(config, NUM2INT(send_timeout_ms));
  }

  if (FIXNUM_P(max_pending_messages)) {
    pulsar_producer_configuration_set_max_pending_messages(config, NUM2INT(max_pending_messages));
  }

  if (FIXNUM_P(max_pending_messages_across_partitions)) {
    pulsar_producer_configuration_set_max_pending_messages_across_partitions(config, NUM2INT(max_pending_messages_across_partitions));
  }

  if (RTEST(block_if_queue_full)) {
    pulsar_producer_configuration_set_block_if_queue_full(config, 1);
  }

  if (RTEST(batching_enabled)) {
    pulsar_producer_configuration_set_batching_enabled(config, 1);
  }

  if (FIXNUM_P(batching_max_publish_delay)) {
    pulsar_producer_configuration_set_batching_max_publish_delay_ms(config, NUM2INT(batching_max_publish_delay));
  }

  if (FIXNUM_P(batching_max_messages)) {
    pulsar_producer_configuration_set_batching_max_messages(config, NUM2UINT(batching_max_messages));
  }

  pulsar_result result = pulsar_client_create_producer(client, StringValueCStr(topic), config, &rb_producer->producer);

  pulsar_producer_configuration_free(config);

  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to create producer: %s", pulsar_result_str(result));
  }
  return self;
}

static VALUE pulsar_rb_producer_topic(VALUE self, VALUE args) {
  // void -> String
  return rb_str_new_cstr(pulsar_producer_get_topic(pulsar_rb_producer_ptr(self)));
}

static VALUE pulsar_rb_producer_name(VALUE self, VALUE args) {
  // void -> String
  return rb_str_new_cstr(pulsar_producer_get_producer_name(pulsar_rb_producer_ptr(self)));
}

static VALUE pulsar_rb_producer_produce(int argc, VALUE* argv, VALUE self) {
  VALUE rb_kwargs;
  rb_scan_args_kw(RB_SCAN_ARGS_KEYWORDS, argc, argv, ":", &rb_kwargs);

  VALUE event_timestamp = rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("event_timestamp")));
  VALUE sequence_id = rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("sequence_id")));
  VALUE partition_key = rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("partition_key")));
  VALUE ordering_key = rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("ordering_key")));
  VALUE data = rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("data")));

  pulsar_message_t *message = pulsar_message_create();

  if (FIXNUM_P(event_timestamp)) {
    pulsar_message_set_event_timestamp(message, NUM2ULL(event_timestamp));
  }

  if (FIXNUM_P(sequence_id)) {
    pulsar_message_set_sequence_id(message, NUM2LONG(sequence_id));
  }

  if (RB_TYPE_P(partition_key, T_STRING) && RSTRING_LEN(partition_key) > 0) {
    pulsar_message_set_partition_key(message, StringValueCStr(partition_key));
  }

  if (RB_TYPE_P(ordering_key, T_STRING) && RSTRING_LEN(ordering_key) > 0) {
    pulsar_message_set_ordering_key(message, StringValueCStr(ordering_key));
  }

  if (RB_TYPE_P(data, T_STRING) && RSTRING_LEN(data) > 0) {
    pulsar_message_set_content(message, StringValuePtr(data), RSTRING_LEN(data));
  }

  pulsar_result result = pulsar_producer_send(pulsar_rb_producer_ptr(self), message);

  pulsar_message_free(message);

  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to produce message: %s", pulsar_result_str(result));
  }

  // Message -> void
  return Qnil;
}

static VALUE pulsar_rb_producer_close(VALUE self) {
  pulsar_result result = pulsar_producer_close(pulsar_rb_producer_ptr(self));
  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to close producer: %s", pulsar_result_str(result));
  }
  return Qnil;
}

static VALUE pulsar_rb_producer_produce_async(int argc, VALUE* argv, VALUE self) {
  // (Message, block) -> void
  return Qnil;
}

static VALUE pulsar_rb_producer_last_sequence_id(VALUE self) {
  // void -> Integer
  return LONG2NUM(pulsar_producer_get_last_sequence_id(pulsar_rb_producer_ptr(self)));
}

static VALUE pulsar_rb_producer_flush(VALUE self) {
  pulsar_result result = pulsar_producer_flush(pulsar_rb_producer_ptr(self));
  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to flush producer: %s", pulsar_result_str(result));
  }
  return Qnil;
}

VALUE rb_cPulsarProducer;
void InitProducer(VALUE module) {
  rb_cPulsarProducer = rb_define_class_under(module, "Producer", rb_cData);
  rb_global_variable(&rb_cPulsarProducer);

  rb_define_alloc_func(rb_cPulsarProducer, pulsar_rb_producer_alloc);

  rb_define_method(rb_cPulsarProducer, "initialize", pulsar_rb_producer_initialize, -1);
  rb_define_method(rb_cPulsarProducer, "close", pulsar_rb_producer_close, 0);
  rb_define_method(rb_cPulsarProducer, "topic", pulsar_rb_producer_topic, 0);
  rb_define_method(rb_cPulsarProducer, "name", pulsar_rb_producer_name, 0);
  rb_define_method(rb_cPulsarProducer, "produce", pulsar_rb_producer_produce, -1);
  rb_define_method(rb_cPulsarProducer, "produce_async", pulsar_rb_producer_produce_async, -1);
  rb_define_method(rb_cPulsarProducer, "last_sequence_id", pulsar_rb_producer_last_sequence_id, 0);
  rb_define_method(rb_cPulsarProducer, "flush", pulsar_rb_producer_flush, 0);
}
