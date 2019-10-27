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
  rb_check_type(topic, T_STRING);

  pulsar_producer_configuration_t *config = pulsar_producer_configuration_create();

  VALUE name = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("name")));
  if (!NIL_P(name) && RB_TYPE_P(name, T_STRING) && RSTRING_LEN(name) > 0) {
    pulsar_producer_configuration_set_producer_name(config, StringValueCStr(name));
  }
  // TODO: raise type error if not string

  VALUE routing_mode = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("routing_mode")));
  // is it possible to have constant values to do a switch case instead?
  if (!NIL_P(routing_mode)) {
    if (SYM2ID(routing_mode) == rb_intern("round_robin")) {
      pulsar_producer_configuration_set_partitions_routing_mode(config, pulsar_RoundRobinDistribution);
    } else if (SYM2ID(routing_mode) == rb_intern("single_partition")) {
      pulsar_producer_configuration_set_partitions_routing_mode(config, pulsar_UseSinglePartition);
    } else if (SYM2ID(routing_mode) == rb_intern("custom_partition")) {
      pulsar_producer_configuration_set_partitions_routing_mode(config, pulsar_CustomPartition);
    }
  }

  VALUE hashing_algorithm = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("hashing_algorithm")));
  // is it possible to have constant values to do a switch case instead?
  if (!NIL_P(hashing_algorithm)) {
    if (SYM2ID(hashing_algorithm) == rb_intern("java_string")) {
      pulsar_producer_configuration_set_hashing_scheme(config, pulsar_JavaStringHash);
    } else if (SYM2ID(hashing_algorithm) == rb_intern("murmur32")) {
      pulsar_producer_configuration_set_hashing_scheme(config, pulsar_Murmur3_32Hash);
    } else if (SYM2ID(hashing_algorithm) == rb_intern("boost")) {
      pulsar_producer_configuration_set_hashing_scheme(config, pulsar_BoostHash);
    }
  }

  VALUE compressor = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("compressor")));
  // is it possible to have constant values to do a switch case instead?
  if (!NIL_P(compressor)) {
    if (SYM2ID(compressor) == rb_intern("lz4")) {
      pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionLZ4);
    } else if (SYM2ID(compressor) == rb_intern("zlib")) {
      pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionZLib);
    } else if (SYM2ID(compressor) == rb_intern("zstd")) {
      // TODO: zstd compression
      // pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionZstd);
    } else if (SYM2ID(compressor) == rb_intern("snappy")) {
      // TODO: snappy compression
      // pulsar_producer_configuration_set_compression_type(config, pulsar_CompressionSnappy);
    }
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

static int rb_string_present(VALUE v) {
  return !NIL_P(v) && RB_TYPE_P(v, T_STRING) && RSTRING_LEN(v) > 0;
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

  if (rb_string_present(partition_key)) {
    pulsar_message_set_partition_key(message, StringValueCStr(partition_key));
  }

  if (rb_string_present(ordering_key)) {
    pulsar_message_set_ordering_key(message, StringValueCStr(ordering_key));
  }

  if (rb_string_present(data)) {
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
}
