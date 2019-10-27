#include "pulsar_ext.h"

typedef struct pulsar_rb_message_wrapper_t {
  pulsar_message_t *message;
} pulsar_rb_message_wrapper_t;

static void pulsar_rb_message_free(void *data) {
  pulsar_message_free(((pulsar_rb_message_wrapper_t*)data)->message);
  free(data);
}

static const rb_data_type_t pulsar_rb_message_t = {
  .wrap_struct_name = "Pulsar::Message",
  .function = {
    .dfree = pulsar_rb_message_free,
  },
  .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

static pulsar_message_t* pulsar_rb_message_ptr(VALUE self) {
  pulsar_rb_message_wrapper_t *rb_message;
  TypedData_Get_Struct(self, pulsar_rb_message_wrapper_t, &pulsar_rb_message_t, rb_message);
  return rb_message->message;
}

static VALUE pulsar_rb_message_alloc(VALUE klass) {
  pulsar_rb_message_wrapper_t *rb_message = malloc(sizeof(*rb_message));
  rb_message->message = NULL;
  return TypedData_Wrap_Struct(klass, &pulsar_rb_message_t, rb_message);
}

static VALUE pulsar_rb_message_topic(VALUE self) {
  // void -> String
  return rb_str_new_cstr(pulsar_message_get_topic_name(pulsar_rb_message_ptr(self)));
}

static VALUE pulsar_rb_message_id(VALUE self) {
  // void -> String
  // TODO: I'm not sure, but this may leak memory.
  return rb_str_new_cstr(pulsar_message_id_str(pulsar_message_get_message_id(pulsar_rb_message_ptr(self))));
}

static VALUE pulsar_rb_message_publish_timestamp(VALUE self) {
  // void -> Fixnum
  return ULONG2NUM(pulsar_message_get_publish_timestamp(pulsar_rb_message_ptr(self)));
}

static VALUE pulsar_rb_message_event_timestamp(VALUE self) {
  // void -> Fixnum
  return ULONG2NUM(pulsar_message_get_event_timestamp(pulsar_rb_message_ptr(self)));
}

static VALUE pulsar_rb_message_partition_key(VALUE self) {
  // void -> nilable(String)
  pulsar_message_t *message = pulsar_rb_message_ptr(self);
  if (!pulsar_message_has_partition_key(message)) {
    return Qnil;
  }
  return rb_str_new_cstr(pulsar_message_get_partitionKey(message));
}

static VALUE pulsar_rb_message_ordering_key(VALUE self) {
  // void -> nilable(String)
  pulsar_message_t *message = pulsar_rb_message_ptr(self);
  if (!pulsar_message_has_ordering_key(message)) {
    return Qnil;
  }
  return rb_str_new_cstr(pulsar_message_get_orderingKey(message));
}

static VALUE pulsar_rb_message_data(VALUE self) {
  // void -> String
  pulsar_message_t *message = pulsar_rb_message_ptr(self);
  return rb_str_new(pulsar_message_get_data(message), pulsar_message_get_length(message));
}

typedef struct pulsar_rb_consumer_wrapper_t {
  pulsar_consumer_t *consumer;
} pulsar_rb_consumer_wrapper_t;

static void pulsar_rb_consumer_free(void *data) {
  pulsar_consumer_free(((pulsar_rb_consumer_wrapper_t*)data)->consumer);
  free(data);
}

static const rb_data_type_t pulsar_rb_consumer_t = {
  .wrap_struct_name = "Pulsar::Consumer",
  .function = {
    .dfree = pulsar_rb_consumer_free,
  },
  .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

static pulsar_consumer_t* pulsar_rb_consumer_ptr(VALUE self) {
  pulsar_rb_consumer_wrapper_t *rb_consumer;
  TypedData_Get_Struct(self, pulsar_rb_consumer_wrapper_t, &pulsar_rb_consumer_t, rb_consumer);
  return rb_consumer->consumer;
}

static VALUE pulsar_rb_consumer_alloc(VALUE klass) {
  pulsar_rb_consumer_wrapper_t *rb_consumer = malloc(sizeof(*rb_consumer));
  rb_consumer->consumer = NULL;
  return TypedData_Wrap_Struct(klass, &pulsar_rb_consumer_t, rb_consumer);
}

static VALUE pulsar_rb_consumer_initialize(int argc, VALUE* argv, VALUE self) {
  pulsar_rb_consumer_wrapper_t *rb_consumer;
  TypedData_Get_Struct(self, pulsar_rb_consumer_wrapper_t, &pulsar_rb_consumer_t, rb_consumer);

  VALUE rb_client_v, rb_config_v;
  rb_scan_args_kw(RB_SCAN_ARGS_KEYWORDS, argc, argv, "1:", &rb_client_v, &rb_config_v);

  if (NIL_P(rb_config_v)) {
    rb_config_v = rb_hash_new();
  }

  pulsar_client_t *client = pulsar_rb_client_ptr(rb_client_v);

  VALUE topic = rb_hash_fetch(rb_config_v, ID2SYM(rb_intern("topic")));
  rb_check_type(topic, T_STRING);

  VALUE subscription_name = rb_hash_fetch(rb_config_v, ID2SYM(rb_intern("subscription")));
  rb_check_type(subscription_name, T_STRING);

  pulsar_consumer_configuration_t *config = pulsar_consumer_configuration_create();

  pulsar_result result = pulsar_client_subscribe(client, StringValueCStr(topic), StringValueCStr(subscription_name), config, &rb_consumer->consumer);

  pulsar_consumer_configuration_free(config);

  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to create consumer: %s", pulsar_result_str(result));
  }

  return self;
}

static VALUE pulsar_rb_consumer_close(VALUE self) {
  pulsar_result result = pulsar_consumer_close(pulsar_rb_consumer_ptr(self));
  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to close consumer: %s", pulsar_result_str(result));
  }
  return Qnil;
}

static VALUE pulsar_rb_consumer_topic(VALUE self) {
  // # void -> String
  return rb_str_new_cstr(pulsar_consumer_get_topic(pulsar_rb_consumer_ptr(self)));
}

static VALUE pulsar_rb_consumer_subscription(VALUE self) {
  // # void -> String
  return rb_str_new_cstr(pulsar_consumer_get_subscription_name(pulsar_rb_consumer_ptr(self)));
}

static VALUE pulsar_rb_consumer_unsubscribe(VALUE self) {
  pulsar_result result = pulsar_consumer_unsubscribe(pulsar_rb_consumer_ptr(self));
  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to unsubscribe: %s", pulsar_result_str(result));
  }

  // # void
  return Qnil;
}

static VALUE pulsar_rb_consumer_receive(VALUE self) {
  VALUE message_v = pulsar_rb_message_alloc(rb_cPulsarMessage);

  pulsar_rb_message_wrapper_t *rb_message;
  TypedData_Get_Struct(message_v, pulsar_rb_message_wrapper_t, &pulsar_rb_message_t, rb_message);

  pulsar_consumer_receive(pulsar_rb_consumer_ptr(self), &rb_message->message);
  return message_v;
}

static VALUE pulsar_rb_consumer_ack(VALUE self, VALUE message) {
  // # Message -> void
  pulsar_result result = pulsar_consumer_acknowledge(pulsar_rb_consumer_ptr(self), pulsar_rb_message_ptr(message));
  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to acknowledge message: %s", pulsar_result_str(result));
  }
  return Qnil;
}

static VALUE pulsar_rb_consumer_ack_id(VALUE self) {
  // # Integer -> void
  return Qnil;
}

static VALUE pulsar_rb_consumer_ack_cummulative(VALUE self, VALUE message) {
  // # Message -> void
  pulsar_result result = pulsar_consumer_acknowledge_cumulative(pulsar_rb_consumer_ptr(self), pulsar_rb_message_ptr(message));
  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to acknowledge comulative message: %s", pulsar_result_str(result));
  }
  return Qnil;
}

static VALUE pulsar_rb_consumer_nack(VALUE self, VALUE message) {
  // # Message -> void
  pulsar_consumer_negative_acknowledge(pulsar_rb_consumer_ptr(self), pulsar_rb_message_ptr(message));
  return Qnil;
}

static VALUE pulsar_rb_consumer_nack_id(VALUE self) {
  // # Integer -> void
  return Qnil;
}

VALUE rb_cPulsarConsumer;
VALUE rb_cPulsarMessage;
void InitConsumer(VALUE module) {
  rb_cPulsarMessage = rb_define_class_under(module, "Message", rb_cObject);
  rb_global_variable(&rb_cPulsarMessage);

  // Messages are only allocated in C land.
  rb_undef_alloc_func(rb_cPulsarMessage);

  rb_define_method(rb_cPulsarMessage, "topic", pulsar_rb_message_topic, 0);
  rb_define_method(rb_cPulsarMessage, "id", pulsar_rb_message_id, 0);
  rb_define_method(rb_cPulsarMessage, "publish_timestamp", pulsar_rb_message_publish_timestamp, 0);
  rb_define_method(rb_cPulsarMessage, "event_timestamp", pulsar_rb_message_event_timestamp, 0);
  rb_define_method(rb_cPulsarMessage, "partition_key", pulsar_rb_message_partition_key, 0);
  rb_define_method(rb_cPulsarMessage, "ordering_key", pulsar_rb_message_ordering_key, 0);
  rb_define_method(rb_cPulsarMessage, "data", pulsar_rb_message_data, 0);

  rb_cPulsarConsumer = rb_define_class_under(module, "Consumer", rb_cData);
  rb_global_variable(&rb_cPulsarConsumer);

  rb_define_alloc_func(rb_cPulsarConsumer, pulsar_rb_consumer_alloc);

  rb_define_method(rb_cPulsarConsumer, "initialize", pulsar_rb_consumer_initialize, -1);
  rb_define_method(rb_cPulsarConsumer, "close", pulsar_rb_consumer_close, 0);
  rb_define_method(rb_cPulsarConsumer, "topic", pulsar_rb_consumer_topic, 0);
  rb_define_method(rb_cPulsarConsumer, "subscription", pulsar_rb_consumer_subscription, 0);
  rb_define_method(rb_cPulsarConsumer, "unsubscribe", pulsar_rb_consumer_unsubscribe, -2);
  rb_define_method(rb_cPulsarConsumer, "receive", pulsar_rb_consumer_receive, -2);
  rb_define_method(rb_cPulsarConsumer, "ack", pulsar_rb_consumer_ack, 1);
  rb_define_method(rb_cPulsarConsumer, "ack_id", pulsar_rb_consumer_ack_id, -2);
  rb_define_method(rb_cPulsarConsumer, "ack_cummulative", pulsar_rb_consumer_ack_cummulative, 1);
  rb_define_method(rb_cPulsarConsumer, "nack", pulsar_rb_consumer_nack, 1);
  rb_define_method(rb_cPulsarConsumer, "nack_id", pulsar_rb_consumer_nack_id, -2);
}

