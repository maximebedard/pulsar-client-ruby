#include "pulsar_ext.h"

typedef struct pulsarRbConsumer {
  pulsar_consumer_t *consumer;
} pulsarRbConsumer;

static pulsar_consumer_t* pulsar_rb_consumer_ptr(VALUE instance) {
  pulsarRbConsumer *rb_consumer;
  Data_Get_Struct(instance, pulsarRbConsumer, rb_consumer);
  return rb_consumer->consumer;
}

static void pulsar_rb_consumer_mark(pulsarRbConsumer *rb_consumer) {
}

static void pulsar_rb_consumer_free(pulsarRbConsumer *rb_consumer) {
  pulsar_consumer_free(rb_consumer->consumer);
  free(rb_consumer);
}

static VALUE pulsar_rb_consumer_alloc(VALUE klass) {
  pulsarRbConsumer *rb_consumer = malloc(sizeof(*rb_consumer));
  rb_consumer->consumer = NULL;
  return Data_Wrap_Struct(klass, pulsar_rb_consumer_mark, pulsar_rb_consumer_free, rb_consumer);
}

static VALUE pulsar_rb_consumer_initialize(int argc, VALUE* argv, VALUE self) {
  VALUE rb_client_v, rb_config_v;
  rb_scan_args_kw(RB_SCAN_ARGS_KEYWORDS, argc, argv, "1:", &rb_client_v, &rb_config_v);

  // TODO: type check rb_client_v;

  if (NIL_P(rb_config_v)) {
    rb_config_v = rb_hash_new();
  }

  VALUE topic = rb_hash_fetch(rb_config_v, ID2SYM(rb_intern("topic")));
  Check_Type(topic, T_STRING);

  VALUE subscription_name = rb_hash_fetch(rb_config_v, ID2SYM(rb_intern("subscription")));
  Check_Type(subscription_name, T_STRING);

  pulsar_consumer_configuration_t *config = pulsar_consumer_configuration_create();

  pulsar_client_t *client = pulsar_rb_client_ptr(rb_client_v);

  pulsarRbConsumer *rb_consumer;
  Data_Get_Struct(self, pulsarRbConsumer, rb_consumer);

  pulsar_result result = pulsar_client_subscribe(client, StringValueCStr(topic), StringValueCStr(subscription_name), config, &rb_consumer->consumer);

  pulsar_consumer_configuration_free(config);

  if (result != pulsar_result_Ok) {
    rb_raise(rb_ePulsarError, "failed to create consumer");
  }

  return self;
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
    rb_raise(rb_ePulsarError, "failed to unsubscribe");
  }

  // # void
  return Qnil;
}

static VALUE pulsar_rb_consumer_receive(VALUE self) {
  // VALUE message_rb = rb_newobj_of(rb_cPulsarConsumer, 0);


  // pulsar_message_t *message;
  // pulsar_consumer_receive(consume, ..., &message)



  // rb_new(rb_intern(""))
  // # void -> Message
  return Qnil;
}

static VALUE pulsar_rb_consumer_ack(VALUE self) {
  // # Message -> void
  return Qnil;
}

static VALUE pulsar_rb_consumer_ack_id(VALUE self) {
  // # Integer -> void
  return Qnil;
}

static VALUE pulsar_rb_consumer_ack_cummulative(VALUE self) {
  // # Message -> void
  return Qnil;
}

static VALUE pulsar_rb_consumer_nack(VALUE self) {
  // # Message -> void
  return Qnil;
}

static VALUE pulsar_rb_consumer_nack_id(VALUE self) {
  // # Integer -> void
  return Qnil;
}

VALUE rb_cPulsarConsumer;
void InitConsumer(VALUE module) {
  rb_cPulsarConsumer = rb_define_class_under(module, "Consumer", rb_cObject);
  rb_global_variable(&rb_cPulsarConsumer);

  rb_define_alloc_func(rb_cPulsarConsumer, pulsar_rb_consumer_alloc);

  rb_define_method(rb_cPulsarConsumer, "initialize", pulsar_rb_consumer_initialize, -1);
  rb_define_method(rb_cPulsarConsumer, "topic", pulsar_rb_consumer_topic, -2);
  rb_define_method(rb_cPulsarConsumer, "subscription", pulsar_rb_consumer_subscription, -2);
  rb_define_method(rb_cPulsarConsumer, "unsubscribe", pulsar_rb_consumer_unsubscribe, -2);
  rb_define_method(rb_cPulsarConsumer, "receive", pulsar_rb_consumer_receive, -2);
  rb_define_method(rb_cPulsarConsumer, "ack", pulsar_rb_consumer_ack, -2);
  rb_define_method(rb_cPulsarConsumer, "ack_id", pulsar_rb_consumer_ack_id, -2);
  rb_define_method(rb_cPulsarConsumer, "ack_cummulative", pulsar_rb_consumer_ack_cummulative, -2);
  rb_define_method(rb_cPulsarConsumer, "nack", pulsar_rb_consumer_nack, -2);
  rb_define_method(rb_cPulsarConsumer, "nack_id", pulsar_rb_consumer_nack_id, -2);
}

