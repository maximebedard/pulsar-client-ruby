#include "pulsar_ext.h"

typedef struct pulsarRbMessage {
  pulsar_message_t *message;
} pulsarRbMessage;

pulsar_message_t* pulsar_rb_message_ptr(VALUE i) {
  pulsarRbMessage *rb_message;
  Data_Get_Struct(i, pulsarRbMessage, rb_message);
  return rb_message->message;
}

static void pulsar_rb_message_mark(pulsarRbMessage *rb_message) {
}

static void pulsar_rb_message_free(pulsarRbMessage *rb_message) {
  pulsar_message_free(rb_message->message);
  free(rb_message);
}

// VALUE pulsar_rb_message_new() {
//   return pulsar_rb_message_alloc(rb_newobj_of(rb_cPulsarConsumer, 0));
// }

static VALUE pulsar_rb_message_alloc(VALUE klass) {
  pulsarRbMessage *rb_message = malloc(sizeof(*rb_message));
  rb_message->message = NULL;
  return Data_Wrap_Struct(klass, pulsar_rb_message_mark, pulsar_rb_message_free, rb_message);
}

static int presentStringValue(VALUE v) {
  return !NIL_P(v) && RB_TYPE_P(v, T_STRING) && RSTRING_LEN(v) > 0;
}

static VALUE pulsar_rb_message_initialize(int argc, VALUE* argv, VALUE self) {
  VALUE rb_kwargs;
  rb_scan_args_kw(RB_SCAN_ARGS_KEYWORDS, argc, argv, ":", &rb_kwargs);

  VALUE event_timestamp = rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("event_timestamp")));
  VALUE sequence_id = rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("sequence_id")));
  VALUE partition_key =  rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("partition_key")));
  VALUE ordering_key =  rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("ordering_key")));
  VALUE data =  rb_hash_aref(rb_kwargs, ID2SYM(rb_intern("data")));

  pulsarRbMessage *rb_message;
  Data_Get_Struct(self, pulsarRbMessage, rb_message);

  rb_message->message = pulsar_message_create();

  if (FIXNUM_P(event_timestamp)) {
    pulsar_message_set_event_timestamp(rb_message->message, NUM2LONG(event_timestamp));
  }

  if (FIXNUM_P(sequence_id)) {
    pulsar_message_set_sequence_id(rb_message->message, NUM2LONG(sequence_id));
  }

  if (presentStringValue(partition_key)) {
    pulsar_message_set_partition_key(rb_message->message, StringValueCStr(partition_key));
  }

  if (presentStringValue(ordering_key)) {
    pulsar_message_set_ordering_key(rb_message->message, StringValueCStr(ordering_key));
  }

  if (presentStringValue(data)) {
    pulsar_message_set_content(rb_message->message, StringValuePtr(data), RSTRING_LEN(data));
  }

  rb_obj_freeze(self);

  return self;
}

static VALUE pulsar_rb_message_topic(VALUE self) {
  // void -> String
  return rb_str_new_cstr(pulsar_message_get_topic_name(pulsar_rb_message_ptr(self)));
}

static VALUE pulsar_rb_message_id(VALUE self) {
  // pulsar_message_get_message_id
  return Qnil;
}

static VALUE pulsar_rb_message_publish_timestamp(VALUE self) {
  // void -> Fixnum
  return LONG2NUM(pulsar_message_get_publish_timestamp(pulsar_rb_message_ptr(self)));
}

static VALUE pulsar_rb_message_event_timestamp(VALUE self) {
  // void -> Fixnum
  return LONG2NUM(pulsar_message_get_event_timestamp(pulsar_rb_message_ptr(self)));
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

VALUE rb_cPulsarMessage;
void InitMessage(VALUE module) {
  rb_cPulsarMessage = rb_define_class_under(module, "Message", rb_cObject);
  rb_global_variable(&rb_cPulsarMessage);

  rb_define_alloc_func(rb_cPulsarMessage, pulsar_rb_message_alloc);

  rb_define_method(rb_cPulsarMessage, "initialize", pulsar_rb_message_initialize, -1);
  rb_define_method(rb_cPulsarMessage, "topic", pulsar_rb_message_topic, -1);
  rb_define_method(rb_cPulsarMessage, "id", pulsar_rb_message_id, -1);
  rb_define_method(rb_cPulsarMessage, "publish_timestamp", pulsar_rb_message_publish_timestamp, -1);
  rb_define_method(rb_cPulsarMessage, "event_timestamp", pulsar_rb_message_event_timestamp, -1);
  rb_define_method(rb_cPulsarMessage, "partition_key", pulsar_rb_message_partition_key, -1);
  rb_define_method(rb_cPulsarMessage, "ordering_key", pulsar_rb_message_ordering_key, -1);
  rb_define_method(rb_cPulsarMessage, "value", pulsar_rb_message_data, -1);
}
