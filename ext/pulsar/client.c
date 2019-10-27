#include "pulsar_ext.h"

typedef struct pulsar_rb_client_wrapper_t {
  pulsar_client_t *client;
} pulsar_rb_client_wrapper_t;

static void pulsar_rb_client_free(void *data) {
  pulsar_client_free(((pulsar_rb_client_wrapper_t*)data)->client);
  free(data);
}

static const rb_data_type_t pulsar_rb_client_t = {
  .wrap_struct_name = "Pulsar::Client",
  .function = {
    .dfree = pulsar_rb_client_free,
  },
  .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

pulsar_client_t* pulsar_rb_client_ptr(VALUE self) {
  pulsar_rb_client_wrapper_t *rb_client;
  TypedData_Get_Struct(self, pulsar_rb_client_wrapper_t, &pulsar_rb_client_t, rb_client);
  return rb_client->client;
}

static VALUE pulsar_rb_client_alloc(VALUE klass) {
  pulsar_rb_client_wrapper_t *rb_client = malloc(sizeof(*rb_client));
  rb_client->client = NULL;
  return TypedData_Wrap_Struct(klass, &pulsar_rb_client_t, rb_client);
}

static VALUE pulsar_rb_client_initialize(int argc, VALUE* argv, VALUE self) {
  pulsar_rb_client_wrapper_t *rb_client;
  TypedData_Get_Struct(self, pulsar_rb_client_wrapper_t, &pulsar_rb_client_t, rb_client);

  VALUE rb_config_v;
  rb_scan_args_kw(RB_SCAN_ARGS_KEYWORDS, argc, argv, ":", &rb_config_v);

  if (NIL_P(rb_config_v)) {
    rb_config_v = rb_hash_new();
  }

  VALUE service_url_v = rb_hash_fetch(rb_config_v, ID2SYM(rb_intern("service_url")));
  Check_Type(service_url_v, T_STRING);


  VALUE logger_proc = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("logger_proc")));
  if (rb_obj_is_proc(logger_proc)) {
    // pulsar_client_configuration_set_logger(pulsar_client_configuration_t *conf, pulsar_logger logger, void *ctx)
    // TODO: set it.
  }
  pulsar_client_configuration_t *config = pulsar_client_configuration_create();

  rb_client->client = pulsar_client_create(StringValueCStr(service_url_v), config);

  pulsar_client_configuration_free(config);
  return self;
}

VALUE rb_cPulsarClient;
void InitClient(VALUE module) {
  rb_cPulsarClient = rb_define_class_under(module, "Client", rb_cData);
  rb_global_variable(&rb_cPulsarClient);

  rb_define_alloc_func(rb_cPulsarClient, pulsar_rb_client_alloc);

  rb_define_method(rb_cPulsarClient, "initialize", pulsar_rb_client_initialize, -1);
}
