#include "pulsar_ext.h"

typedef struct pulsarRbClient {
  pulsar_client_t *client;
} pulsarRbClient;

pulsar_client_t* pulsar_rb_client_ptr(VALUE i) {
  pulsarRbClient *rb_client;
  Data_Get_Struct(i, pulsarRbClient, rb_client);
  return rb_client->client;
}

static void pulsar_rb_client_mark(pulsarRbClient *rb_client) {
  // noop
}

static void pulsar_rb_client_free(pulsarRbClient *rb_client) {
  pulsar_client_free(rb_client->client);
  free(rb_client);
}

static VALUE pulsar_rb_client_alloc(VALUE klass) {
  pulsarRbClient *rb_client = malloc(sizeof(*rb_client));
  rb_client->client = NULL;
  return Data_Wrap_Struct(klass, pulsar_rb_client_mark, pulsar_rb_client_free, rb_client);
}

static VALUE pulsar_rb_client_initialize(int argc, VALUE* argv, VALUE self) {
  VALUE rb_config_v;
  rb_scan_args_kw(RB_SCAN_ARGS_KEYWORDS, argc, argv, ":", &rb_config_v);

  if (NIL_P(rb_config_v)) {
    rb_config_v = rb_hash_new();
  }

  VALUE service_url_v = rb_hash_fetch(rb_config_v, ID2SYM(rb_intern("service_url")));
  Check_Type(service_url_v, T_STRING);


  VALUE logger_proc = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("logger_proc")));
  if (rb_obj_is_proc(logger_proc)) {
    // TODO: set it.
  }

  pulsarRbClient *rb_client;
  Data_Get_Struct(self, pulsarRbClient, rb_client);

  pulsar_client_configuration_t *config = pulsar_client_configuration_create();

  rb_client->client = pulsar_client_create(StringValueCStr(service_url_v), config);

  pulsar_client_configuration_free(config);
  return self;
}

VALUE rb_cPulsarClient;
void InitClient(VALUE module) {
  rb_cPulsarClient = rb_define_class_under(module, "Client", rb_cObject);
  rb_global_variable(&rb_cPulsarClient);

  rb_define_alloc_func(rb_cPulsarClient, pulsar_rb_client_alloc);

  rb_define_method(rb_cPulsarClient, "initialize", pulsar_rb_client_initialize, -1);
}
