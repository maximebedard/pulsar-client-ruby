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

static void pulsar_rb_client_logger(pulsar_logger_level_t level, const char *file, int line, const char *message, void *ctx) {
  VALUE proc = (VALUE)ctx;
  rb_funcall(proc, rb_intern("call"), 2, INT2NUM(1337), INT2NUM(1338));
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
  VALUE io_threads = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("io_threads")));
  VALUE listener_threads = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("listener_threads")));
  VALUE operation_timeout_s = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("operation_timeout_s")));
  VALUE concurrent_lookup_requests = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("concurrent_lookup_requests")));
  VALUE stats_interval_s = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("stats_interval_s")));
  VALUE tls_validate_hostname = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("tls_validate_hostname")));
  VALUE tls_allow_insecure_connection = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("tls_allow_insecure_connection")));
  VALUE tls_trust_certs_file_path = rb_hash_aref(rb_config_v, ID2SYM(rb_intern("tls_trust_certs_file_path")));

  pulsar_client_configuration_t *config = pulsar_client_configuration_create();

  if (rb_obj_is_proc(logger_proc)) {
    pulsar_client_configuration_set_logger(config, pulsar_rb_client_logger, (void*)logger_proc);
  }

  if (FIXNUM_P(io_threads) && NUM2INT(io_threads) > 0) {
    pulsar_client_configuration_set_io_threads(config, NUM2INT(io_threads));
  }

  if (FIXNUM_P(listener_threads) && NUM2INT(listener_threads) > 0) {
    pulsar_client_configuration_set_message_listener_threads(config, NUM2INT(listener_threads));
  }

  if (FIXNUM_P(operation_timeout_s) && NUM2INT(operation_timeout_s) > 0) {
    pulsar_client_configuration_set_operation_timeout_seconds(config, NUM2INT(operation_timeout_s));
  }

  if (FIXNUM_P(concurrent_lookup_requests) && NUM2INT(concurrent_lookup_requests) > 0) {
    pulsar_client_configuration_set_concurrent_lookup_request(config, NUM2INT(concurrent_lookup_requests));
  }

  if (FIXNUM_P(stats_interval_s) && NUM2UINT(stats_interval_s) > 0) {
    pulsar_client_configuration_set_stats_interval_in_seconds(config, NUM2UINT(stats_interval_s));
  }

  const char* prefix = "pulsar+ssl://";
  if (strncmp(prefix, StringValueCStr(service_url_v), strlen(prefix)) == 0) {
    pulsar_client_configuration_set_use_tls(config, 1);
  }

  if (RTEST(tls_validate_hostname)) {
    pulsar_client_configuration_set_validate_hostname(config, 1);
  }

  if (RTEST(tls_allow_insecure_connection)) {
    pulsar_client_configuration_set_tls_allow_insecure_connection(config, 1);
  }

  if (RB_TYPE_P(tls_trust_certs_file_path, T_STRING) && RSTRING_LEN(tls_trust_certs_file_path) > 0) {
    pulsar_client_configuration_set_tls_trust_certs_file_path(config, StringValueCStr(tls_trust_certs_file_path));
  }

  // TODO: authentication

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
