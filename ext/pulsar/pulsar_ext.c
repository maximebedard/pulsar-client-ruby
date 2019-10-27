#include "pulsar_ext.h"

VALUE rb_ePulsarError;
void Init_pulsar(void) {
  VALUE mPulsar = rb_define_module("Pulsar");

  rb_ePulsarError = rb_define_class_under(mPulsar, "Error", rb_eRuntimeError);

  InitClient(mPulsar);
  InitProducer(mPulsar);
  InitConsumer(mPulsar);
}
