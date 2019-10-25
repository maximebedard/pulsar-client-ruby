require "mkmf"

abort("missing pulsar lib") unless have_library("pulsar")

create_makefile "pulsar/pulsar"
