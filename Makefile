# See LICENSE for licensing information.

PROJECT = mongo

# Options.
PLT_APPS = ssl crypto public_key asn1
CT_SUITES = eunit mongo

# Standard targets.
include erlang.mk
