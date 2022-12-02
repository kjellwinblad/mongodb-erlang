#!/usr/bin/env bash

docker run -p 27017:27017 --rm -d --name mongo_test mongo:5.0.14

echo 'Running all tests suites with the legacy protocol enabled'

ERL_FLAGS='-mongodb use_legacy_protocol true' rebar3 ct

RESULT_LEGACY=$?

echo 'Running mc_worker_api_SUITE with the modern protocol enabled'

ERL_FLAGS='-mongodb use_legacy_protocol false' rebar3 ct --suite test/mc_worker_api_SUITE.erl

RESULT_MODERN=$?

docker stop mongo_test

test $RESULT_LEGACY = 0 && test $RESULT_MODERN = 0

RESULT=$?

if [ $RESULT != 0 ]
then
    echo FAILED
else
    echo SUCCESS
fi

exit $?
