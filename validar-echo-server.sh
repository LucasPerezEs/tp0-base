#!/bin/bash

msg="hola server"
response=$(docker run --rm --network tp0_testing_net busybox sh -c "echo '$msg' | nc server 12345")
if [ "$response" = "$msg" ]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi