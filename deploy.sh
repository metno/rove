#!/bin/bash

cargo build --release
if [[ $? -ne 0 ]] ; then
    exit 1
fi
  
pushd ansible

cp ../target/release/example_binary roles/deploy/files/rove_bin

ansible-playbook -i hosts deploy.yml

popd
