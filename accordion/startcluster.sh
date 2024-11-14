#!/bin/bash
stty erase ^H
cd ./sbin
./startSlaves.sh
cd ..
./run.sh
