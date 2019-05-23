#!/bin/bash

set -e
set -x

pytest eurito_daps
# # Run every test
# for TOPDIRNAME in production packages;
# do
#     TESTDIRS=$(find nesta/$TOPDIRNAME -name "test*" -type d)
#     for TESTDIRNAME in $TESTDIRS;
#     do
# 	#python -m unittest discover $TESTDIRNAME
# 	pytest $TESTDIRNAME
#     done
# done
