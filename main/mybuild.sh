#!/bin/sh

#
# basic map-reduce test
#

RACE=

# uncomment this to run the tests with the Go race detector.
#RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*
echo '***' recreate folder mr-tmp 
# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
echo '***' plugin  completed
(cd .. && go build $RACE mrmaster.go) || exit 1
echo '***' mrmaster completed
(cd .. && go build $RACE mrworker.go) || exit 1
echo '***' mrworker completed

echo '***' All Build Completed Successfully!



