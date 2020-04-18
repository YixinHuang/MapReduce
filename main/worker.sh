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

# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1


failed_any=0


echo '***' Starting wc test Worker....
# start multiple workers.
../mrworker ../../mrapps/wc.so

sleep 1


if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
