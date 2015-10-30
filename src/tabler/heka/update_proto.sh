#!/bin/sh
#
# Fetches heka/message/message.proto, removes gogoprotobuf dependencies,
# and compiles the result to go.

set -ex

DIR=$(cd $(dirname $0); pwd)
VENDOR_DIR=$(cd $DIR/../../../vendor/src; pwd)
cd $DIR

HEKA_REF=v0.9.2
# MESSAGES_URL=https://raw.githubusercontent.com/mozilla-services/heka/$HEKA_REF/message/message.pb.go
# curl -L ${MESSAGES_URL} | \
#     sed s+code.google.com/p/gogoprotobuf/+github.com/gogo/protobuf/+g > \
#     $DIR/message.pb.go

MESSAGES_URL=https://raw.githubusercontent.com/mozilla-services/heka/$HEKA_REF/message/message.proto
curl -O -L $MESSAGES_URL
protoc --gogofast_out=. -I . \
    -I $VENDOR_DIR/github.com/gogo/protobuf/gogoproto/ \
    -I $VENDOR_DIR/github.com/gogo/protobuf/protobuf/ \
    message.proto
