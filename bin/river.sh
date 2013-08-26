#!/bin/bash

# ZOOKEEPER={zookeeper1:2181,zookeeper2:2181} ES={elasticsearch address} FROM={messages from} TOPIC={kafka topic} BULKSIZE=10000 STATSD={statsd address:8125} bin/river.sh add
# ES={elasticsearch address} RIVER=cs-us-east-1-svoice-swc-dev-error bin/river.sh del
# ES={elasticsearch address} RIVER=cs-us-east-1-svoice-swc-dev-error bin/river.sh stat

# FROM
# RIVER
# ES
# ZOOKEEPER
# TOPIC
# GROUPID
# BULKSIZE
# STATSD

dir=`dirname "$0"`
dir=`cd "$dir"; pwd`

JQ=$(which jq)

if [ -z "$1" ]; then
  echo "usage: ./river.sh (add | del | stat)"
  echo "environment variables:"
  echo -e "  RIVER\n  ES\n  ZOOKEEPER\n  TOPIC\n  GROUPID\n  BULKSIZE\n  STATSD\n"
  exit 1
fi

if [ -z "$ES" ]; then
  echo "ES=localhost:9200"
  exit 1
fi

if [ -z "$FROM" ]; then
  echo "FROM=FROM WHERE"
  exit 1
fi

if [ -z "$TOPIC" ]; then
  echo "TOPIC=topic1"
  exit 1
fi

if [ -z "$RIVER" ]; then
  RIVER="${FROM}-${TOPIC}"
  echo "RIVER=${FROM}-${TOPIC}"
fi

if [ "$1" = "del" ]; then
  command="curl -XDELETE '$ES/_river/$RIVER'"
  if [ -n "$JQ" ]; then
    command+="| $JQ ."
  fi
  bash -c "$command"

  exit 0
fi

if [ "$1" = "stat" ]; then
  command="curl -XGET '$ES/_river/$RIVER/_status'"
  if [ -n "$JQ" ]; then
    command+=" | $JQ ."
  fi
  bash -c "$command"

  exit 0
fi

if [ -z "$ZOOKEEPER" ]; then
  echo "ZOOKEEPER=localhost:2181,localhost:2182"
  exit 1
fi

TOPIC="${FROM}.${TOPIC}"

if [ -z "$GROUPID" ]; then
  GROUPID="${TOPIC}-flow"
  echo "GROUPID=${TOPIC}-flow"
fi

if [ -z "$BULKSIZE" ]; then
  echo "use default BULKSIZE=10000"
  BULKSIZE=10000
fi

if [ -z "$STATSD" ]; then
  echo "TOPIC=localhost:8125"
  exit 1
fi

sed -e "s/ZOOKEEPER/$ZOOKEEPER/g" -e "s/TOPIC/$TOPIC/g" -e "s/GROUPID/$GROUPID/g" -e "s/BULKSIZE/$BULKSIZE/g" -e "s/STATSD/$STATSD/g" $dir/../_request.json > $dir/../request.json

if [ "$1" = "add" ]; then
  command="curl -XPUT '$ES/_river/$RIVER/_meta' -T $dir/../request.json"
  if [ -n "$JQ" ]; then
    command+=" | $JQ ."
  fi
  bash -c "$command"
fi

exit 0

