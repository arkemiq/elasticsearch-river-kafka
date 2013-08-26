Kafka River Plugin for ElasticSearch
==================================

The Kafka River plugin allows index bulk format messages into elasticsearch.

1. Build this plugin:
	mvn install:install-file -Dfile=contrib/kafka-0.7.2.jar -DgroupId=org.apache.kafka -DartifactId=kafka -Dversion=0.7.2 -Dpackaging=jar
        mvn compile package 
        # this will create a file here: target/releases/elasticsearch-river-kafka-0.1.0.zip
        PLUGIN_PATH=`pwd`/target/releases/elasticsearch-river-kafka-0.1.0.zip

2. Install the PLUGIN

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -install kafka-river -url file://$PLUGIN_PATH 

3. Updating the plugin

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -remove kafka-river
        ./bin/plugin --install kafka-river --url file://$PLUGIN_PATH 

4. If it is not worked, restart elasticsearch server.

Creating the kafka river is as simple as (all configuration parameters are provided, with default values):

```sh
curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
  "type" : "kafka",
  "kafka" : {
    "zkaddress" : "localhost:2181",
    "topic" : "info",
    "groupid" : "info-river",
    "zk_session_timeout" : 5000,
    "zk_sync_time": 200
  },
  "index" : {
    "name" : "info",
    "bulk_size" : 1000,
    "bulk_timeout" : "1000ms",
    "ordered" : true,
    "ttl": "1209600000ms"
  },
  "custom" : {
    "ttl_field" : "time",
    "daily_index" : true,
    "daily_index_field" : "time",
    "type_field" : "type",
    "uid_field" : "uid",
    "statsd" : "localhost:8125"
  }
}'
```

The river is automatically bulking queue messages if the queue is overloaded, allowing for faster catchup with the
messages streamed into the queue. The `ordered` flag allows to make sure that the messages will be indexed in the
same order as they arrive in the query by blocking on the bulk request before picking up the next data to be indexed.
It can also be used as a simple way to throttle indexing.

Custom field have serveral features like following:
daily_index: indices are generated as epoch based number.
daily_index_field: if document have a milliseconds time value, indices are generated by this value.
type_field: indices type is set by this.
uid_field: if document have a manual uid, elasticsearch use this value as a document _id.
statsd: river send metrics like message per second and indexing latency.
