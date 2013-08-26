/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.kafka;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.core.JsonFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.net.InetAddress;

import org.json.*;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.elasticsearch.util.StatsdClient;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 */
public class KafkaRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final String zkAddress;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    private final boolean kafkaMsgPack;
    private final int zkSessionTimeout;
    private final int zkSyncTime;
    private final int zkAutocommitInterval;
    private final long fetchSize;

    private final String indexName;
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;
    private final TimeValue ttl;

    private final String typeField;
    private final String uidField;
    private final boolean dailyIndex;
    private final String dailyIndexField;
    private final String ttlField;

    private volatile boolean closed = false;
    private volatile Thread thread;
    private volatile ConsumerConnector consumerConnector;

    private volatile StatsdClient statsd;
    private final String statsdServer;
    private final int statsdPort;
    private final String statsdPrefix;

    private volatile ConcurrentLinkedQueue<BulkRequestBuilder> delayed;

    @SuppressWarnings({"unchecked"})
    @Inject
    public KafkaRiver(RiverName riverName, RiverSettings settings, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.client = client;

        if (settings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) settings.settings().get("kafka");
            
            zkAddress = XContentMapValues.nodeStringValue(kafkaSettings.get("zkaddress"), "localhost:2181");

            kafkaTopic = XContentMapValues.nodeStringValue(kafkaSettings.get("topic"), "info");
            kafkaGroupId = XContentMapValues.nodeStringValue(kafkaSettings.get("groupid"), "default");
            zkSessionTimeout = XContentMapValues.nodeIntegerValue(kafkaSettings.get("zk_session_timeout"), 400);
            zkSyncTime = XContentMapValues.nodeIntegerValue(kafkaSettings.get("zk_sync_time"), 200);
            zkAutocommitInterval = XContentMapValues.nodeIntegerValue(kafkaSettings.get("zk_autocommit_interval"), 1000);
            fetchSize = XContentMapValues.nodeLongValue(kafkaSettings.get("fetch_size"), 307200);

            kafkaMsgPack = XContentMapValues.nodeBooleanValue(kafkaSettings.get("msgpack"), false);
        } else {
     	    logger.warn("using localhost zookeeper");            
    	    zkAddress = new String("localhost:2181");
            kafkaTopic = "helloworld";
            kafkaGroupId = "helloworld";
            zkSessionTimeout = 5000;
            zkSyncTime = 200;
            zkAutocommitInterval = 1000;
            fetchSize = 307200;

            kafkaMsgPack = false;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("name"), kafkaTopic);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "1000ms"), TimeValue.timeValueMillis(1000));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(1000);
            }
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
            if (indexSettings.containsKey("ttl")) {
                ttl = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("ttl"), "0ms"), TimeValue.timeValueMillis(0));
            } else {
                ttl = TimeValue.timeValueMillis(0);
            }
        } else {
            indexName = kafkaTopic;
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(1000);
            ordered = false;
            ttl = TimeValue.timeValueMillis(0);
        }

        if (settings.settings().containsKey("custom")) {
            Map<String, Object> customSettings = (Map<String, Object>) settings.settings().get("custom");
            String[] statsdString = XContentMapValues.nodeStringValue(customSettings.get("statsd"), null).split(":");
            if (statsdString.length > 1) {
                statsdServer = statsdString[0];
                statsdPort = Integer.parseInt(statsdString[1]);
            }
            else {
                statsdServer = null;
                statsdPort = 0;
            }

            typeField = XContentMapValues.nodeStringValue(customSettings.get("type_field"), null);
            uidField = XContentMapValues.nodeStringValue(customSettings.get("uid_field"), null);
            dailyIndex = XContentMapValues.nodeBooleanValue(customSettings.get("daily_index"), false);
            if (dailyIndex) {
                dailyIndexField = XContentMapValues.nodeStringValue(customSettings.get("daily_index_field"), null);
            }
            else {
                dailyIndexField = null;
            }
            ttlField = XContentMapValues.nodeStringValue(customSettings.get("ttl_field"), null);
        } else {
            typeField = null;
            uidField = null;
            ttlField = null;
            statsdServer = null;
            statsdPort = 0;
            dailyIndex = false;
            dailyIndexField = null;
        }

        String hostname;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            hostname = addr.getHostName().replace('.', '-');
            logger.info("Hostname: " + hostname);
        } catch (java.net.UnknownHostException e) {
            logger.info("Exception: " + e);
            hostname = new String("unknown-host");
        }

        statsdPrefix = new String("system.elastic-river." + hostname + ".river." + kafkaTopic);
    }

    @Override
    public void start() {
        if (statsdServer != null && statsdPort > 0) {
            try {
                statsd = new StatsdClient(statsdServer, statsdPort);
            } catch (Exception e) {
                logger.warn("Exception: " + e);
            }
        }
        else {
            statsd = null;
        }

        Properties props = new Properties();
        props.put("zk.connect", zkAddress); 
        props.put("groupid", kafkaGroupId);
        props.put("zk.sessiontimeout.ms", Integer.toString(zkSessionTimeout));
        props.put("zk.synctime.ms", Integer.toString(zkSyncTime));
        props.put("fetch.size", Long.toString(fetchSize));
        props.put("autocommit.interval.ms", Integer.toString(zkAutocommitInterval));
        props.put("consumer.timeout.ms", Long.toString(bulkTimeout.millis()));

        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kafka_river").newThread(new Consumer());
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        logger.info("closing kafka river");
        closed = true;
        thread.interrupt();
    }

    private class Consumer implements Runnable {

        @Override
        public void run() {
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(kafkaTopic, new Integer(1));
            Map<String, List<KafkaStream<Message>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

            KafkaStream<Message> stream =  consumerMap.get(kafkaTopic).get(0);
            ConsumerIterator<Message> it = stream.iterator();

            delayed = new ConcurrentLinkedQueue<BulkRequestBuilder>();

            while (true) {
                if (closed) {
                    // when interrupted
                    while (!delayed.isEmpty()) {
                        processRequest(delayed.poll());
                    }
                    break;
                }

                while (!delayed.isEmpty()) {
                    processRequest(delayed.poll());
                    try {
                        Thread.sleep(1000);
                    } catch(InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }

                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();;
                try {
                    while (it.hasNext()) {
                        if (bulkRequestBuilder.numberOfActions() < bulkSize) {
                            byte [] body = getMessage(it.next().message());
                            try {
                                processBody(body, bulkRequestBuilder);
                            }
                            catch (Exception e) {
                                logger.warn("failed to parse request. [{}]", e);
                                if (statsd != null) {
                                    statsd.increment(statsdPrefix + ".error.consume");
                                }
                                continue;
                            }
                        }
                        else if (bulkRequestBuilder.numberOfActions() >= bulkSize) {
                            break;
                        }
                        else {
                            logger.warn("Err..Where am I?");
                        }
                    }
                }
                catch (kafka.consumer.ConsumerTimeoutException e) {
                    logger.debug("consumer timeout [{}]", e);
                }
                catch (Exception e) {
                    logger.warn("consumer failed [{}]", e);
                    if (statsd != null)
                        statsd.increment(statsdPrefix + ".error.consume");
                }
                finally {
                    // finally all messages in queue should be sent
                    processRequest(bulkRequestBuilder);    
                }
            }
            cleanup(0, "closing river");
        }

        private byte[] getMessage(Message message)
        {
            ByteBuffer buffer = message.payload();
            byte [] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }

        private void cleanup(int code, String message) {
            try {
                consumerConnector.shutdown();
                logger.info("clean up kafka consumer [{}]", code, message);
            } catch (Exception e) {
                logger.info("failed to clean up [{}]", e, message);
            }
        }

        private void processRequest(final BulkRequestBuilder bulkRequestBuilder) {
            if (logger.isTraceEnabled()) {
              logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
            }
            if (bulkRequestBuilder.numberOfActions() > 0 && statsd != null) {
                statsd.increment(statsdPrefix + ".sent", bulkRequestBuilder.numberOfActions());
            }
            if (ordered) {
                try {
                    if (bulkRequestBuilder.numberOfActions() > 0) {
                        BulkResponse response = bulkRequestBuilder.execute().actionGet();
                        if (response.hasFailures()) {
                            logger.warn("failed to execute" + response.buildFailureMessage());
                            if (statsd != null)
                                statsd.increment(statsdPrefix + ".error.bulk");
                            delayed.offer(bulkRequestBuilder);
                        }

                        long latency = response.getTookInMillis();
                        if (statsd != null)
                            statsd.timing(statsdPrefix + ".indexing", (int)latency);
                    }
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                    if (statsd != null)
                        statsd.increment(statsdPrefix + ".error.bulk");
                    delayed.offer(bulkRequestBuilder);
                }
            }
            else {
                if (bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse response) {
                            if (response.hasFailures()) {
                                logger.warn("failed to execute" + response.buildFailureMessage());
                                if (statsd != null)
                                    statsd.increment(statsdPrefix + ".error.bulk");
                                delayed.offer(bulkRequestBuilder);
                            }
                        }
                        
                        @Override
                        public void onFailure(Throwable e) {
                            logger.warn("failed to execute bulk [{}]", e);
                            if (statsd != null)
                                statsd.increment(statsdPrefix + ".error.bulk");
                            delayed.offer(bulkRequestBuilder);
                        }
                    });
                }
            }
        }
        
        private void processBody(byte[] body, BulkRequestBuilder bulkRequestBuilder) throws Exception {
            if (body == null) return;
            String message = new String(body);
            JSONObject jsonObject = new JSONObject(message);

            String index = indexName != null ? indexName : kafkaTopic;
            if (dailyIndex) {
                Long time = dailyIndexField != null ? jsonObject.getLong(dailyIndexField) : System.currentTimeMillis();
                int day = (int)((time / 86400000) + 0);
                index = new Integer(day).toString();
            }

            if (ttl.millis() > 0) {
                long now = System.currentTimeMillis();
                long time = ttlField != null ? jsonObject.getLong(ttlField) : now;
                long newTtl = time + ttl.millis() - now;

                if (newTtl < 0) {
                    logger.debug("skipped expired doc");
                    return;
                }
                else {
                    message = jsonObject.put("_ttl", newTtl).toString();
                }
            }
            
            String type = null;
            try {
                type = typeField != null ? jsonObject.getString(typeField) : kafkaTopic;
            }
            catch (JSONException e) {
                logger.debug("Exception: " + e);
                type = kafkaTopic;
            }
            
            String uid = null;
            try {
                uid = uidField != null ? jsonObject.getString(uidField) : null;
            }
            catch (JSONException e) {
                logger.debug("Exception: " + e);
            }

            if (uid != null) {
                bulkRequestBuilder.add(client.prepareIndex(index, type, uid).setSource(message));
            }
            else {
                bulkRequestBuilder.add(client.prepareIndex(index, type).setSource(message));
            }
        }
    }
}
