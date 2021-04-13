/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.status.api.v1.JacksonMessageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SparkEventKafkaReporter {
  private SparkConf conf;
  private final static Logger LOGGER = LoggerFactory.getLogger(SparkEventKafkaReporter.class);
  private KafkaProducer<String, byte[]> kafkaProducer;
  private Properties kafkaProps;
  protected final ObjectMapper mapper = new ObjectMapper();

  public enum EventTopic {
    LogicalPlan("logicalplan");
    private String topic;

    EventTopic(String topic) {
      this.topic = topic;
    }

    public String getTopic() {
      return topic;
    }
  }

  public SparkEventKafkaReporter() {
  }

  public SparkEventKafkaReporter(SparkConf conf) {
    mapper.registerModule(new DefaultScalaModule());
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat());
    this.conf = conf;
    initKafka(conf);
  }

  public void start(SparkConf conf) {
    initKafka(conf);
  }

  private void initKafka(SparkConf conf) {
    this.conf = conf;
    kafkaProps = new Properties();
    kafkaProps.put(ProducerConfig.ACKS_CONFIG, conf.get("spark.acks", "0"));
    kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, conf.get("spark.batch.size", "20000"));
    kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, conf.get("spark.buffer.memory", "33554432"));
    kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, conf.get("spark.linger.ms", "500"));
    kafkaProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, conf.get("spark.max.request.size", "1048576"));
    kafkaProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        conf.get("spark.request.timeout.ms", "50000"));
    kafkaProps.put(ProducerConfig.RETRIES_CONFIG, conf.get("spark.retries", "3"));
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get("spark.bootstrap.servers",
        "10.69.179.17:9092,10.69.179.18:9092,10.69.179.19:9092,"
            + "10.69.179.20:9092,10.69.181.30:9092,10.69.181.31:9092,10.69.181.32:9092,10.69.181.33:9092,10.70.38.11:9092"));
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, conf.get("spark.key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"));
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        conf.get("spark.value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"));
    kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    kafkaProducer = new KafkaProducer<>(kafkaProps);
  }

  public void reportLogicalPlan(LogicalPlan analyzed, EventTopic topic) {
    String logicalPlanString = null;
    try {
      logicalPlanString = analyzed.toJSON();
    } catch (Exception e) {
      try {
        logicalPlanString = analyzed.toJSON();
      } catch (Exception re) {
        LOGGER.error("Analyzed logical plan to json failed two times", re);
      }
    }
    if (logicalPlanString != null) {
      report(logicalPlanString, topic);
    }
  }

  public void report(Object event, EventTopic topic) {
    ProducerRecord<String, byte[]> record;
    try {
      String data = null;
      if (event instanceof String) {
        data = (String) event;
      } else {
        data = mapper.writeValueAsString(event);
      }
      String kafkaTopic = conf.get("spark." + topic.getTopic() + ".topic");
      record = new ProducerRecord<>(kafkaTopic, null, null, data.getBytes());
      kafkaProducer.send(record, (metadata, exception) -> {
        if (exception != null) {
          LOGGER.error("send event to kafka error,errMsg:{}", exception);
        }
      });
      LOGGER.warn("send logical plan succeed!");
    } catch (Exception e) {
      LOGGER.warn("send logical plan error:{}", e.toString());
      return;
    }


  }

  public void close() {
    kafkaProducer.close(10, TimeUnit.SECONDS);
  }
}