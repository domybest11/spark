package org.apache.spark.metrics.sink;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.event.SimpleWrapEvent;
import org.apache.spark.metrics.event.WrapEvent;
import org.apache.spark.status.api.v1.JacksonMessageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wth
 * @Date:2018/11/15
 * @Time:10:45 AM
 */
public class KafkaSink {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    private KafkaProducer<String, byte[]> kafkaProducer;
    private Properties kafkaProps;
    private long kafkaMaxRecordSize;
    public final static String METRIC_TOPIC = "lancer_bigdata_spark_spark";
    protected ObjectMapper mapper;

    public KafkaSink() {

    }

    public KafkaSink(SparkConf conf) {
        this.mapper = new ObjectMapper();
        mapper.registerModule(new DefaultScalaModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat());
        initKafka(conf);
    }

    private void initKafka(SparkConf conf) {
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
                "172.22.33.94:9092,172.22.33.99:9092,172.22.33.97:9092"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, conf.get("spark.key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer"));
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                conf.get("spark.value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"));
        kafkaProducer = new KafkaProducer<>(kafkaProps);
        kafkaMaxRecordSize = conf.getLong("spark.max.record.size", 10000L);
    }

    public void report(String topic, WrapEvent event) {
        ProducerRecord<String, byte[]> record;
        try {
            record = convert2ProducerRecord(topic, event);
            if (record == null) return;
        } catch (Exception e) {
            LOGGER.warn("convert2ProducerRecord error:{}", e.toString());
            return;
        }

        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("{} send event to kafka error,errMsg:{}", event.getEventName(),
                        exception);
            }
        });
    }

    public void close() {
        kafkaProducer.close(10, TimeUnit.SECONDS);
    }

    private ProducerRecord<String, byte[]> convert2ProducerRecord(String topic, WrapEvent event)
            throws JsonProcessingException {
        String data = mapper.writeValueAsString(event);
        if (data.length() > kafkaMaxRecordSize) {
            LOGGER.info("kafka sends data exceeding the specified threshold is ignored.");
            return null;
        }
        return new ProducerRecord<>(topic, null, null, data.getBytes());
    }
}
