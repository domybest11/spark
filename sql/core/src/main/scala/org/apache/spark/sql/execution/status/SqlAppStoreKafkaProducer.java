package org.apache.spark.sql.execution.status;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.status.api.v1.JacksonMessageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class SqlAppStoreKafkaProducer {
    private SparkConf conf;
    private final static Logger LOGGER = LoggerFactory.getLogger(SqlAppStoreKafkaProducer.class);
    private KafkaProducer<String, byte[]> kafkaProducer;
    private Properties kafkaProps;
    private String METRIC_TOPIC = "lancer_bigdata_spark_spark";
    protected final ObjectMapper mapper = new ObjectMapper();

    public SqlAppStoreKafkaProducer() {

    }

    public SqlAppStoreKafkaProducer(SparkConf conf) {
        mapper.registerModule(new DefaultScalaModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat());
        this.conf = conf;
        METRIC_TOPIC =  conf.get("spark.topic", "lancer_bigdata_spark_spark");
        initKafka(conf);
    }

    public void start(SparkConf conf) {
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
                        "10.69.179.17:9092,10.69.179.18:9092,10.69.179.19:9092,"
                                + "10.69.179.20:9092,10.69.181.30:9092,10.69.181.31:9092,10.69.181.32:9092,10.69.181.33:9092,10.70.38.11:9092"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, conf.get("spark.key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer"));
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                conf.get("spark.value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"));
        kafkaProducer = new KafkaProducer<>(kafkaProps);
    }

    public void report(SqlAppStoreReportWrap thriftServerReportWrap) {
        ProducerRecord<String, byte[]> record;
        try {
            record = convert2ProducerRecord(thriftServerReportWrap);
        } catch (Exception e) {
            LOGGER.warn("convert2ProducerRecord error:{}", e.toString());
            return;
        }

        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("{} send event to kafka error,errMsg:{}", thriftServerReportWrap.getAppId(),
                        exception);
            }
        });
    }

    public void close() {
        kafkaProducer.close(10, TimeUnit.SECONDS);
    }

    private ProducerRecord<String, byte[]> convert2ProducerRecord(SqlAppStoreReportWrap thriftServerReportWrap)
            throws JsonProcessingException {
        String data = mapper.writeValueAsString(thriftServerReportWrap);
        return new ProducerRecord<>(METRIC_TOPIC, null, null, data.getBytes());
    }
}
