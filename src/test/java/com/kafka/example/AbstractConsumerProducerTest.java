package com.kafka.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static java.lang.Boolean.FALSE;

abstract public class AbstractConsumerProducerTest {

    private String topicName;

    protected Consumer<String, String> consumer;
    protected Producer<String, String> producer;

    public void setUp() throws Exception {
        setUpConsumer();
        setUpProducer();
        this.topicName  = "topic_" + new Random().nextInt();
    }

    protected Map<String, Object> createProducerConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getEmbeddedKafkaBrokerListAsString());
        return props;
    }

    protected Map<String, Object> createConsumerConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupName" + new Random().nextInt());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getEmbeddedKafkaBrokerListAsString());
        return props;
    }

    protected String topicName() {
        return topicName;
    }

    abstract String getEmbeddedKafkaBrokerListAsString();

    protected DefaultKafkaConsumerFactory<String, String> createKafkaConsumerFactory(Map<String, Object> configs) {
        return new DefaultKafkaConsumerFactory<>(
                configs, new StringDeserializer(), new StringDeserializer()
        );
    }

    protected DefaultKafkaProducerFactory<String, String> createKafkaProducerFactory(Map<String, Object> configs) {
        return new DefaultKafkaProducerFactory<>(
                configs, new StringSerializer(), new StringSerializer()
        );
    }

    protected boolean hasOnlyOnePartitionBeenWrittenOnTopic(String topicName) {
        return getConsumerOffset(topicName, 0) +
                getConsumerOffset(topicName, 1)
                == 1;
    }

    protected long getConsumerOffset(String topicName, int partitionNumber) {
        List<PartitionInfo> partitionList = consumer.partitionsFor(topicName);
        final PartitionInfo partitionInfo = partitionList.get(partitionNumber);
        return consumer.position(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }

    private void setUpConsumer() {
        Map<String, Object> consumerConfiguration = createConsumerConfiguration();
        DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory = createKafkaConsumerFactory(consumerConfiguration);
        this.consumer = kafkaConsumerFactory.createConsumer();
    }

    private void setUpProducer() {
        Map<String, Object> producerConfiguration = createProducerConfiguration();
        DefaultKafkaProducerFactory<String, String> kafkaProducerFactory = createKafkaProducerFactory(producerConfiguration);
        this.producer = kafkaProducerFactory.createProducer();
    }

    static Message<String> buildMessage(String topicName, String key, String message) throws JsonProcessingException {
        return MessageBuilder.withPayload(message)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .build();
    }
}
