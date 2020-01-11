package com.kafka.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.Boolean.FALSE;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestBeanConfiguration.class)
@EmbeddedKafka(controlledShutdown = true)
public class TwoPartitionsOrderedLogConsumptionUsingSemanticKeyTest extends AbstractConsumerProducerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker_TwoPartitions;

    public TwoPartitionsOrderedLogConsumptionUsingSemanticKeyTest() {
        super("topic");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void should_Read_Messages_In_Ordered_Fashion_When_TopicPartitions_Are_Two() {
        String topicName = generateNewTopicName();
        String recordSemanticKey = "same-semantic-key-for-all-records-i-want-ordered";
        List<ConsumerRecord<String, String>> consumerRecordList = new ArrayList<>();
        List<String> sortedRecordValues = produceRecordsAndReturnOrderedValues(topicName, recordSemanticKey);

        consumer.subscribe(singleton(topicName));
        List<String> valuesFromRecord = consumeRecordValues(consumerRecordList);

        assertThat(valuesFromRecord.size()).isEqualTo(5);
        assertThat(valuesFromRecord).isEqualTo(sortedRecordValues);

        consumer.close();
    }

    private List<String> consumeRecordValues(List<ConsumerRecord<String, String>> consumerRecordList) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(5));
        consumerRecords.iterator().forEachRemaining(consumerRecordList::add);
        return consumerRecordList.stream().map(
                record -> record.value()).collect(Collectors.toList()
        );
    }

    private List<String> produceRecordsAndReturnOrderedValues(String topicName, String semanticKey) {
        List<String> expectedOrderedRecordValues = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String recordKey = semanticKey;
            String recordValue = "my-test-value-" + i;
            producer.send(new ProducerRecord<>(topicName, recordKey, recordValue));
            expectedOrderedRecordValues.add(recordValue);
        }
        producer.flush();
        producer.close();
        Collections.sort(expectedOrderedRecordValues);
        return expectedOrderedRecordValues;
    }

    @Override
    Map<String, Object> createProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker_TwoPartitions.getBrokersAsString());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Override
    Map<String, Object> createConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker_TwoPartitions.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupName");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

}
