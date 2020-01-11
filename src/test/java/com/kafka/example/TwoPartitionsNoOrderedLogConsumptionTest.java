package com.kafka.example;

import com.kafka.config.TestBeanConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestBeanConfiguration.class)
@EmbeddedKafka(controlledShutdown = true)
public class TwoPartitionsNoOrderedLogConsumptionTest extends AbstractConsumerProducerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker_TwoPartitions;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void should_Not_Read_Messages_In_Ordered_Fashion_When_TopicPartitions_Are_Two() {
        String topicName = topicName();
        List<ConsumerRecord<String, String>> consumerRecordList = new ArrayList<>();
        List<String> sortedRecordValues = produceRecordsAndReturnOrderedValues(topicName);

        consumer.subscribe(singleton(topicName));
        List<String> valuesFromRecord = consumeRecordValues(consumerRecordList);

        assertThat(valuesFromRecord.size()).isEqualTo(5);
        assertThat(valuesFromRecord).isNotEqualTo(sortedRecordValues);

        consumer.close();
    }

    @Override
    String getEmbeddedKafkaBrokerListAsString() {
        return embeddedKafkaBroker_TwoPartitions.getBrokersAsString();
    }

    private List<String> consumeRecordValues(List<ConsumerRecord<String, String>> consumerRecordList) {
        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(this.consumer);
        consumerRecords.iterator().forEachRemaining(consumerRecordList::add);
        return consumerRecordList.stream().map(
                record -> record.value()).collect(Collectors.toList()
        );
    }

    private List<String> produceRecordsAndReturnOrderedValues(String topicName) {
        List<String> expectedOrderedRecordValues = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String recordKey = "key-" + i;
            String recordValue = "my-test-value-" + i;
            producer.send(new ProducerRecord<>(topicName, recordKey, recordValue));
            expectedOrderedRecordValues.add(recordValue);
        }
        producer.flush();
        producer.close();
        Collections.sort(expectedOrderedRecordValues);
        return expectedOrderedRecordValues;
    }

}
