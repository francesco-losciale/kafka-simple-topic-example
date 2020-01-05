import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestBeanConfiguration.class)
@EmbeddedKafka(controlledShutdown = true)
public class SimpleConsumerProducerIntegrationTest extends  AbstractConsumerProducerTest {

    private static String TOPIC_BASE_NAME = "topic-";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker_TwoPartitions;

    public SimpleConsumerProducerIntegrationTest() {
        super(TOPIC_BASE_NAME);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void should_Be_Able_To_Consume_Message_When_Message_Is_Produced() {
        String topicName = generateNewTopicName();
        producer.send(new ProducerRecord<>(topicName, "my-aggregate-id", "my-test-value"));
        producer.flush();
        producer.close();

        consumer.subscribe(singleton(topicName));
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, topicName);
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo("my-aggregate-id");
        assertThat(singleRecord.value()).isEqualTo("my-test-value");

        consumer.commitSync(Duration.ofSeconds(5));

        assertThat(singleRecord.offset()).isEqualTo(0);
        assertThat(hasOnlyOnePartitionBeenWrittenOnTopic(topicName)).isTrue();

        consumer.close();
    }

    @Test
    public void should_Be_Able_To_Consume_Message_When_Message_Is_Sent_With_KafkaTemplate() throws ExecutionException, InterruptedException, JsonProcessingException {
        String topicName = generateNewTopicName();
        KafkaTemplate<String, String> kafkaTemplate =
                new KafkaTemplate<>(createKafkaProducerFactory(createProducerConfig()));
        Future<SendResult<String, String>> result =
                kafkaTemplate.send(buildMessage(topicName, "my-aggregate-id", "my-test-value"));

        assertThat(result).isNotNull();
        assertThat(result.get().getProducerRecord().key()).isEqualTo("my-aggregate-id");
        assertThat(result.get().getProducerRecord().value()).isEqualTo("my-test-value");
    }

    Map<String, Object> createProducerConfig() {
        return new HashMap<>(KafkaTestUtils.producerProps(this.embeddedKafkaBroker_TwoPartitions));
    }

    Map<String, Object> createConsumerConfig() {
        Map<String, Object> configs = new HashMap<>(
                KafkaTestUtils.consumerProps("consumerGroupName"+getClass(), "false", embeddedKafkaBroker_TwoPartitions)
        );
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return configs;
    }

}

