import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestBeanConfiguration.class)
@EmbeddedKafka
public class SimpleConsumerProducerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private String TOPIC_BASE_NAME = "topic-";
    private Consumer<String, String> consumer;
    private Producer<String, String> producer;

    @Before
    public void setUp() throws Exception {
        setUpConsumer();
        setUpProducer();
    }

    @Test
    public void should_Be_Able_To_Consume_Message_When_Message_Is_Produced() {
        String topicName = generateNewTopicName();
        producer.send(new ProducerRecord<>(topicName, "my-aggregate-id", "my-test-value"));
        producer.flush();

        consumer.subscribe(singleton(topicName));
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, topicName);
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo("my-aggregate-id");
        assertThat(singleRecord.value()).isEqualTo("my-test-value");
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

    private String generateNewTopicName() {
        return TOPIC_BASE_NAME + new Random().nextInt();
    }

    private void setUpConsumer() {
        Map<String, Object> configs = setUpConsumerConfigMap();
        DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(
                configs, new StringDeserializer(), new StringDeserializer()
        );
        this.consumer = kafkaConsumerFactory.createConsumer();
    }

    private void setUpProducer() {
        Map<String, Object> configs = createProducerConfig();
        DefaultKafkaProducerFactory<String, String> kafkaProducerFactory = createKafkaProducerFactory(configs);
        this.producer = kafkaProducerFactory.createProducer();
    }

    private Map<String, Object> setUpConsumerConfigMap() {
        Map<String, Object> configs = new HashMap<>(
                KafkaTestUtils.consumerProps("consumerGroupName", "false", embeddedKafkaBroker)
        );
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return configs;
    }

    private Map<String, Object> createProducerConfig() {
        return new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    }

    private DefaultKafkaProducerFactory<String, String> createKafkaProducerFactory(Map<String, Object> configs) {
        return new DefaultKafkaProducerFactory<>(
                configs, new StringSerializer(), new StringSerializer()
        );
    }

    private Message<String> buildMessage(String topicName, String key, String message) throws JsonProcessingException {
        return MessageBuilder.withPayload(message)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .build();
    }
}

