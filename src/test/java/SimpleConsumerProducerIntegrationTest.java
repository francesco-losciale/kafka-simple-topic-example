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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=TestBeanConfiguration.class)
@EmbeddedKafka
public class SimpleConsumerProducerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private String TOPIC = "topic";
    private Consumer<String, String> consumer;
    private Producer<String, String> producer;

    @Before
    public void setUp() throws Exception {
        setUpConsumer();
        setUpProducer();
    }

    @Test
    public void should_Be_Able_To_Consume_Message_When_Message_Is_Produced() {
        producer.send(new ProducerRecord<>(TOPIC, "my-aggregate-id", "my-test-value"));
        producer.flush();

        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TOPIC);
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo("my-aggregate-id");
        assertThat(singleRecord.value()).isEqualTo("my-test-value");
    }

    private void setUpConsumer() {
        Map<String, Object> configs = setUpConsumerConfigMap();
        DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(
                configs, new StringDeserializer(), new StringDeserializer()
        );
        this.consumer = kafkaConsumerFactory.createConsumer();
        this.consumer.subscribe(singleton(TOPIC));
    }

    private Map<String, Object> setUpConsumerConfigMap() {
        Map<String, Object> configs = new HashMap<>(
                KafkaTestUtils.consumerProps("consumerGroupName", "false", embeddedKafkaBroker)
        );
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return configs;
    }

    private void setUpProducer() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        DefaultKafkaProducerFactory<String, String> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(
                configs, new StringSerializer(), new StringSerializer()
        );
        this.producer = kafkaProducerFactory.createProducer();
    }
}

