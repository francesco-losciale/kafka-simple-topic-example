import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.Random;

abstract public class AbstractConsumerProducerTest {

    private String topicBaseName;

    protected Consumer<String, String> consumer;
    protected Producer<String, String> producer;

    public AbstractConsumerProducerTest(String topicBaseName) {
        this.topicBaseName = topicBaseName;
    }

    public void setUp() throws Exception {
        setUpConsumer();
        setUpProducer();
    }

    protected String generateNewTopicName() {
        return topicBaseName + new Random().nextInt();
    }

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

    abstract Map<String, Object> createProducerConfig();

    abstract Map<String, Object> createConsumerConfig();

    private void setUpConsumer() {
        Map<String, Object> configs = createConsumerConfig();
        DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory = createKafkaConsumerFactory(configs);
        this.consumer = kafkaConsumerFactory.createConsumer();
    }

    private void setUpProducer() {
        Map<String, Object> configs = createProducerConfig();
        DefaultKafkaProducerFactory<String, String> kafkaProducerFactory = createKafkaProducerFactory(configs);
        this.producer = kafkaProducerFactory.createProducer();
    }

    static Message<String> buildMessage(String topicName, String key, String message) throws JsonProcessingException {
        return MessageBuilder.withPayload(message)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .build();
    }
}
