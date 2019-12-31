import kafka.server.KafkaConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Collections;

@Configuration
public class TestBeanConfiguration {

    @Bean
    public EmbeddedKafkaBroker embeddedKafkaBroker_TwoPartitions() {
        return new EmbeddedKafkaBroker(1)
                .brokerProperties(Collections.singletonMap(KafkaConfig.LogDirProp(), "./tmp"))
                ;
    }

}
