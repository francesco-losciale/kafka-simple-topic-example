import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@Configuration
public class TestBeanConfiguration {

    @Bean
    public EmbeddedKafkaBroker embeddedKafkaBroker_TwoPartitions() {
        return new EmbeddedKafkaBroker(1);
    }

}
