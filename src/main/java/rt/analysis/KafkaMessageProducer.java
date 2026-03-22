package rt.analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import rt.analysis.pojo.StockData;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
@Component
public class KafkaMessageProducer {
    @Autowired
    private KafkaTemplate<String, String> producer;

    @Value("${kafka-topic}")
    private String topic;

    ObjectMapper objectMapper;

    public KafkaMessageProducer() {
        this.objectMapper = new ObjectMapper();
    }
    public void produceMessage(List<StockData> messages) {
        try {
            for(StockData message:messages)
            {
                String jsonMessage = objectMapper.writeValueAsString(message);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.getName()+ LocalDateTime.now().toString(), jsonMessage);
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
