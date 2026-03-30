package rt.analysis;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rt.analysis.pojo.StockData;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class KafkaMessageConsumer {
    @Value("${kafka-topic}")
    private String topic;

    KafkaConsumer<String,String> kafkaConsumer;
    public KafkaMessageConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ingest-trade-value-group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10000");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"10242880");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"10048576");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumer=new KafkaConsumer<>(props);
    }
    private final AtomicLong atomicLong = new AtomicLong(0);
    public List<String> consumeData(){
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        List<String> data = new ArrayList<>();
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                data.add(record.key() + ":" + record.value());
            }
            kafkaConsumer.commitAsync();
            if(!data.isEmpty())
                break;
        }
        return data;
    }

    {

    }
}
