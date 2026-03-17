package rt.analysis;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import rt.analysis.pojo.StockData;

@RestController
public class KafkaController {

    @GetMapping("/welcome")
    public String welcome(){

        return "welcome to real time analysis";
    }

    @GetMapping("/realTimeGenerate")
    public void threadMonitor(){
        String brokers = "broker1:9092";
        String topic = "sp-ingest-trade-value";

        KafkaMessageProducer producer = new KafkaMessageProducer(brokers, topic);

        StockData kafkaMessage = KafkaDataFactory.createMessage();
        producer.produceMessage("Testing 123..", kafkaMessage);

        producer.close();
    }
}
