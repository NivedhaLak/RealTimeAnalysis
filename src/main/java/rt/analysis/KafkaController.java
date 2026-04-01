package rt.analysis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import rt.analysis.consumer.KafkaMessageConsumer;
import rt.analysis.generate.data.GenerateDataKafkaService;
import rt.analysis.producer.KafkaMessageProducer;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController
public class KafkaController {

    @Autowired
    GenerateDataKafkaService kafkaDataService;
    @Autowired
    KafkaMessageProducer kafkaMessageProducer;
    @Autowired
    KafkaMessageConsumer kafkaMessageConsumer;

    @GetMapping("/realTimeGenerate")
    public List<String> fetchData(){
        List<String> stockData =kafkaMessageConsumer.consumeData();
        return stockData;
    }

    @GetMapping("/stockValue/{count}")
    public String batchAsyncStockValue(@PathVariable int count) throws RuntimeException {
        return kafkaMessageProducer.generateAndSendStockValueInBatches(count);
    }
}
