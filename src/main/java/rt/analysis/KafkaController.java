package rt.analysis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import rt.analysis.pojo.StockData;

import java.util.List;

@RestController
public class KafkaController {

    @Autowired
    KafkaDataService kafkaDataService;
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
    public String batchAsyncStockValue(@PathVariable int count) throws InterruptedException{
        return kafkaMessageProducer.sendStockValuesInBatches(count);
    }
}
