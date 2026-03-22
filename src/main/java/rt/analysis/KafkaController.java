package rt.analysis;

import org.springframework.beans.factory.annotation.Autowired;
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

    @GetMapping("/welcome")
    public String welcome(){
        return "welcome to real time analysis";
    }

    @GetMapping("/realTimeGenerate/{count}")
    public String threadMonitor(@PathVariable int count){
        List<StockData> stockData =kafkaDataService.createMessage(count);
        if(stockData.isEmpty()){
            return "Error while generating message";
        }
        kafkaMessageProducer.produceMessage(stockData);

        return "Message generated";
    }
}
