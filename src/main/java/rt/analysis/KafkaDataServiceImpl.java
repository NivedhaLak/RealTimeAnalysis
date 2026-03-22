package rt.analysis;

import org.springframework.stereotype.Service;
import rt.analysis.pojo.StockData;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class KafkaDataServiceImpl implements KafkaDataService {
    String[] currency = {"USD","EUR","JPY","CAD","AUD","INR"};
    @Override
    public List<StockData> createMessage(int messageCount) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<StockData> stockData = new ArrayList<>();
        for(int i=0;i<messageCount;i++){
            stockData.add(StockData.builder()
                    .name("Trade:"+random.nextInt(messageCount))
                    .tradeDate(LocalDateTime.now())
                    .settlementDate(LocalDateTime.now().plusDays(random.nextInt(1,5)))
                    .currency(currency[random.nextInt(0,5)]).price(random.nextDouble(200.90,3010.99))
                    .build());
        }
        return stockData;
    }
}
