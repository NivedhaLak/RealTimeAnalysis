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
    public List<StockData> createMessage(int startCount , int endCount) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<StockData> stockData = new ArrayList<>();
        for(int i=startCount;i<=endCount;i++){
            stockData.add(StockData.builder()
                    .name("Trade:"+random.nextInt(startCount,endCount+1))
                    .tradeDate(LocalDateTime.now())
                    .settlementDate(LocalDateTime.now().plusDays(random.nextInt(1,5)))
                    .currency(currency[random.nextInt(0,5)]).price(random.nextDouble(200.90,3010.99))
                    .build());
        }
        return stockData;
    }

    @Override
    public List<StockData> createMessage(int messageCount) {
        return createMessage(0,messageCount);
    }
}
