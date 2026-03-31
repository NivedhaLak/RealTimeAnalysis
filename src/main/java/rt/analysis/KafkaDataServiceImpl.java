package rt.analysis;

import org.springframework.stereotype.Service;
import rt.analysis.pojo.Country;
import rt.analysis.pojo.StockData;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class KafkaDataServiceImpl implements KafkaDataService {

    @Override
    public List<StockData> createMessage(int startCount, int endCount, int actualCount) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<StockData> stockData = new ArrayList<>();

        for (int i = startCount; i <= endCount; i++) {
            Country country = Country.getByValue(random.nextInt(246));
            stockData.add(StockData.builder()
                    .name("Trade:" + System.currentTimeMillis() + "_" + random.nextInt(startCount, actualCount + 1))
                    .tradeDate(LocalDateTime.now())
                    .settlementDate(LocalDateTime.now().plusDays(random.nextInt(1, 5)))
                    .currency(country.getCurrencyCode())
                    .price(random.nextDouble(200.90, 3010.99))
                    .country(country).volumn(random.nextInt(1500, 25000))
                    .build());
        }
        return stockData;
    }

    @Override
    public List<StockData> createMessage(int messageCount) {
        return createMessage(0, messageCount, messageCount);
    }
}
