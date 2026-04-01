package rt.analysis.generate.data;

import org.springframework.stereotype.Service;
import rt.analysis.pojo.Country;
import rt.analysis.pojo.StockData;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class GenerateDataKafkaServiceImpl implements GenerateDataKafkaService {
    Logger logger = LoggerFactory.getLogger(GenerateDataKafkaServiceImpl.class);

    @Override
    public List<StockData> generateMessage(int startCount, int endCount, int actualCount) {
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
                    .country(country).volume(random.nextInt(1500, 25000))
                    .build());
        }
        logger.info("generate message success,count:{}",stockData.size());
        return stockData;
    }

    @Override
    public List<StockData> generateMessage(int messageCount) {
        return generateMessage(0, messageCount, messageCount);
    }


}
