package rt.analysis;

import rt.analysis.pojo.StockData;

import java.time.LocalDateTime;
import java.util.Currency;

public class KafkaDataFactory {
    private KafkaDataFactory(){}

    public static StockData createMessage(){

        return StockData.builder().name("ABC").tradeDate(LocalDateTime.now()).settlementDate(LocalDateTime.now())
                .currency("USD").price(12.50).build();
    }
}
