package rt.analysis.pojo;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class StockData {
    private String name;
    private LocalDateTime tradeDate;
    private LocalDateTime settlementDate;
    private String currency;
    private double price;
    private int volumn;
    private Country country;
}
