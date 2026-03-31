package rt.analysis;

import rt.analysis.pojo.StockData;

import java.util.List;

public interface KafkaDataService {
    public List<StockData> createMessage(int messageCount);
    public List<StockData> createMessage(int startCount ,int endCount, int actualCount);
}
