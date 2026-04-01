package rt.analysis.generate.data;

import rt.analysis.pojo.StockData;

import java.util.List;

public interface GenerateDataKafkaService {
    public List<StockData> generateMessage(int messageCount);
    public List<StockData> generateMessage(int startCount , int endCount, int actualCount);
}
