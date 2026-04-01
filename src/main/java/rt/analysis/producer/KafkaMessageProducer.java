package rt.analysis.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import rt.analysis.DateTimeHelper;
import rt.analysis.generate.data.GenerateDataKafkaService;
import rt.analysis.pojo.StockData;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class KafkaMessageProducer {
    @Autowired
    private KafkaTemplate<String, String> producer;

    private ExecutorService executorService;
    @Value("${kafka-topic}")
    private String topic;
    @Value("${kafka-batch-size}")
    int batch_size;
    private ObjectMapper objectMapper;
    @Autowired
    GenerateDataKafkaService kafkaDataService;

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    public KafkaMessageProducer(ExecutorService executorService) {
        this.objectMapper = new ObjectMapper();
        this.executorService = executorService;
    }
//
//    public long produceMessage(List<StockData> messages) throws InterruptedException {
//        try {
//            AtomicLong size = new AtomicLong();
//            executorService.submit(() -> {
//                for (StockData message : messages) {
//                    String jsonMessage = objectMapper.writeValueAsString(message);
//                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.getName() + LocalDateTime.now().toString(), jsonMessage);
//                    CompletableFuture<SendResult<String, String>> future = producer.send(record);
//                    future.thenAccept(result -> {
//                        log.info("send successfully: {}", result.getRecordMetadata().offset());
//                    }).exceptionally(ex -> {
//                        log.error("Error: {}" , ex.getMessage());
//                        throw new RuntimeException(ex);
//                    });
//                    size.addAndGet(record.value().toString().getBytes().length);
//                }
//                producer.flush();
//            });
//            executorService.wait(100);
//            return size.get();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return 0l;
//    }

    public void sendMessageAsyncBatches(List<StockData> messages, String traceId,int startBatchIndex, int endBatchIndex) throws RuntimeException {
        try {
            executorService.submit(() -> {
                List<CompletableFuture<SendResult<String,String>>> futureList = new ArrayList<>();

                for (StockData message : messages) {
                    String jsonMessage = objectMapper.writeValueAsString(message);

                    ProducerRecord<String,String> record= new ProducerRecord<>(topic, message.getName()
                            + LocalDateTime.now().toString(), jsonMessage);

                    record.headers().add("traceId", traceId.getBytes());
                    CompletableFuture<SendResult<String,String>> future =producer.send(record);
                    futureList.add(future);
                }
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                        .thenRun(()-> sendMessageCallBack(futureList, traceId,startBatchIndex,endBatchIndex))
                        .exceptionally(exception ->{
                            log.error("Exception in send message for traceId: {}, error message:{}",traceId,exception.getMessage());
                            throw new RuntimeException(exception);
                        });
            });
        } catch (Exception e) {
            throw e;
        }
    }

    public String generateAndSendStockValueInBatches(int totalStock) throws RuntimeException {
        long startTime = System.currentTimeMillis();
        log.info("Stock Value Generation started for count: {}", totalStock);
        String traceId = "stock_"+startTime;
        int total_record=0;
        for (int index = 1; index <= (totalStock / batch_size) + 1; index++) {
            int startBatchIndex = ((index - 1) * batch_size)+1;
            int endBatchIndex = index * batch_size;
            endBatchIndex = endBatchIndex > totalStock ? totalStock : endBatchIndex;

            List<StockData> stockData = kafkaDataService.generateMessage(startBatchIndex, endBatchIndex,totalStock);

            total_record+=stockData.size();
            if (stockData.isEmpty()) {
                return "Error occurred while generating stockData";
            }
            this.sendMessageAsyncBatches(stockData, traceId,startBatchIndex, endBatchIndex);
        }
        long endTime = System.currentTimeMillis();
        log.info("Time taken to finish the stock for count: {},total stock generated: {}, in: {}seconds, traceId: {}", totalStock, total_record, DateTimeHelper.getDifferenceInSecond(startTime, endTime), traceId);
        return "Stock value generated successfully";
    }

    void sendMessageCallBack(List<CompletableFuture<SendResult<String,String>>> futureList , String traceId, int startBatchIndex, int endBatchIndex){
        int success = 0;
        int failure = 0;

        for (CompletableFuture<SendResult<String, String>> future : futureList) {
            try {
                SendResult<String, String> result = future.join();
                success++;
            } catch (Exception e) {
                failure++;
                log.error("FAILED traceId{}, message:{} " ,traceId, e.getMessage());
            }
        }
        log.info("Successfully processed for traceId:{}, totalBatch:{}, success:{}, failure:{} for index: {} - {}",traceId,futureList.size(),success,failure,startBatchIndex,endBatchIndex);
    }

}
