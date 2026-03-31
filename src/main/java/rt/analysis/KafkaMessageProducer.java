package rt.analysis;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import rt.analysis.pojo.StockData;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    KafkaDataService kafkaDataService;

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    public KafkaMessageProducer(ExecutorService executorService) {
        this.objectMapper = new ObjectMapper();
        this.executorService = executorService;
    }

    public long produceMessage(List<StockData> messages) throws InterruptedException {
        try {
            AtomicLong size = new AtomicLong();
            executorService.submit(() -> {
                for (StockData message : messages) {
                    String jsonMessage = objectMapper.writeValueAsString(message);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.getName() + LocalDateTime.now().toString(), jsonMessage);
                    CompletableFuture<SendResult<String, String>> future = producer.send(record);
                    future.thenAccept(result -> {
                        log.info("send successfully:" + result.getRecordMetadata().offset());
                    }).exceptionally(ex -> {
                        log.error("Error: " + ex.getMessage());
                        throw new RuntimeException(ex);
                    });
                    size.addAndGet(record.value().toString().getBytes().length);
                }
                producer.flush();
            });
            executorService.wait(100);
            return size.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;
    }

    public void produceMessageInBatchesAsync(List<StockData> messages, String traceId) throws InterruptedException {
        try {
            executorService.submit(() -> {
                List<CompletableFuture<?>> futureList = new ArrayList<>();

                for (StockData message : messages) {
                    String jsonMessage = objectMapper.writeValueAsString(message);
                    ProducerRecord<String,String> record = new ProducerRecord<>(topic, message.getName() + LocalDateTime.now().toString(), jsonMessage);
                    record.headers().add("traceId", traceId.getBytes());
                    CompletableFuture<?> future = producer.send(record);
                    futureList.add(future);
                }
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                    .exceptionally(ex -> {
                        log.error("Error: " + ex.getMessage() + " \n" + ex.getLocalizedMessage());
                        throw new RuntimeException(ex);
                    });
                producer.flush();
            });
        } catch (Exception e) {
            throw e;

        }
    }

    public String sendStockValuesInBatches(int count) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        log.info("Stock Value Generation started for count" +count);
        String traceId = "stock_"+startTime;
        int total_record=0;
        for (int i = 1; i <= (count / batch_size) + 1; i++) {
            int end = i * batch_size;
            end = end > count ? count : end;
            List<StockData> stockData = kafkaDataService.createMessage((i - 1) * batch_size, end,count);
            total_record+=stockData.size();
            if (stockData.isEmpty()) {
                return "Error while generating message";
            }
            this.produceMessageInBatchesAsync(stockData, traceId);
        }
        long endTime = System.currentTimeMillis();
        log.info("Time taken to finish the stock for count: " + count +",total stock generated: "+total_record+ ", in: "
                + DateTimeHelper.getDifferenceInSecond(startTime, endTime) + "seconds, traceId: " +traceId);
        return "Message generated successfully";
    }


}
