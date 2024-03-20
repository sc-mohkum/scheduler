package kafka.scheduler;

import kafka.scheduler.config.KafkaConfig;
import kafka.scheduler.config.MongoConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.json.JsonParseException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
@Component
public class KafkaToMongoDBScheduler {

    private final Logger logger = LoggerFactory.getLogger(KafkaToMongoDBScheduler.class);

//   @Autowired
//   MongoTemplate mongoTemplate;
//
//    private KafkaConsumer<String, String> consumer;
//
//    private MongoCollection<Document> collection;
//
//
//    @PostConstruct
//    public void initialize() {
//        this.consumer = new KafkaConsumer<>(kafkaConfig.getProperties());
//        this.consumer.subscribe(Collections.singletonList(kafkaConfig.getTopic()));
//        consumer.seekToBeginning(consumer.assignment());
//        MongoClient mongoClient = MongoClients.create(mongoConfig.getUri());
//        MongoDatabase database = mongoTemplate.getDb();
//        this.collection = mongoTemplate.getCollection(mongoConfig.getCollection());
//    }
//    @Scheduled(fixedDelay = 60000) // Every 1 minutes
//    public void consumeAndSaveMessages() {
//        logger.info(collection+"...collectionName");
//        LocalDateTime currentTime = LocalDateTime.now();
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//        String formattedTime = currentTime.format(formatter);
//
//        logger.info("Scheduler started at {}", formattedTime);
//        boolean newDataAdded = false;
//
//        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//        for (ConsumerRecord<String, String> record : records) {
//            Document document = Document.parse(record.value());
//            String id = document.getObjectId("_id").toString();
//
//            if (!collection.find(new Document("_id", id)).iterator().hasNext()) {
//                collection.insertOne(document);
//                logger.info("Saved message to MongoDB: {}", document);
//                newDataAdded = true;
//            }
//        }
//
//        if (!newDataAdded) {
//            logger.info("No new data added to MongoDB");
//        }
//    }

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    @Autowired
    private MongoTemplate mongoTemplate;


    @Scheduled(fixedDelay = 60000) // 5 minutes
    public void fetchAndSaveMessages() {
        LocalDateTime currentTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedTime = currentTime.format(formatter);
        logger.info("Scheduler started at {}", formattedTime);

        kafkaConsumer.subscribe(Collections.singletonList("input2"));
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));


        records.forEach(record -> {
            String message = record.value();
            if (!isMessageAlreadyExists(message)) {
                // New message found, save it to MongoDB
                saveMessageToMongoDB(message);
                logger.info("New data added to MongoDB: {}", message);
            } else {
                // Message already exists in MongoDB
                logger.info("Data found in Kafka but already exists in MongoDB: {}", message);
            }
        });
        if (records.isEmpty()) {
            System.out.println("No data found in Kafka topic.");

        }
    }


    private boolean isMessageAlreadyExists(String message) {
        String deviceId = null;
       try {
           JSONObject jsonObject = new JSONObject(message);
            deviceId = jsonObject.getString("deviceID");
       }
       catch (NullPointerException | JSONException e)
       {
           logger.error("Error in parsing the json"+ e.getMessage());
       }
        // Check if the message with the same deviceID exists in MongoDB
        Query query = new Query();
        query.addCriteria(Criteria.where("deviceID").is(deviceId));
        return mongoTemplate.exists(query, "Data");

    }

    private void saveMessageToMongoDB(String message) {

        mongoTemplate.save(message, "Data");
    }
}