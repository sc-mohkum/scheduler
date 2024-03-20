package kafka.scheduler;

import kafka.scheduler.config.KafkaConfig;
import kafka.scheduler.config.MongoConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController

public class SchedulerController {
    @Autowired
    private KafkaToMongoDBScheduler messageScheduler;
    @Autowired
    private KafkaConfig kafkaConfig;
    @Autowired
    MongoConfig mongoConfig;

    @PostMapping("/start-scheduler")
    public String startScheduler() {
        // The scheduler will start automatically when the application starts

        messageScheduler.fetchAndSaveMessages();
        return "Scheduler started";
    }
}
