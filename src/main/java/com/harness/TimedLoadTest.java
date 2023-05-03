package com.harness;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import jakarta.annotation.PostConstruct;

@Controller
public class TimedLoadTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private ScheduledExecutorService ses;

    @Autowired
    private RandomStartupJitter randomStartupJitter;
    @Autowired
    private ProduceToKafka produceToKafka;
    @Autowired
    private ConsumeFromKafka consumeFromKafka;
    @Autowired
    private IAMManager iamManager;

    @Value("${time.to.run.for.seconds:-1}")
    private int timeToRunForSeconds;

    @PostConstruct
    void init() {
        if (timeToRunForSeconds == -1) {
            logger.info("Timed run is not enabled");
            return;
        }
        if (!randomStartupJitter.hasWaited()) {
            throw new RuntimeException("Autowired did not complete randomStartupJitter init before this.");
        }
        this.ses = Executors.newScheduledThreadPool(1);
        logger.info("Send a stop signal after {} seconds", timeToRunForSeconds);
        ses.schedule(() -> {
            logger.info(
                    "Invoke stop and cleanup, do not send a sig term as ECS will simply restart the container.");
            produceToKafka.stopProducer();
            produceToKafka.stopStats();
            consumeFromKafka.stopConsumer();
            consumeFromKafka.stopStats();
            iamManager.cleanIam();
        }, timeToRunForSeconds, TimeUnit.SECONDS);
    }
}
