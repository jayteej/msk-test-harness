package com.harness;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
public class RandomStartupJitter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${start.jitter.seconds}")
    private int randomStartJitterSeconds;

    private boolean hasRun = false;

    @PostConstruct
    public void waitForRandomTime() {
        int waitTimeInSeconds = new Random().nextInt(randomStartJitterSeconds) + 1;
        logger.info("Waiting {} seconds to start", waitTimeInSeconds);
        Utils.sleepQuietly(waitTimeInSeconds * 1000);
        hasRun = true;
    }

    public boolean hasWaited() {
        return hasRun;
    }

}
