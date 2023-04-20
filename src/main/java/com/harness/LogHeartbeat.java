package com.harness;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import jakarta.annotation.PostConstruct;

@Controller
public class LogHeartbeat {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);

    @PostConstruct
    private void init() {
        ses.scheduleAtFixedRate(() -> {
            logger.info("Heartbeat");
        }, 30, 30, TimeUnit.SECONDS);
    }
}
