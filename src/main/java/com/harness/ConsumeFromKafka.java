package com.harness;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import com.amazonaws.services.identitymanagement.model.Role;

import jakarta.annotation.PostConstruct;

@Controller
public class ConsumeFromKafka {

    private static final int STATS_RATE_SECONDS = 10;
    private static final String IAM = "iam";

    @Autowired
    private KafkaManager kafkaManager;

    @Autowired
    private IAMManager iamManager;

    @Value("${use.dynamic.roles}")
    private boolean useDynamicRoles;

    @Value("${auth.method}")
    private String authMethod;

    @Value("${sasl.scram.username:foo}")
    private String username;

    @Value("${sasl.scram.password:bar}")
    private String password;

    @Value("${start.consumer}")
    private boolean startConsumer;

    @Value("${num.consumer.groups}")
    private int numConsumerGroups;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Role tempIamRole;
    private KafkaConsumer<String, String> kafkaConsumer;
    private boolean running;
    private long messagesConsumed = 0L;
    private long lastMessagesConsumed = 0L;
    private ScheduledExecutorService ses;

    @PostConstruct
    public void init() {
        if (startConsumer) {
            logger.info("Initialising and starting ConsumeFromKafka");

            if (useDynamicRoles && useIam()) {
                this.tempIamRole = iamManager.getTempIamRole();
                iamManager.tryAssumeRole();
            }

            kafkaConsumer = new KafkaConsumer<>(useIam() ? iamProps() : saslProps());
            CompletableFuture.runAsync(() -> startConsumer());
            startStats();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> stopConsumer()));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> stopStats()));
        } else {
            logger.info("Consumption is disabled.");
        }

    }

    public void stopStats() {
        this.ses.shutdownNow();
    }

    private void startStats() {
        this.ses = Executors.newScheduledThreadPool(1);
        ses.scheduleAtFixedRate(this::stats, 0, STATS_RATE_SECONDS, TimeUnit.SECONDS);
    }

    void stats() {
        var recInWindow = messagesConsumed - lastMessagesConsumed;
        var recPerSecond = recInWindow > 0 ? recInWindow / STATS_RATE_SECONDS : -1;
        logger.info("STATS: recInWindow={}, recPerSecond={}", recInWindow, recPerSecond);
        lastMessagesConsumed = messagesConsumed;
    }

    public void stopConsumer() {
        logger.info("Stopping consumer...");
        running = false;
    }

    private void startConsumer() {
        logger.info("Starting consumer for {}", kafkaManager.getTestTopics().toString());
        running = true;
        kafkaConsumer.subscribe(kafkaManager.getTestTopics());
        while (running) {
            try {
                var records = kafkaConsumer.poll(Duration.ofMillis(100));
                messagesConsumed += records.count();
            } catch (Exception ex) {
                logger.error("Error during consume.", ex);
            }
        }
        kafkaConsumer.close(Duration.ofSeconds(5));
    }

    private Properties saslProps() {
        var consumerGroup = numConsumerGroups != -1
                ? "msk-test-harness-group-" + (new Random().nextInt(numConsumerGroups) + 1)
                : "msk-test-harness";
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaManager.getBrokers(false));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_SSL");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password));
        return props;
    }

    private boolean useIam() {
        return authMethod.equalsIgnoreCase(IAM);
    }

    Properties iamProps() {
        Properties props = new Properties();
        var currentRoleUserId = iamManager.getCurrentRole();
        props.put("bootstrap.servers", kafkaManager.getBrokers(true));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_SSL");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                useDynamicRoles ? tempIamRole.getRoleName() : "msk-test-harness");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        if (useDynamicRoles) {
            // Use the dynamically created role.
            props.put("sasl.jaas.config", String.format(
                    "software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"%s\" awsRoleSessionName=\"%s\";",
                    tempIamRole.getArn(), tempIamRole.getRoleName()));
        } else {
            // Use the role of the current machine or ecs task.
            logger.info("Not using dynamic roles for consumer, using {}", currentRoleUserId);
            props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return props;
    }

}
