package com.harness;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import com.amazonaws.services.identitymanagement.model.Role;

import jakarta.annotation.PostConstruct;

@Controller
public class ProduceToKafka {

    private static final int STATS_RATE_SECONDS = 10;

    @Autowired
    private KafkaManager kafkaManager;

    @Autowired
    private IAMManager iamManager;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private volatile KafkaProducer<String, String> kafkaProducer;
    private ScheduledExecutorService ses;
    private ScheduledExecutorService badClient;
    private List<String> testTopics;

    @Value("${cluster.arn}")
    private String clusterArn;

    @Value("${start.producer}")
    private boolean startProducer;

    @Value("${use.dynamic.roles}")
    private boolean useDynamicRoles;

    @Value("${producer.recreate.connection}")
    private boolean recreateProducerConnection;

    @Value("${producer.messages.sleep.between}")
    private int sleepBetweenProduces;

    @Value("${producer.recreate.connection.every.seconds}")
    private int recreateConnectionSeconds;

    private Role tempIamRole;
    private volatile long lastSeenMessagesSent = 0L;
    private volatile long messagesSentAndAcked = 0L;
    private boolean running = false;
    private Properties props;

    @PostConstruct
    void initAndStart() {
        if (startProducer) {
            logger.info("Initialising and starting ProduceToKafka");

            if (useDynamicRoles) {
                this.tempIamRole = iamManager.getTempIamRole();
                iamManager.tryAssumeRole();
            }

            this.props = iamProps();
            this.testTopics = kafkaManager.getTestTopics();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> stopProducer()));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> stopStats()));

            this.ses = Executors.newScheduledThreadPool(1);
            ses.scheduleAtFixedRate(this::stats, 0, STATS_RATE_SECONDS, TimeUnit.SECONDS);
            this.kafkaProducer = new KafkaProducer<>(this.props);
            if (recreateProducerConnection) {
                // This is for simulating a badly behaving client that creates a new client on
                // each produce.
                logger.warn("Badly behaving producer is enabled, new client created every {} seconds",
                        recreateConnectionSeconds);
                this.badClient = Executors.newScheduledThreadPool(1);
                badClient.scheduleAtFixedRate(() -> {
                    this.kafkaProducer = new KafkaProducer<>(this.props);
                }, recreateConnectionSeconds, recreateConnectionSeconds,
                        TimeUnit.SECONDS);
            }
            CompletableFuture.runAsync(() -> startProducing());
        } else {
            logger.info("Producing is disabled.");
        }
    }

    public void startProducing() {
        running = true;
        while (running) {
            testTopics.forEach(this::produce);
            Utils.sleepQuietly(sleepBetweenProduces);
        }
        closeProducer();
    }

    public void stopProducer() {
        logger.info("Shutting down producer...");
        running = false;
        logger.info("Shutting down producer - completed.");
    }

    public void stopStats() {
        if (ses != null) {
            logger.info("Shutting down stats...");
            Optional.ofNullable(ses).ifPresent(ExecutorService::shutdownNow);
            Optional.ofNullable(badClient).ifPresent(ExecutorService::shutdownNow);
            logger.info("Shutting down stats - completed.");
        }
    }

    void stats() {
        var sentInWindow = messagesSentAndAcked - lastSeenMessagesSent;
        var sentPerSecond = sentInWindow > 0 ? sentInWindow / STATS_RATE_SECONDS : -1;
        logger.info("STATS: sentInWindow={}, sentPerSecond={} topics={}", sentInWindow, sentPerSecond, testTopics);
        lastSeenMessagesSent = messagesSentAndAcked;
    }

    void produce(String topicName) {
        // Setting a key to fix sticky partition bug in older versions of kafka.
        var key = RandomStringUtils.randomAlphanumeric(8);
        var payload = RandomStringUtils.randomAlphanumeric(32);
        logger.debug("Try produce to topic {} with payload {}", topicName, payload);
        var record = new ProducerRecord<String, String>(topicName, key, payload);
        kafkaProducer.send(record, (metadata, ex) -> {
            if (ex != null) {
                logger.error("Unable to produce to ".concat(topicName), ex);
            } else {
                logger.debug("Succesfull produce to topic={} offset={} partition={}", metadata.topic(),
                        metadata.offset(), metadata.partition());
                messagesSentAndAcked++;
            }
        });
    }

    Properties iamProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaManager.getBrokers());
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        if (useDynamicRoles) {
            // Use the dynamically created role.
            props.put("sasl.jaas.config", String.format(
                    "software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"%s\" awsRoleSessionName=\"%s\";",
                    tempIamRole.getArn(), tempIamRole.getRoleName()));
        } else {
            // Use the role of the current machine or ecs task.
            props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }

        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return props;
    }

    void closeProducer() {
        Optional.ofNullable(this.kafkaProducer).ifPresent(kp -> kp.close(Duration.ofSeconds(5)));
    }

}
