package com.harness;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.identitymanagement.model.Role;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
public class ProduceToKafka {

    private static final String IAM = "iam";
    private static final int STATS_RATE_SECONDS = 10;

    @Autowired
    private KafkaManager kafkaManager;

    @Autowired
    private IAMManager iamManager;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DescriptiveStatistics stats = new DescriptiveStatistics();

    private volatile KafkaProducer<String, String> kafkaProducer;
    private ScheduledExecutorService ses;
    private ScheduledExecutorService badClient;
    private List<String> testTopics;
    private Map<String, KafkaProducer<String, String>> topicToProducer;

    @Value("${cluster.arn}")
    private String clusterArn;

    @Value("${producer.linger.ms}")
    private int producerLinger;

    @Value("${producer.batch.kb}")
    private int producerBatchKb;

    @Value("${producer.use.per.topic.connection}")
    private boolean connectionPerTopic;

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

    @Value("${auth.method}")
    private String authMethod;

    @Value("${sasl.scram.username:foo}")
    private String username;

    @Value("${sasl.scram.password:bar}")
    private String password;

    @Value("${enable.scatter}")
    private boolean enableScatter;

    private Role tempIamRole;
    private volatile long lastSeenMessagesSent = 0L;
    private volatile long messagesSentAndAcked = 0L;
    private boolean running = false;
    private Properties props;
    private AmazonCloudWatch cwClient;

    @PostConstruct
    void initAndStart() {
        if (startProducer) {
            logger.info("Initialising and starting ProduceToKafka");

            if (useDynamicRoles && useIam()) {
                this.tempIamRole = iamManager.getTempIamRole();
                iamManager.tryAssumeRole();
            }

            this.props = useIam() ? iamProps() : saslProps();
            this.testTopics = enableScatter ? kafkaManager.getRandomTopic() : kafkaManager.getTestTopics();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> stopProducer()));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> stopStats()));

            this.cwClient = AmazonCloudWatchClientBuilder.defaultClient();

            this.ses = Executors.newScheduledThreadPool(1);
            ses.scheduleAtFixedRate(this::stats, 0, STATS_RATE_SECONDS, TimeUnit.SECONDS);
            if (connectionPerTopic) {
                log.info("Using connection per topic, topics={}", testTopics.size());
                this.topicToProducer = this.testTopics.stream()
                        .collect(Collectors.toMap(Function.identity(),
                                topic -> new KafkaProducer<String, String>(this.props)));
            } else {
                this.kafkaProducer = new KafkaProducer<>(this.props);
            }

            if (recreateProducerConnection && !connectionPerTopic) {
                // This is for simulating a badly behaving client that creates a new client on
                // each produce.
                logger.warn("Badly behaving producer is enabled, new client created every {} seconds",
                        recreateConnectionSeconds);
                this.badClient = Executors.newScheduledThreadPool(1);
                badClient.scheduleAtFixedRate(() -> {
                    var oldProducer = this.kafkaProducer;
                    this.kafkaProducer = new KafkaProducer<>(this.props);
                    oldProducer.close(Duration.ofSeconds(5));
                }, recreateConnectionSeconds, recreateConnectionSeconds,
                        TimeUnit.SECONDS);
            } else if (recreateProducerConnection && connectionPerTopic) {
                var msg = "Badly behaving connection and multiple connections per process not currently supported.";
                logger.warn(msg);
                throw new RuntimeException(msg);
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
            if (sleepBetweenProduces > 0) {
                Utils.sleepQuietly(sleepBetweenProduces);
            }
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
        var latencyMax = stats.getMax();
        var latencyMean = stats.getMean();
        var latencyP99 = stats.getPercentile(0.99);
        logger.info("STATS: sentInWindow={}, sentPerSecond={} topics={}", sentInWindow, sentPerSecond, testTopics);
        logger.info("Produce Latency Stats Window={}s: max={}ms mean={}ms p99={}ms", STATS_RATE_SECONDS, latencyMax,
                latencyMean, latencyP99);
        lastSeenMessagesSent = messagesSentAndAcked;
        if(latencyMax > 0) {
            sendMetric("ProduceLatencyMaxMs", latencyMax);
        }
        if(latencyMean > 0) {
            sendMetric("ProduceLatencyMeanMs", latencyMean);
        }
        if(latencyP99 > 0) {
            sendMetric("ProduceLatencyP99Ms", latencyP99);
        }
        stats.clear();
    }

    private void sendMetric(String metricName, Double value) {
        MetricDatum datum = new MetricDatum()
                .withMetricName(metricName)
                .withUnit("Milliseconds")
                .withValue(value)
                .withDimensions(new Dimension().withName("Topic").withValue("All"));

        PutMetricDataRequest request = new PutMetricDataRequest()
                .withNamespace("KafkaTestHarness")
                .withMetricData(datum);

        cwClient.putMetricData(request);
    }

    void produce(String topicName) {
        // Setting a key to fix sticky partition bug in older versions of kafka.
        var key = RandomStringUtils.randomAlphanumeric(8);
        var payload = System.currentTimeMillis() + "," + RandomStringUtils.randomAlphanumeric(500);

        logger.debug("Try produce to topic {} with payload {}", topicName, payload);
        var record = new ProducerRecord<String, String>(topicName, key, payload);

        // We support using 1 producer aka 1 connection, or 1 connection/producer per
        // topic.
        var producer = connectionPerTopic ? topicToProducer.get(topicName) : kafkaProducer;
        long startTime = System.currentTimeMillis();
        producer.send(record, (metadata, ex) -> {
            if (ex != null) {
                logger.error("Unable to produce to ".concat(topicName), ex);
                if(ex instanceof AuthorizationException) {
                    logger.warn("Stopping produce due to authorisation exception: ".concat(topicName), ex);
                    stopProducer();
                }
            } else {
                logger.debug("Succesfull produce to topic={} offset={} partition={}", metadata.topic(),
                        metadata.offset(), metadata.partition());
                messagesSentAndAcked++;

            }
            stats.addValue(System.currentTimeMillis() - startTime);
        });
    }

    private Properties saslProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaManager.getBrokers(false));
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchKb * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
        props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password));
        logger.info(props.toString());
        return props;
    }

    private boolean useIam() {
        return authMethod.equalsIgnoreCase(IAM);
    }

    Properties iamProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaManager.getBrokers(useIam()));
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchKb * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
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
        logger.info(props.toString());
        return props;
    }

    void closeProducer() {
        Optional.ofNullable(this.kafkaProducer).ifPresent(kp -> kp.close(Duration.ofSeconds(5)));
        Optional.ofNullable(topicToProducer)
                .map(Map::values).stream().flatMap(Collection::stream)
                .forEach(kp -> kp.close(Duration.ofSeconds(5)));
    }

}
