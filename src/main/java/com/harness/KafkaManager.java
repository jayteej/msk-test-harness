package com.harness;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClientBuilder;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersRequest;

import jakarta.annotation.PostConstruct;

@Component
public class KafkaManager {

    private static final short RF = 3;

    @Value("${num.partitions}")
    private Integer numPartitions;

    @Value("${test.topic.count}")
    private Integer testTopicCount;

    @Value("${test.topic.prefix}")
    private String testTopicPrefix;

    @Value("${test.topic.fixed}")
    private boolean useFixedTestTopics;

    @Value("${cleanup.old.topics}")
    private boolean cleanupOldTopics;

    @Value("${cluster.arn}")
    private String clusterArn;

    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    private List<String> testTopics;

    private final Random random = new Random();
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AWSKafka mskClient;

    public KafkaManager() {
        this.mskClient = AWSKafkaClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    @PostConstruct
    private void init() {
        this.clusterName = this.clusterArn.split("/")[1];
        // @Value is only populated after constructor.
        if (cleanupOldTopics) {
            cleanupOldTopics();
        }
        createTestTopics();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteTestTopics(testTopics)));
    }

    private void createTestTopics() {
        if (cleanupOldTopics)
            return;
        var adminClient = AdminClient.create(iamPropsAdminClient());
        if (useFixedTestTopics) {
            logger.info("Using fixed naming test topics on cluster {}", clusterName);
            this.testTopics = IntStream.rangeClosed(1, testTopicCount)
                    .mapToObj(i -> createTopic(false, testTopicPrefix.concat(String.valueOf(i)), adminClient))
                    .collect(Collectors.toList());
        } else {
            logger.info("Generating random test topics with prefix {}", testTopicPrefix);
            this.testTopics = IntStream.rangeClosed(1, testTopicCount)
                    .mapToObj(i -> createTopic(true, null, adminClient))
                    .collect(Collectors.toList());
        }
        adminClient.close(Duration.ofSeconds(5));
    }

    public List<String> getRandomTopic() {
        return Collections.singletonList(this.testTopics.get(random.nextInt(this.testTopics.size())));
    }

    public List<String> getTestTopics() {
        return this.testTopics;
    }

    public void deleteTestTopics(List<String> topicsToDelete) {
        if (useFixedTestTopics && !cleanupOldTopics) {
            logger.info("Test topics are fixed and therefore we will not tidy them up.");
            logger.info("If you wish to clean them up, restart with cleanup.old.topics=true");
            return;
        }
        Optional.ofNullable(topicsToDelete).filter(tt -> tt.size() > 0).ifPresent(tt -> {
            logger.info("deleting {} topics", tt.size());
            var adminClient = AdminClient.create(iamPropsAdminClient());
            try {
                adminClient.deleteTopics(tt).all().get();
            } catch (Exception ex) {
                logger.warn("Failed to delete topic ".concat(tt.toString()), ex);
            } finally {
                adminClient.close(Duration.ofSeconds(5));
            }
        });
    }

    private String createTopic(boolean runtimeOnFailure, String overrideName, AdminClient adminClient) {
        var topicName = overrideName == null
                ? testTopicPrefix.concat(String.format("%s-%s-%s", RandomStringUtils.randomAlphanumeric(5),
                        RandomStringUtils.randomAlphanumeric(5), RandomStringUtils.randomAlphanumeric(5)))
                : overrideName;
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("retention.ms", String.valueOf(6 * 60 * 60 * 1000)); // 6 hours millis
        topicConfig.put("retention.bytes", String.valueOf(50L * 1024 * 1024 * 1024)); // 50 GB in bytes

        var request = new NewTopic(topicName, numPartitions, RF).configs(topicConfig);

        int retryCount = 0, maxRetries = 50;

        try {
            adminClient.createTopics(Collections.singleton(request)).all().get();
            while (retryCount < maxRetries) {
                if (!adminClient.listTopics().names().get().contains(topicName)) {
                    if (++retryCount >= maxRetries) {
                        var message = String.format("Topic named %s was not present after creation.", topicName);
                        throw new RuntimeException(message);
                    } else {
                        logger.info("Retry {}", retryCount);
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                } else {
                    logger.info("Created topic {}", topicName);
                    break;
                }
            }
        } catch (ExecutionException | InterruptedException ex) {
            if (runtimeOnFailure) {
                logger.error("Error when creating topic.", ex);
                throw new RuntimeException(ex);
            } else {
                logger.warn("Problem when creating topic. Expected if using fixed and already exists: {}",
                        ex.getMessage());
            }

        }
        return topicName;
    }

    public void cleanupOldTopics() {
        logger.info("Cleaning up old topics...");
        var adminClient = AdminClient.create(iamPropsAdminClient());
        var response = adminClient.listTopics();
        try {
            var allTopics = response.listings().get();
            logger.info("Found {} topics", allTopics.size());
            this.testTopics = allTopics.stream().map(tl -> tl.name())
                    .filter(topic -> topic.startsWith(testTopicPrefix))
                    .collect(Collectors.toList());
            logger.info("test topics to delete {} {}", testTopics.size(), testTopics.toString());
            deleteTestTopics(testTopics);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            adminClient.close(Duration.ofSeconds(5));
        }

    }

    private Properties saslProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", getBrokers(false));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                "alice", "alice-secret"));
        logger.info(props.toString());
        return props;
    }

    Properties iamPropsAdminClient() {
        Properties props = new Properties();
        props.put("bootstrap.servers", getBrokers(true));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return props;
    }

    public String getBrokers(boolean useIam) {
        var request = new GetBootstrapBrokersRequest();
        request.setClusterArn(clusterArn);
        request.setSdkRequestTimeout(10000);
        try {
            var response = mskClient.getBootstrapBrokers(request);
            logger.info("bootstrap servers={}", response.getBootstrapBrokerStringSaslIam());
            return useIam ? response.getBootstrapBrokerStringSaslIam() : response.getBootstrapBrokerStringSaslScram();
        } catch (Exception ex) {
            logger.error("unable to get broker strings", ex);
            throw new RuntimeException(ex);
        }

    }

}
