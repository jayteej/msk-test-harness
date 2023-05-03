package com.harness;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
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

    private static final int NUM_PARTITIONS = 3;
    private static final short RF = 3;

    @Value("${test.topic.count}")
    private Integer testTopicCount;

    @Value("${test.topic.prefix}")
    private String testTopicPrefix;

    @Value("${test.topic.fixed:#{null}}")
    private String testTopicFixed;

    @Value("${cleanup.old.topics}")
    private boolean cleanupOldTopics;

    @Value("${cluster.arn}")
    private String clusterArn;

    private List<String> testTopics;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AWSKafka mskClient;
    private AdminClient adminClient;

    public KafkaManager() {
        this.mskClient = AWSKafkaClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

    }

    @PostConstruct
    private void init() {
        // @Value is only populated after constructor.
        this.adminClient = AdminClient.create(iamPropsAdminClient());
        if (cleanupOldTopics) {
            cleanupOldTopics();
        }
        createTestTopics();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteTestTopics(testTopics)));
    }

    private void createTestTopics() {

        if (testTopicFixed != null && testTopicFixed.length() > 0) {
            logger.info("Using fixed test topics {}", testTopicFixed);
            this.testTopics = Arrays.stream(this.testTopicFixed.split(","))
                    .map(providedName -> createTopic(false, providedName))
                    .collect(Collectors.toList());
        } else {
            logger.info("Generating test topics with prefix {}", testTopicPrefix);
            this.testTopics = IntStream.rangeClosed(1, testTopicCount)
                    .mapToObj(i -> createTopic(true, null))
                    .collect(Collectors.toList());
        }
    }

    public List<String> getTestTopics() {
        return this.testTopics;
    }

    public void deleteTestTopics(List<String> topicsToDelete) {
        if (testTopicFixed != null && testTopicFixed.length() > 0) {
            logger.info("Test topics are fixed and therefore we will not tidy them up.");
            return;
        }
        Optional.ofNullable(topicsToDelete).filter(tt -> tt.size() > 0).ifPresent(tt -> {
            logger.info("deleting {} topics", tt.size());
            try {
                adminClient.deleteTopics(tt).all().get();
            } catch (Exception ex) {
                logger.warn("Failed to delete topic ".concat(tt.toString()), ex);
            }
        });
    }

    private String createTopic(boolean runtimeOnFailure, String overrideName) {
        var topicName = overrideName == null
                ? testTopicPrefix.concat(String.format("%s-%s-%s", RandomStringUtils.randomAlphanumeric(5),
                        RandomStringUtils.randomAlphanumeric(5), RandomStringUtils.randomAlphanumeric(5)))
                : overrideName;
        var request = new NewTopic(topicName, NUM_PARTITIONS, RF);

        try {
            adminClient.createTopics(Collections.singleton(request)).all().get();
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                var message = String.format("Topic named %s was not present after creation.", topicName);
                throw new RuntimeException(message);
            } else {
                logger.info("Created topic {}", topicName);
            }
        } catch (ExecutionException | InterruptedException ex) {
            if (runtimeOnFailure) {
                logger.error("Error when creating topic.", ex);
                throw new RuntimeException(ex);
            } else {
                logger.warn("Problem when creating topic. Expected if using fixed and already exists: {}", ex.getMessage());
            }

        }
        return topicName;
    }

    public void cleanupOldTopics() {
        logger.info("Cleaning up old topics...");
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
        }

    }

    Properties iamPropsAdminClient() {
        Properties props = new Properties();
        props.put("bootstrap.servers", getBrokers());
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return props;
    }

    public String getBrokers() {
        var request = new GetBootstrapBrokersRequest();
        request.setClusterArn(clusterArn);
        request.setSdkRequestTimeout(10000);
        try {
            var response = mskClient.getBootstrapBrokers(request);
            logger.info("bootstrap servers={}", response.getBootstrapBrokerStringSaslIam());
            return response.getBootstrapBrokerStringSaslIam();
        } catch (Exception ex) {
            logger.error("unable to get broker strings", ex);
            throw new RuntimeException(ex);
        }

    }

}
