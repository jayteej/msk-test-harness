package com.harness;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.Role;
import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClientBuilder;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersRequest;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import jakarta.annotation.PostConstruct;

@Controller
public class ProduceToKafka {

    private static final int STATS_RATE_SECONDS = 10;
    private static final int NUM_PARTITIONS = 3;
    private static final short RF = 3;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private AWSKafka kafkaClient;
    private KafkaProducer<String, String> kafkaProducer;
    private AmazonIdentityManagement iamClient;
    private AWSSecurityTokenService stsClient;
    private AdminClient adminClient;
    private ScheduledExecutorService ses;
    private List<String> testTopics;

    @Value("${cluster.arn}")
    private String clusterArn;

    @Value("${test.topic.prefix}")
    private String testTopicPrefix;

    @Value("${test.role.prefix}")
    private String testRolePrefix;

    @Value("${test.role.policy.arn}")
    private String testRolePolicyArn;

    @Value("${test.topic.count}")
    private Integer testTopicCount;

    @Value("${cleanup.old.roles}")
    private boolean cleanupOldRoles;

    @Value("${start.producer}")
    private boolean startProducer;

    private Role tempIamRole;
    private volatile long lastSeenMessagesSent = 0L;
    private volatile long messagesSentAndAcked = 0L;
    private boolean running = false;

    @PostConstruct
    void initAndStart() {
        logger.info("Using DefaultAWSCredentialsProviderChain");
        this.ses = Executors.newScheduledThreadPool(1);
        this.kafkaClient = AWSKafkaClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        // This is for simulation of multiple iam users where each running produce will
        // have its own iam.
        this.iamClient = AmazonIdentityManagementClientBuilder.defaultClient();
        this.stsClient = AWSSecurityTokenServiceClientBuilder.defaultClient();

        // This is a flag that can be set in properties to intially remove all old test
        // roles.
        // ONLY ENABLE FROM SINGLE PROCESS IN EC2.
        if (cleanupOldRoles) {
            IAMHelpers.cleanupIamRoles(iamClient, testRolePrefix, testRolePolicyArn);
        }

        this.tempIamRole = IAMHelpers.createIamRole(iamClient, stsClient, testRolePolicyArn, testRolePrefix);

        IAMHelpers.tryAssumeRole(stsClient, tempIamRole);

        var props = iamProps();
        this.kafkaProducer = new KafkaProducer<>(props);
        this.adminClient = AdminClient.create(props);
        this.testTopics = createTopics();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopProducer()));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> cleanIam()));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopStats()));

        ses.scheduleAtFixedRate(this::stats, 0, STATS_RATE_SECONDS, TimeUnit.SECONDS);

        if (startProducer) {
            startProducing();
        }
    }

    private List<String> createTopics() {
        return IntStream.rangeClosed(1, testTopicCount)
                .mapToObj(i -> createTopic())
                .collect(Collectors.toList());
    }

    private String createTopic() {
        var topicName = testTopicPrefix.concat(String.format("%s-%s-%s", RandomStringUtils.randomAlphanumeric(5),
                RandomStringUtils.randomAlphanumeric(5), RandomStringUtils.randomAlphanumeric(5)));
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
            logger.error("Error when creating topic.", ex);
            throw new RuntimeException(ex);
        }
        return topicName;
    }

    public void startProducing() {
        running = true;
        while (running) {
            testTopics.forEach(this::produce);
        }
    }

    public void stopProducer() {
        logger.info("Shutting down producer...");
        running = false;
        closeProducer();
        deleteTestTopics();
        logger.info("Shutting down producer - completed.");
    }

    public void cleanIam() {
        logger.info("Clean IAM...");
        IAMHelpers.deleteTempIamRole(iamClient, tempIamRole.getRoleName(), testRolePolicyArn);
        logger.info("Clean IAM - completed.");
    }

    public void stopStats() {
        logger.info("Shutting down stats...");
        ses.shutdownNow();
        logger.info("Shutting down stats - completed.");
    }

    void stats() {
        try {
            long startTime = System.currentTimeMillis();
            var response = adminClient.listTopics();
            var allTopics = response.listings().get();
            long totalTime = System.currentTimeMillis() - startTime;
            logger.info("Topic List Time Ms={}", totalTime);

            startTime = System.currentTimeMillis();
            var allTopicNames = allTopics.stream().map(tl -> tl.name()).collect(Collectors.toList());
            adminClient.describeTopics(allTopicNames).all();
            totalTime = System.currentTimeMillis() - startTime;
            logger.info("Describe List Time Ms={}", totalTime);

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        var sentInWindow = messagesSentAndAcked - lastSeenMessagesSent;
        var sentPerSecond = sentInWindow > 0 ? sentInWindow / STATS_RATE_SECONDS : -1;
        logger.info("STATS: sentInWindow={}, sentPerSecond={}", sentInWindow, sentPerSecond);
        lastSeenMessagesSent = messagesSentAndAcked;
    }

    void produce(String topicName) {
        var payload = RandomStringUtils.randomAlphanumeric(32);
        logger.debug("Try produce to topic {} with payload {}", topicName, payload);
        var record = new ProducerRecord<String, String>(topicName, payload);
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
        props.put("bootstrap.servers", getBrokers());
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", String.format(
                "software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"%s\" awsRoleSessionName=\"producer\";",
                tempIamRole.getArn()));
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return props;
    }

    String getBrokers() {
        var request = new GetBootstrapBrokersRequest();
        request.setClusterArn(clusterArn);
        request.setSdkRequestTimeout(10000);
        try {
            var response = kafkaClient.getBootstrapBrokers(request);
            logger.info("bootstrap servers={}", response.getBootstrapBrokerStringSaslIam());
            return response.getBootstrapBrokerStringSaslIam();
        } catch (Exception ex) {
            logger.error("unable to get broker strings", ex);
            throw new RuntimeException(ex);
        }

    }

    void closeProducer() {
        Optional.ofNullable(this.kafkaProducer).ifPresent(kp -> kp.close(Duration.ofSeconds(5)));
    }

    void deleteTestTopics() {
        Optional.ofNullable(testTopics).filter(tt -> tt.size() > 0).ifPresent(tt -> {
            try {
                adminClient.deleteTopics(tt).all().get();
            } catch (Exception ex) {
                logger.warn("Failed to delete topic ".concat(tt.toString()), ex);
            }
        });
    }

}
