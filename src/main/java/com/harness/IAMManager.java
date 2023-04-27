package com.harness;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.Role;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import jakarta.annotation.PostConstruct;

@Component
public class IAMManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AmazonIdentityManagement iamClient;
    private final AWSSecurityTokenService stsClient;

    @Autowired
    private RandomStartupJitter randomStartupJitter;

    @Value("${stop.jitter.seconds}")
    private int stopJitterSeconds;

    @Value("${test.role.prefix}")
    private String testRolePrefix;

    @Value("${test.role.policy.arn}")
    private String testRolePolicyArn;

    @Value("${cleanup.old.roles}")
    private boolean cleanupOldRoles;

    private Role tempIamRole;

    public IAMManager() {
        this.iamClient = AmazonIdentityManagementClientBuilder.defaultClient();
        this.stsClient = AWSSecurityTokenServiceClientBuilder.defaultClient();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanIam();
            // Loggers may have closed by now, so also using stdout.
            System.out.println("stdout:: finished cleanIam()");
        }));
    }

    @PostConstruct
    public void runCleanupIfEnabled() {
        // This is a flag that can be set in properties to intially remove all old test
        // roles.
        // ONLY ENABLE FROM SINGLE PROCESS IN EC2.
        if (cleanupOldRoles) {
            doBulkRoleCleanup();
        }
    }

    public synchronized Role getTempIamRole() {
        if (!randomStartupJitter.hasWaited()) {
            throw new RuntimeException("Autowired did not complete randomStartupJitter init before this.");
        }
        if (this.tempIamRole == null) {
            this.tempIamRole = IAMHelpers.createIamRole(iamClient, stsClient, testRolePolicyArn, testRolePrefix);
        }
        return this.tempIamRole;
    }

    public void cleanIam() {
        logger.info("Sigterm recieced: cleanIam()");
        if (tempIamRole != null) {
            // Sigterm in ECS has 30 seconds grace, then sigkill.
            int randomSeconds = new Random().nextInt(stopJitterSeconds) + 1;
            logger.info("Waiting {} seconds jitter before cleaning IAM.", randomSeconds);
            Utils.sleepQuietly(randomSeconds * 1000);
            logger.info("Clean IAM...");
            IAMHelpers.deleteTempIamRole(iamClient, tempIamRole.getRoleName(), testRolePolicyArn);
            logger.info("Clean IAM - completed.");
        }
    }

    public void tryAssumeRole() {
        IAMHelpers.tryAssumeRole(stsClient, tempIamRole);
    }

    public void doBulkRoleCleanup() {
        IAMHelpers.cleanupIamRoles(iamClient, testRolePrefix, testRolePolicyArn);
    }

}
