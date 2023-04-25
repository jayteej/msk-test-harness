package com.harness;

import java.util.Optional;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.DeleteRoleRequest;
import com.amazonaws.services.identitymanagement.model.DetachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.ListRolesRequest;
import com.amazonaws.services.identitymanagement.model.Role;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;

public class IAMHelpers {

    private final static Logger logger = LoggerFactory.getLogger(IAMHelpers.class);

    public static Role createIamRole(AmazonIdentityManagement iamClient, AWSSecurityTokenService stsClient,
            String testRolePolicyArn, String testRolePrefix) {
        // Get the currently running assumed role.
        var currentArn = stsClient.getCallerIdentity(new GetCallerIdentityRequest()
                .withRequestCredentialsProvider(DefaultAWSCredentialsProviderChain.getInstance())).getArn();
        logger.info("Current role arn is {}", currentArn);

        // Create a new random role name.
        var iamRoleRandom = testRolePrefix.concat(String.format("%s-%s-%s", RandomStringUtils.randomAlphanumeric(5),
                RandomStringUtils.randomAlphanumeric(5), RandomStringUtils.randomAlphanumeric(5)));

        // When we create the new role, allow the creator to assume
        var createRoleRequest = new CreateRoleRequest()
                .withRoleName(iamRoleRandom)
                .withAssumeRolePolicyDocument(assumeRolePolicy(currentArn));
        var response = iamClient.createRole(createRoleRequest);

        // Attach the general test policy for accessing MSK
        var attachPolicyRequest = new AttachRolePolicyRequest()
                .withRoleName(response.getRole().getRoleName())
                .withPolicyArn(testRolePolicyArn);
        iamClient.attachRolePolicy(attachPolicyRequest);

        logger.info("Created role {} ", response.getRole().getArn());

        return response.getRole();
    }

    private static String assumeRolePolicy(String role) {
        return String.format("""
                {
                    \"Version\": \"2012-10-17\",
                    \"Statement\": [
                      {
                        \"Effect\": \"Allow\",
                        \"Principal\": {
                          \"AWS\": \"%s\"
                        },
                        \"Action\": \"sts:AssumeRole\"
                      }
                    ]
                  }""", role);
    }

    public static void cleanupIamRoles(AmazonIdentityManagement iamClient, String prefix, String policyToRemove) {
        var res = iamClient.listRoles(new ListRolesRequest().withMaxItems(1000));
        res.getRoles().stream().filter(r -> r.getRoleName().startsWith(prefix))
                .forEach(r -> IAMHelpers.deleteTempIamRole(iamClient, r.getRoleName(), policyToRemove));
    }

    public static void deleteTempIamRole(AmazonIdentityManagement iamClient, String roleName, String policyToRemove) {
        Optional.ofNullable(iamClient).ifPresent(ic -> {
            try {
                ic.detachRolePolicy(
                        new DetachRolePolicyRequest().withRoleName(roleName)
                                .withPolicyArn(policyToRemove));
                logger.info("deleting role={}", roleName);
                ic.deleteRole(new DeleteRoleRequest().withRoleName(roleName));
            } catch (Exception ex) {
                logger.error("Unable to delete role ".concat(roleName), ex);
            }
        });
    }


    private static void sleepQuietly(long timeInMs) {
        try {
            Thread.sleep(timeInMs);
        } catch (InterruptedException e) {
        }
    }

    public static AWSStaticCredentialsProvider tryAssumeRole(AWSSecurityTokenService stsClient, Role tempIamRole) {
        DefaultAWSCredentialsProviderChain.getInstance().refresh();
        logger.info("Waiting 30 seconds to make sure IAM policies have caught up.");
        sleepQuietly(30000);

        logger.info("Assume role for {}", tempIamRole.getArn());
        var assumeRequest = new AssumeRoleRequest()
                .withRoleArn(tempIamRole.getArn())
                .withRoleSessionName("msk-test-harness-assume-role-session");

        var assumeResult = stsClient.assumeRole(assumeRequest);
        var credentials = assumeResult.getCredentials();
        var bsc = new BasicSessionCredentials(
                credentials.getAccessKeyId(),
                credentials.getSecretAccessKey(),
                credentials.getSessionToken());
        return new AWSStaticCredentialsProvider(bsc);
    }
}
