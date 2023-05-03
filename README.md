Setup
=====
1. Create an MSK cluster with the steps here
https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html

2. Keep the test host you created, add IAMFullAccess policy permissions to the ec2 role for it.
3. You will need to populate test.role.policy.arn and cluster.arn with the arn of the MSK cluster you created and the policy you created in application.properties.
4. You should be able to test and run locally on the ec2 host before moving to ecs deployment. (Run Application.main or use maven as below)
5. To run in your own ecs cluster you will need to create an ECR repository.
6. Once you have done this you can create and upload your docker image as per below. The tag names can be whatever you like.
7. Follow this guide for creating the ECS/ECR components. You can ignore the application creation part as you will deploy this application. https://mydeveloperplanet.com/2021/09/07/how-to-deploy-a-spring-boot-app-on-aws-ecs-cluster/
8. The ECS task role and instance role will need creating (uou can use the same one for both). You should attach the policy that works for your ec2 instance above. Also you will need to attach IAMFullAccess, CloudWatchLogsFullAccess, 
and EC2InstanceProfileForImageBuilderECRContainerBuilds policies.

- You can control the behaviour for the load test per process/container by editing `application.properties`.
- You will need to add the cluster arn and the policy to `application.properties` as the process creates its own IAM role (for testing IAM scaling).
- If you make a config change to `application.properties` you will need to republish the image right now.

TODO Ideas
====
- Add support for environment variables to allow configuration from ECS console.
- Wire up cloudwatch metrics for client side visibility during a load test.

Cleanup after OutOfMemory Or Other Unexpected Failure
=========================
```
rm /tmp/topics.txt
$KAFKABIN/kafka-topics.sh --bootstrap-server $KAFKASTR --list | grep msk-test-harness > /tmp/topics.txt
while read topic; do
  echo "Deleting topic $topic"
  $KAFKABIN/kafka-topics.sh --bootstrap-server $KAFKASTR --delete --topic $topic 
done < /tmp/topics.txt
```

Install Maven on Amazon Linux Machine
==================
Assumes you do this in /home/ec2-user/ and are using bash
```
sudo wget https://dlcdn.apache.org/maven/maven-3/3.9.1/binaries/apache-maven-3.9.1-bin.tar.gz
tar xzvf apache-maven-3.9.1-bin.tar.gz
echo 'export PATH=$PATH:/home/ec2-user/apache-maven-3.9.1/bin' >> ~/.bash_profile
source ~/.bash_profile
```

Running From Maven
==================
```
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Xms2G -Xmx2G"
```

Docker Image Create and Upload
===================
Assumes you have populated the env variable $AWS_ACCOUNT_ID
```
mvn clean verify
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
docker tag msk/harness:0.0.1-SNAPSHOT  $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/msk/harness:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/msk/harness:latest
```
