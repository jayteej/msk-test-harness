Setup
=====
1. Create an MSK cluster with the steps here
https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html

2. Keep the test host you created, add IAMFull policy permissions to the ec2 role for it.
3. You will need to populate test.role.policy.arn and cluster.arn with the arn of the MSK cluster you created and the policy you created in application.properties.
4. You should be able to test and run locally on the ec2 host before moving to ecs deployment. (Run Application.main or use maven as below)
5. To run in your own ecs cluster you will need to create an ECR repository.
6. Once you have done this you can create and upload your docker image as per below. The tag names can be whatever you like.
7. Follow this guide for creating the ECS/ECR components. You can ignore the application creation part as you will deploy this application. https://mydeveloperplanet.com/2021/09/07/how-to-deploy-a-spring-boot-app-on-aws-ecs-cluster/

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
```
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
```

Running From Maven
==================
```
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Xms2G -Xmx2G"
```

Docker Image Create and Upload
===================
```
mvn clean verify
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com
docker tag msk/harness:0.0.1-SNAPSHOT  <account>.dkr.ecr.us-east-1.amazonaws.com/msk/harness:latest
docker push <account>.dkr.ecr.us-east-1.amazonaws.com/msk/harness:latest
```
