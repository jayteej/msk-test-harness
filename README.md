Cleanup after OutOfMemory
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
