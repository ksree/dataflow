# dataflow : A  framework that lets you easily create spark ETL jobs  using simple configuration files

Dataflow is a software paradigm based on the idea of disconnecting computational actors into stages (pipelines) that can execute concurrently

Set the GCP credentials in the env. variable:
GOOGLE_APPLICATION_CREDENTIALS=C:\Users\Kapil.Sreedharan\secrets\dataflow\blade-ai-282114-34167c2579bd.json
 
Setup Kafka cloud property file :
# Kafka
bootstrap.servers=pkc-4ygn6.europe-west3.gcp.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="IISGACNAE3UDW5WF" password="kcBMkyWshxH8QqjvGd0elGEIR3Q85gU4O73ZRkkFmyz+8I3zgQeOp3xqJRhQk1va";
ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN

# Confluent Cloud Schema Registry
schema.registry.url=https://psrc-lgy7n.europe-west3.gcp.confluent.cloud
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=IOSGASCXQSPVGBXX:yceCkcqTfWAV7ITg9WEAxbkI8dCREKCs/Mz5aNGViTK4Cw5EU3sk+bTNd/I21K26

/home/bladeaico/.confluent/kafka_cloud.config
chmod 775 ~/.confluent/kafka_cloud.config
Create a temporary bucket 
gsutil mb gs://tmpdataflowbucketkafka


Install openjdk 1.8
sudo apt-get install openjdk-8-jre
sudo update-alternatives --config java
#select java-8-openjdk
Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
mvn clean install -DskipTests

gcloud dataproc jobs submit spark \
--cluster=dataflowcluster  \
--region=us-central1 \
--files /home/bladeaico/dataflow/src/main/resources/config/covid_tracking.yaml \
--class=com.ksr.dataflow.Run \
--jars=/home/bladeaico/dataflow/target/dataflow-1.0-SNAPSHOT.jar,gs://spark-lib/bigquery/spark-bigquery-latest.jar \
-- covid_tracking.yaml