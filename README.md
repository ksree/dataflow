# dataflow : A  framework that lets you easily create spark ETL jobs  using simple configuration files

Dataflow is a software paradigm based on the idea of disconnecting computational actors into stages (pipelines) that can execute concurrently

Set the GCP credentials in the env. variable:
GOOGLE_APPLICATION_CREDENTIALS=*.json
 
Setup Kafka cloud property file :
# Kafka

# Confluent Cloud Schema Registry
schema.registry.url=https://psrc-lgy7n.europe-west3.gcp.confluent.cloud
basic.auth.credentials.source=
schema.registry.basic.auth.user.info=

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
--files ~/dataflow/src/main/resources/config/covid_tracking.yaml \
--class=com.ksr.dataflow.Run \
--jars=~/dataflow/target/dataflow-1.0-SNAPSHOT.jar,gs://spark-lib/bigquery/spark-bigquery-latest.jar \
-- covid_tracking.yaml