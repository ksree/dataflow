 dataflow 
===========================

 
 Is a framework that lets you easily create spark ETL jobs  using simple configuration files(YAML/JSON)

**Prerequisite**

Requires JDK8 

**How to create a new ELT job using dataflow?**

To create a new ETL job, define 2 YAML configuration. 

The first configuration  file consists the following information :
  - Input sources(eg. CSV,JSON, parquet, Azure, Kafka, JDBC)
  - Transformation definition file
  - Output sinks(eg. CSV, JSON, parquet, Redshift, JDBC, Kafka, GCS, Azure)

For example a configuration that reads form input source Azure Blob storage, defines a transformation file
 and wirtes to an Output sink is as follows:
 
```yaml
appName: CovidCaseTracker

inputs:
  covid:
    azure:
      storageType: AzureBlobStorage
      containerName: public
      storageAccountName: pandemicdatalake
      blob_sas_token: ""
      blob_relative_path: curated/covid-19/ecdc_cases/latest/ecdc_cases.csv
      format: csv
      options:
        quoteAll: false

transformations:
  - /config/covid_tracking_transformations.yaml

output:
  file:
    dir: gs://dataflow_covid_data
```

The second configuration file(referenced above) defines the transformations that you want to apply on your data
Here we apply Spark SQL transformations on covid dataframes defined in the first config file. And the 
new dataframes are writen into GCPBigQuery sink 
 
```yaml
steps:
  - dataFrameName: casesInUS
    sql:
      SELECT *
      FROM covid
      WHERE COUNTRIES_AND_TERRITORIES='United_States_of_America'
  - dataFrameName: casesInCanada
    sql:
      SELECT *
      FROM covid
      WHERE COUNTRIES_AND_TERRITORIES='Canada'
output:
    outputType: GCPBigQuery
    outputOptions:
      saveMode: Append
      dbTable: dataflow.casesInUS
  - dataFrameName: casesInCanada
    outputType: GCPBigQuery
    outputOptions:
      saveMode: Append
      dbTable: dataflow.casesInCanada
```
**Running test cases:**

For more samples checkout out [test configs](src/test/resources/config)



**Acknowlegement**

This project is build on top of a  lightweight version of the amazing opensource spark ETL framework [metorikku](https://github.com/YotpoLtd/metorikku)
Check it out if you require more advanced features like instrumentation. 

 