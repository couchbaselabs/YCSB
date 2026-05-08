# BigQuery Driver for YCSB

## Getting Started

### 1\. Start your database

Go to https://console.cloud.google.com/ and create a project.

Go to BigQuery and create a new dataset in the "Explore" tab.  
Make sure to set a specific data location region (e.g. `europe-west10`).

Be sure that you know the following information: Project ID, Dataset ID and Data Location.

### 2\. Set up the table

Create the table by executing the following query (insert your specific Project and Dataset ID):

```
CREATE TABLE <project_id>.<dataset>.usertable (
	YCSB_KEY STRING,
	airplane STRING,
	src_airport STRING,
	dst_airport STRING,
	stops INT64,
	field1 STRING,
	airline_alias STRING,
	airline_name STRING,
	codeshares_0 STRING,
	codeshares_1 STRING,
	codeshares_2 STRING,
  PRIMARY KEY (YCSB_KEY) NOT ENFORCED
 )
```

### 3\. Install and authenticate gcloud

Follow the instructions on this page https://docs.cloud.google.com/bigquery/docs/reference/libraries, namely:

Install `gcloud` (https://docs.cloud.google.com/sdk/docs/install-sdk).

Initialize gcloud by running this command in the console:

```
gcloud init
```

Authenticate by running this command:

```
gcloud auth application-default login
```

### 4\. Build the BigQuery Binding

run the following commands:

```
mvn -pl site.ycsb:core -am package -Dcheckstyle.skip
```

```
mvn -pl site.ycsb:bigquery-binding -am package -Dcheckstyle.skip
```

### 5\. Run YCSB

Run YCSB with the following commands:

```
bin/ycsb.sh load bigquery -p bigquery.projectId=<project_id> -p bigquery.dataset=<dataset_id> -p bigquery.location=<location> -P workloads/<workload>
```

```
bin/ycsb.sh run bigquery -p bigquery.projectId=<project_id> -p bigquery.dataset=<dataset_id> -p bigquery.location=<location> -P workloads/<workload>
```

### Properties

*   bigquery.insertStream: enables the streaming of inserts using InsertAllRequest. This allowes for very fast INSERTS but operations like UPDATE or DELETE might only be possible up to 90 minutes after the INSERT.
*   bigquery.batchSize=: (default 0 -> disabled) enables batch inserts when loading and defines the batchsize for these inserts.