# Features

The following is a list of implemented and planned (Future) features of Smart Data Lake Builder.

##### Filebased metadata
* Easily versionable with a VCS for DevOps
* Flexible structure by splitting over multiple files and subdirectories
* Easily generateable from third party metadata (e.g. source system table catalog) to automate transformation of huge number of DataObjects

##### Support for complex workflows & streaming
* Fork, join, parallel execution, multiple start- & end-nodes possible
* Recovery of failed runs
* Switch a workflow between batch or streaming execution by using just a command line switch

##### Execution Engines
* Spark (DataFrames)
* File (Input&OutputStream)
* Future: Kafka Streams, Flink, …

##### Connectivity
* Spark: diverse connectors (HadoopFS, Hive, DeltaLake, JDBC, Kafka, Splunk, Webservice, JMS) and formats (CSV, JSON, XML, Avro, Parquet, Excel, Access …)
* File: SFTP, Local, Webservice
* Easily extendable through implementing predefined scala traits
* Support for getting secrets from different secret providers
* Support for SQL update & merge (Jdbc, DeltaLake) 

##### Generic Transformations
* Spark based: Copy, Historization, Deduplication (incl. incremental update/merge mode for streaming)
* File based: FileTransfer
* Easily extendable through implementing predefined scala traits
* Future: applying MLFlow machine learning models

##### Customizable Transformations
* Spark Transformations: 
  * Chain predefined standard transformations (e.g. filter, row level data validation and more) and custom transformations within the same action 
  * Custom Transformation Languages: SQL, Scala (Class, compile from config), Python
  * Many input DataFrames to many outputs DataFrames (but only one output recommended normally, in order to define dependencies as detailed as possible for the lineage)
  * Add metadata to each transformation to explain your data pipeline.
* File Transformations: 
  * Language: Scala
  * Only one to one (one InputStream to one OutputStream)

##### Early Validation
* Execution in 3 phases before execution
  * Load Config: validate configuration
  * Prepare: validate connections
  * Init: validate Spark DataFrame Lineage (missing columns in transformations of later actions will stop the execution)

##### Execution Modes
* Process all data
* Partition parameters: give partition values to process for start nodes as parameter
* Partition Diff: search missing partitions and use as parameter
* Spark Incremental: compare sortable column between source and target, load the difference
* Spark Streaming: asynchronous incremental processing by using Spark Structured Streaming
* Spark Streaming Once: synchronous incremental processing by using Spark Structured Streaming with Trigger=Once mode

##### Schema Evolution
* Automatic evolution of data schemas (new column, removed column, changed datatype)
* Support for changes in complex datatypes (e.g. new column in array of struct)
* Automatic adaption of DataObjects with fixed schema (Jdbc, DeltaLake) 

##### Metrics
* Number of rows written per DataObject
* Execution duration per Action
* StateListener interface to get notified about progress & metrics

##### Data Catalog
* Report all DataObjects attributes (incl. foreign keys if defined) for visualisation of data catalog in BI tool
* Metadata support for categorizing Actions and DataObjects
* Custom metadata attributes

##### Lineage
* Report all dependencies between DataObjects for visualisation of lineage in BI tool

##### Data Quality
* Metadata support for primary & foreign keys
* Check & report primary key violations by executing primary key checker action
* Future: Metadata support for arbitrary data quality checks
* Future: Report data quality (foreign key matching & arbitrary data quality checks) by executing data quality reporter action

##### Testing
* Support for CI
  * Config validation
  * Custom transformation unit tests
  * Spark data pipeline simulation (acceptance tests)
* Support for Deployment
  * Dry-run

##### Spark Performance
* Execute multiple Spark jobs in parallel within the same Spark Session to save resources
* Automatically cache and release intermediate results (DataFrames)

##### Housekeeping
* Delete, or archive & compact partitions according to configurable expressions