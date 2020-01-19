# Ch6 Ingesting data with Spark Streaming

* 6.1. Writing Spark Streaming applications
* 6.2. Using external data sources
* 6.3. Performance of Spark Streaming jobs
* 6.4. Structured Streaming
* 6.5. Summary

> `Spark Streaming` has connectors for reading data from `Hadoop-compatible filesystems` (such as HDFS and S3) and `distributed systems ` (such as Flume, Kafka, and Twitter)

## 6.1 Write Spark Streaming application


> spark uses `mini-batches` to provide streaming functionalities </br>
> concept: [https://livebook.manning.com/book/spark-in-action/chapter-6/12](https://livebook.manning.com/book/spark-in-action/chapter-6/12) </br>

### 6.1.1 Introducing the example in this chapter

[https://livebook.manning.com/book/spark-in-action/chapter-6/17](/https://livebook.manning.com/book/spark-in-action/chapter-6/17)

### 6.2.2 create streaming context 

`Spark standalone`, `YARN`, or `Mesos cluster`: CH10, 11, 12

> make sure to have more than one core available to your executors: </br>
> 1 core (thread) for reading incoming data, others for calculation  

example (local cluster)

~~~shell
$ spark-shell --master local[4]
scala> import org.apache.spark._
scala> import org.apache.spark.streaming._
scala> val ssc = new StreamingContext(
						sc,        // SparkContext provided by spark-shell
						Seconds(5) // mini-batch interval
					) 
~~~

besides reusing existing `SparkContext`, also can start a new `SparkContext` by giving a `SparkConf` object

~~~scala
// run it on a new JVM 
// each JVM can only instantiate 1 Spark context
val conf = new SparkConf().setMaster("local[4]").setAppName("App name")
val ssc = new StreamingContext(conf, Seconds(5))
~~~

### 6.1.3 Creating a discretized stream

#### Downloading the data to be streamed

#### Creating a DStream object

### 6.1.4. Using discretized streams

#### Parsing the lines

#### Counting the numbers of buy and sell orders

### 6.1.5. Saving the results to a file

### 6.1.6. Starting and stopping the streaming computation

#### Sending data to Spark Streaming

#### Stopping the Spark Streaming context

#### Examining the generated output

### 6.1.7. Saving the computation state over time

#### Keeping track of the state using updateStateByKey

#### Combining two DStreams using union

#### Specifying the checkpointing directory

#### Starting the streaming context and examining the new output

#### Using the mapWithState method

### 6.1.8. Using window operations for time-limited calculations

#### Solving the final task with window operations

#### Exploring the other window operations

### 6.1.9. Examining the other built-in input streams

#### File input streams

#### Socket input streams

## 6.2. Using external data sources

### 6.2.1. Setting up Kafka

### 6.2.2. Changing the streaming application to use Kafka

#### Using the Spark Kafka connector

#### Writing messages to Kafka

#### Running the example

## 6.3. Performance of Spark Streaming jobs

### 6.3.1. Obtaining good performance

#### Lowering the processing time

#### Increasing parallelism

#### Limiting the input rate

### 6.3.2. Achieving fault-tolerance

#### Recovering from executor failures

#### Recovering from driver failures

## 6.4. Structured Streaming

### 6.4.1. Creating a streaming DataFrame

### 6.4.2. Outputting streaming data

### 6.4.3. Examining streaming executions

### 6.4.4. Future direction of structured streaming

## 6.5. Summary




