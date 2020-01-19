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

### 6.1.3 Creating a discretized stream (DStream)

> stream the data from a file in the example

#### Downloading the data

A file containing 500,000 lines representing buy and sell orders</br>
Fields includes: `order_timestamp`, `order_id`, `client_id`, `Stock symbol`, `stock_num`, `price`, `buy_or_sell`

~~~
$ cd first-edition/ch06
$ tar xvfz orders.tar.gz
~~~

#### Monitor newly creted files

`StreamingContext.textFileStream` method monitors a Hadoop-comliant directory (HDFS, S2, GlusterFS, local directories, ...) and reads each <b>newly created</b> (won't process data newly added into old files) file in the directory

> there is a script `splitAndSend.sh` in the git repo for trigger 500000 new file added event for testing

#### Creating a DStream object

~~~scala
// filestream is an instance of class DStream (stands for "discretized stream"), representing a sequence of RDDs, peirodically created from the input stream
val filestream = ssc.textFileStream("/home/spark/ch06input")
~~~

### 6.1.4. Using discretized streams

#### Parsing the lines

`Order` class for hold order data

~~~scala
import java.sql.Timestamp
case class Order(
	time: java.sql.Timestamp, 
	orderId:Long,
	clientId:Long, 
	symbol:String, 
	amount:Int, 
	price:Double, 
	buy:Boolean)
~~~

parse the lines from the filestream `DStream` and thus obtain a new `DStream` containing Order objects

~~~scala
import java.text.SimpleDateFormat
val orders = filestream.flatMap( 	// use flatMap not map
	line => {
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
		val s = line.split(",")
		try {
			assert(s(6) == "B" || s(6) == "S")
			List(
				Order(
					new Timestamp(dateFormat.parse(s(0)).getTime()), //ts
					s(1).toLong, 		//	orderId
					s(2).toLong, 		//	clientId
					s(3), 				//	symbol
					s(4).toInt, 		//	amount
        			s(5).toDouble, 	// 	price
        			s(6) == "B"		// 	"B" or "S"
        		)
			)
    } catch {
        case e : Throwable => println("Wrong line format ("+e+"): "+line)
        List()
    }
})
~~~

> The reason to use `flatMap` and not `map` is for ignoring any lines that don’t match the format expected. </br>
> If the line can be parsed, the function returns a list with a single element and an empty list otherwise

#### Counting the numbers of buy and sell orders per second

use `PairDStreamFunctions` object, similar to RDDs, `DStream`s containing two-element tuples get automatically converted to `PairDStreamFunctions` objects.

> By this way, functions such as `combineByKey`, `reduceByKey`, `flatMapValues`, variouse `joins`, ... (function in CH04), become available on `DStream` Object</br>
> `RDDs` implicitly converted to instances of the `PairRDDFunctions` object if they contain two-element tuples

~~~scala
val numPerType = orders.
	map(o => (o.buy, 1L)).         // key: o.buy; value: 1L
	reduceByKey((c1, c2) => c1+c2) // no countByKey fucntion in Pair-DStreamFunctions
~~~

### 6.1.5. Saving the results to a file

~~~scala
// folder (has suffix): <prefix>-<time_in_milliseconds>.<suffix>
// folder (no  suffix): <prefix>-<time_in_milliseconds>
numPerType.
	repartition(1).  // to create only 1 part-xxxxx file per RDD folder
	saveAsTextFiles(
		"/home/spark/ch06output/output", 	// prefix
		"txt"									// suffix
)
~~~

output file can again be a local file or a HDFS file

> `DStream`'s `print(n)` is helpful for debugging

### 6.1.6. Starting and stopping the streaming computation

Start the streaming computation

~~~scala
ssc.start()
~~~

notice: 

> Although you can construct several StreamingContext instances using the same SparkContext object </br>
> you can’t start more than one StreamingContext in the same JVM at a time

the main thread will exist after the receiver threads started, if don't want main driver exist, add following code

~~~scala
ssc.awaitTermination()
~~~

another option is `awaitTerminationOrTimeout(<timeout in milliseconds>)`, return `true` if streaming computation completed, return `false` if timeout

#### Sending data to Spark Streaming locally

~~~shell
$ chmod +x first-edition/ch06/splitAndSend.sh
$ cd first-edition/ch06
$ ./splitAndSend.sh /home/spark/ch06input local
~~~

> This will start copying parts of the orders.txt file to the specified folder, and trigger the new file added events

#### Stopping the Spark Streaming context

~~~scala
ssc.stop(false) 
~~~

`falses tells the streaming context not to stop the Spark context (defaul behavior)

> You can’t restart a stopped streaming context, but you can reuse the existing Spark context to create a new streaming contex </br>
> Spark shell allows you to overwrite the previously used variable names, you can paste all the previous lines in your shell and run the whole application again (if you wish to do so)

#### Examining the generated output

~~~scala
val allCounts = sc.textFile("/home/spark/ch06output/output*.txt")
~~~

> read several text files in one go using asterisks (*)

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




