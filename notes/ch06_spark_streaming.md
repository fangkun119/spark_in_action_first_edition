[TOC]

# CH06 Spark Streaming

> Spark Streaming具有从Hadoop兼容的文件系统（例如HDFS、S3）以及分布式系统（例如Flume、Kafka、Twitter）读取数据的连接器（Connectors）。另外在第13章将会给出Spring Streaming应用于实时日志分析的例子

## 6.1 编写Spark Streaming应用程序

原理：使用mini-batches，即把输入数据流拆分为基于时间的小批量RDD，然后像往常一样使用其他Spark组件（例如Spark Core，Spark SQL，ML，GraphX等）进行处理

> <div align="left"><img src="https://raw.githubusercontent.com/kenfang119/pics/main/500_spark/spark_streaming_concept.jpg" width="600" /></div>
>
> 数据源：文件系统、socket、Kafka、Flume、Twitter、Amazon Kinesis等

### 6.1.1 Demo程序

> [本章demo](../ch06/)是一个券商交易仪表盘应用中的一个小功能，计算每秒买卖的订单数量，按买卖总金额计算前5名客户，以及过去一个小时内交易量最高的卖前5支股票
>
> 涉及到的Spark Streaming功能如下：
>
> * 从HDFS读取数据并写回到HDFS
> * 从Kafka读取数据

### 6.2.2 创建streaming context ongoing

> 可以启动本地VM中的Spark，也可以连接到独立或者在YARN、Mesos集群中部署的Spark（第10、11、12章介绍相关的集群部署），需要确保executors有多个CPU内核可用，因为Spark Streaming至少需要一个内核读取数据、另一个内核执行计算

代码例子（在本地集群上运行）

> ~~~scala
> import org.apache.spark._
> import org.apache.spark.streaming._
> val ssc = new StreamingContext(
> 					sc,        // sc是spark shell提供的SparkContext
> 					Seconds(5) // 每个mini-batch覆盖的时间跨度，也可以指定Milliseconds或Minutes等
> 				) 
> ~~~

上面的`StreamingContext`复用了当前会话的`SparkContext`，如果想要使用一个全新的SparkContext可以采用如下方法

> ~~~scala
> // 这段代码只能在一个独立的应用程序中使用，如果在spark shell中使用，会运行失败,
> // 因为无法再同一个JVM中实例化两个Spark Context
> val conf = new SparkConf().setMaster("local[4]").setAppName("App name")
> val ssc  = new StreamingContext(conf, Seconds(5))
> ~~~

### 6.1.3 创建离散流（DStream）读取文件数据

#### (1) 数据

> 500000笔交易数据，字段包括：时间戳、订单ID、客户端ID、股票代码、股数，价格，买入还是卖出
>
> ~~~bash
> $ cd first-edition/ch06
> $ tar xvfz orders.tar.gz
> ~~~

#### (2) `StreamingContext.textFileStream`

> 该方法监控Hadoop Client兼容目录（例如HDFS、S3、GlusterFS、本地目录、……） 并读取未来目录中出现的新文件（不会读取已有的文件）。
>
> 配套Demo中使用名为`splitAndSend.sh`的脚本来模拟文件新建过程
>
> ~~~scala
> // 创建DStream对象监控并读取HDFS目录中新添加的文件
> // 登录的用户需要有访问HDFS目录的权限
> // 如果监控本地文件夹，需要添加一个local参数
> val filestream = ssc.textFileStream("/home/spark/ch06input")
> ~~~
>
> 创建出来的DStream是惰性求值的，即只有在真实数据出现时，才会开始执行

### 6.1.4. 使用DStream（离散流、discretized streams）

> 与RDD类似，DStream提供了过滤、映射、规约（reduce)等方法、将DStream转化为其他的DStream

#### (1) 解析行：使用`DStream.flatMap`

首先创建用于保存数据的`Order`类

> ~~~scala
> import java.sql.Timestamp
> case class Order(
> 	time: java.sql.Timestamp, // 这个类型受到DataFrame类的支持
> 	orderId:Long,
> 	clientId:Long, 
> 	symbol:String, 
> 	amount:Int, 
> 	price:Double, 
> 	buy:Boolean)
> ~~~

接下来解析数据

> ~~~scala
> import java.text.SimpleDateFormat
> // flatMap: 将传入的函数作用在所有RDD的所有element上
> // * 使用flatMap而不是map的原因是，想忽略任何与期望格式不匹配的行
> // * 如果格式匹配，返回解析后的List
> // * 如果格式不匹配，返回空的List
> val orders = filestream.flatMap( 	// use flatMap not map
> 	line => {
> 		// Java的SimpleDateFormat类
> 		val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
> 		val s = line.split(",")
> 		try {
> 			assert(s(6) == "B" || s(6) == "S")
> 			List(
> 				Order(
> 					new Timestamp(dateFormat.parse(s(0)).getTime()), //ts
> 					s(1).toLong, 		//	orderId
> 					s(2).toLong, 		//	clientId
> 					s(3), 				//	symbol
> 					s(4).toInt, 		//	amount
> 					s(5).toDouble, 		// 	price
> 					s(6) == "B"			// 	"B" or "S"
> 				)
> 			)
> 		} catch {
> 			case e : Throwable => println("Wrong line format ("+e+"): "+line)
> 			List()
>     	}
> 	}
> )
> ~~~

#### (2) 计算买入卖出订单的数量：使用`PairDStreamFunctions`

DStream对象存储二元组时会隐式转换为`PairDStreamFunctions`对象

> 与RDD类似，RDD存储二元组时、会隐式转换为`PairRDDFunctions`对象

PairDStreamFunctions对象提供了例如`combineByKey`、`reduceByKey`、`flatMapValues`、各类`joins`（同第四章的介绍）等方法。计算买入和卖出订单数量的代码如下

> ~~~scala
> // 使用reduceByKey而不是countByKey是因为PairDStreamFunctions没提供该方法
> val numPerType = orders.
> 	map(o => (o.buy, 1L)).         // key: o.buy; value: 1L
> 	reduceByKey((c1, c2) => c1+c2) // no countByKey fucntion in Pair-DStreamFunctions
> ~~~

### 6.1.5. 保存计算结果到文件

> ~~~scala
> // 传入suffix参数时的存放目录：<prefix>-<time_in_milliseconds>.<suffix>
> // 没有suffix参数时的存放目录：<prefix>-<time_in_milliseconds>
> numPerType.
> 	// to create only 1 part-xxxxx file per RDD folder
> 	repartition(1).  
> 	saveAsTextFiles(
> 		"/home/spark/ch06output/output", 	// prefix
> 		"txt"								// suffix
> )
> ~~~
>
> 同样可以输出到本地文件系统或者HDFS上
>
> 使用`DStream`的`print(n)`方法可以有助于调试

### 6.1.6. 启动和停止流计算

启动流式计算

> ~~~scala
> // 调用StreamContext的start方法
> ssc.start()
> ~~~
>
> 注意：虽然可以使用同一个SparkContext对象构造多个StreamingContext，但是一个JVM一次只能启动一个StreamingContext

让主线程等待Receiver线程运行完毕

> ~~~scala
> // 不指定超时时间一直等待 
> ssc.awaitTermination()
> // 指定超时时间，单位是毫秒
> ssc.awaitTerminationOrTimeout(timeOutMS)
> ~~~
>
> 如果不调用上面的方法，Receiver线程创建后，主线程会立即退出

调用模拟脚本，来定时向向数据目录中放入新文件

> ~~~bash
> $ chmod +x first-edition/ch06/splitAndSend.sh
> $ cd first-edition/ch06
> $ ./splitAndSend.sh /home/spark/ch06input local
> ~~~
>
> 之后就可以观察上述代码处理新增文件中的数据

#### 停止Spark Streaming Context

> 所有文件处理完毕后，可以关闭Spark Streaming Context
>
> ~~~scala
> // 参数false表示，只停止Spark Streaming Context，不要停止Spark Context
> // 因为demo中的代码都是在Spark Shell中执行，还希望保留Spark Context以进行其他实验
> ssc.stop(false)
> ~~~

#### 检查Spark Streaming的输出

> ~~~scala
> val allCounts = sc.textFile("/home/spark/ch06output/output*.txt")
> ~~~
>
> 路径中使用了通配符`*` 

### 6.1.7. 随着时间推移保存计算状态

> 之前小节的例子，只需要当前mini-batch的数据。这一小节演示如下一种应用：(1) 根据每个mini-batch更新state；(2) 根据`<state, current_mini_batch>`生成新的输出
>
> <div align="left"><img src="https://raw.githubusercontent.com/kenfang119/pics/main/500_spark/spark_streaming_with_state.jpg" width="500" /></div>

#### (1) 使用`updateStateByKey`来跟踪状态变化

> 

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

## 6.2. 使用外部数据源

### 6.2.1. 设置Kafka

### 6.2.2. 让Spark Streaming使用Kafka

#### 使用Spark Kafka Connector

#### 向Kafka写入messages

#### 例子

## 6.3 Spark Streaming job性能

### 6.3.1 性能调优

#### 降低处理时间

#### 增加并行度

#### 限制输入频次

### 6.3.2 提高容错能力（fault-tolerance）

#### 从executor failures中恢复

#### 从driver failures中恢复

## 6.4 结构化数据流（Structured Streaming）

### 6.4.1 创建Streaming DataFrame

### 6.4.2 输出Streaming数据

### 6.4.3 检查Streaming Executors

### 6.4.4 Future direction of structured streaming

## 6.5. Summary



