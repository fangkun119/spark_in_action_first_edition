# Ch 02. Spark fundamentals

> code: https://github.com/fangkun119/spark_in_action_first_edition/blob/master/ch02/scala/ch02-listings.scala

## 2.1. Using the spark-in-action VM

<b>start vm</b>

change to the folder where you put vagrant file

~~~shell
$ vagrant up
$ vagrant ssh # or other ssh client
$ git clone https://github.com/spark-in-action/first-edition
~~~

<b>java</b>

~~~shell
$ which java
/usr/bin/java
$ ls -la /usr/bin/java
lrwxrwxrwx 1 root root 22 Apr 19 18:36 /usr/bin/java -> /etc/alternatives/java
$ ls -la /etc/alternatives/java
lrwxrwxrwx 1 root root 46 Apr 19 18:36 /etc/alternatives/java -> /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
$ echo $JAVA_HOME
/usr/lib/jvm/java-8-openjdk-amd64/jre
~~~

<b>hadoop</b>

> reference: http://mng.bz/Y9FP

~~~shell
$ hadoop fs -ls /user
Found 1 items
drwxr-xr-x   - spark supergroup          0 2016-04-19 18:49 /user/spark
~~~

<b>hdfs-deamon</b>

~~~shell
$ /usr/local/hadoop/sbin/start-dfs.sh
$ /usr/local/hadoop/sbin/stop-dfs.sh
~~~

<b>spark</b>

download spark archive from https://spark.apache.org/downloads.html and unpack it to any folder. </br>
In the spark-in-action VM, similar to Hadoop, spark is available from the /usr/local/spark folder, which is a symlink pointing to /opt/spark-2.0.0-bin-hadoop2.7 </br>
for manually building spark, need to follow appendix B </br>

<b>manage spark versions: </b>

~~~shell
$ ls /opt | grep spark
spark-1.3.0-bin-hadoop2.4
spark-1.3.1-bin-hadoop2.4
spark-1.4.0-bin-hadoop2.4
spark-1.5.0-bin-hadoop2.4
spark-1.6.1-bin-hadoop2.6
spark-2.0.0-bin-hadoop2.7
$ sudo rm -f /usr/local/spark
$ sudo ln -s /opt/spark-1.6.1-bin-hadoop2.4 /usr/local/spark
~~~

<b>SPARK_HOME</b>

~~~shell
$ export | grep SPARK
declare -x SPARK_HOME="/usr/local/spark"
~~~

## 2.2. Using Spark shell and writing your first Spark program

### 2.2.1 Starting the Spark shell

<b>Spark Shell: </b>

> spark REPL (read-eval-print loop)

<b>start spark shell (scalar)</b>

~~~python
$ spark-shell
Spark context Web UI available at http://10.0.2.15:4040
…
Type :help for more information.
scala>
~~~

<b>quit spark shell</b>

~~~shell
scala> :quit   # or pressing Ctrl-D
~~~

<b>start pyspark shell (python)</b>

~~~shell
$pyspark
~~~

<b>log for troubleshooting</b>

~~~shell
$ ls ${SPARK_HOME}/logs/info.log               # log file
$ cat /usr/local/spark/conf/log4j.properties   # log configuration (also can use nano to edit the file, Ctrl + X to quit)

# set global logging severity to INFO (and upwards: WARN, ERROR, FATAL)
log4j.rootCategory=INFO, console, file

# console config (restrict only to ERROR and FATAL)
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.threshold=ERROR
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss}
 %p %c{1}: %m%n

# file configlog4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=logs/info.log
log4j.appender.file.MaxFileSize=5MB
log4j.appender.file.MaxBackupIndex=10
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss}
 %p %c{1}: %m%n

# Settings to quiet third party logs that are too verbose
log4j.logger.org.eclipse.jetty=WARN
log4j.logger.org.eclipse.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.spark=WARN
log4j.logger.org.apache.hadoop=WARN
~~~

### 2.2.2 The first Spark code example

<b>count of lines</b>

~~~shell
scala> val licLines = sc.textFile("/usr/local/spark/LICENSE")
licLines: org.apache.spark.rdd.RDD[String] = LICENSE MapPartitionsRDD[1] at textFile at <console>:27
scala> val lineCnt = licLines.count
lineCnt: Long = 294
~~~

<b>count of lines contains "BSD"</b>

~~~shell
scala> val bsdLines = licLines.filter(line => line.contains("BSD"))
bsdLines: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at filter at <console>:23
scala> bsdLines.count
res0: Long = 34
~~~
￼
> `line => line.contains("BSD")`<br/>
> fat-arrow function you use for filtering lines is anonymous

匿名函数：`line => line.contains("BSD")`<br/>
使用匿名函数：`val bsdLines = licLines.filter(line => line.contains("BSD"))`

定义非匿名函数：

~~~shell
scala > def isBSD(line: String) = { line.contains("BSD") } 
isBSD: (line: String)Boolean
~~~

将某个函数定义存储在一个变量中：

~~~shell
scala > val isBSD = (line: String) => line.contains("BSD")
isBSD: String => Boolean = <function1>
~~~

使用上面定义的函数

~~~shell
scala > val bsdLines1 = licLines.filter(isBSD)
bsdLines1: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[5] at filter at <console>:25
scala > bsdLines1.count
res1: Long = 34

scala> bsdLines.foreach(bLine => println(bLine))
BSD-style licenses
The following components are provided under a BSD-style license. See
 project link for details.
     (BSD 3 Clause) netlib core (com.github.fommil.netlib:core:1.1.2 -
 https://github.com/fommil/netlib-java/core)
     (BSD 3 Clause) JPMML-Model (org.jpmml:pmml-model:1.1.15 -
 https://github.com/jpmml/jpmml-model)
     (BSD 3-clause style license) jblas (org.jblas:jblas:1.2.4 -
 http://jblas.org/)
     (BSD License) AntLR Parser Generator (antlr:antlr:2.7.7 -
 http://www.antlr.org/)
...

# alternatively
scala> bsdLines.foreach(println)
~~~

### 2.2.3 The notion of a resilient distributed dataset (RDD)

RDD (Resilient Distributed Dataset, like `licLines` and `bsdLines` above) represents a collection that is:

* Immutable (read-only): RDD transformations always yield a new RDD instance
* Resilient (fault-tolerant again node failure by logging transformations and re-compute)
* Distributed 

> use RDD like local collection, but sometimes need care about partitioning and persistence options 

## 2.3. Basic RDD actions and transformations

2 types of RDD operations: `transformations` and `actions`

* <b>transformations</b>(e.g.: `filter` or `map`): manipulate input RDD and produce a new RDD as output
* <b>actions</b>(e.g.: `count` or `foreach`): trigger program invoking or calculation on a RDD

> `transformations` is a lazy execution based on the `graph of operations`<br/>
> RDD operation examples: `map`,`flatMap`,`take`,`distinct`,`filter`

### 2.3.1. map transformation

`filter`: conditionally remove (in the returned new collection)<br/>
`take`: conditionally take (in the returned new collection)<br/>
`map`: apply func to all element 

~~~scala
class RDD[T] {
  // ... other methods ...
  def map[U](f: (T) => U): RDD[U]
  // ... other methods ...
}
~~~

example 1 of `RDD.map()`

~~~shell
scala> val numbers = sc.parallelize(10 to 30 by 10)
numbers: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[2] at
 parallelize at <console>:12
scala> numbers.foreach(x => println(x))
30
10
20
scala> val numbersSquared = numbers.map(num => num * num)
numbersSquared: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[7] at map
 at <console>:23
scala> numbersSquared.foreach(x => println(x))
100
400
900
~~~

`sc`: return the Spark Context
`parallelize`: return RDD distributed in spark executors from a Seq 
`makeRDD`: alias of `parallelize`
`10 to 30 by 10`: scala way of creating a `Range`

example 2 of `RDD.map()`

~~~shell
scala> val reversed = numbersSquared.map(x => x.toString.reverse)
reversed: org.apache.spark.rdd.RDD[String] = MappedRDD[4] at map at
 <console>:16
scala> reversed.foreach(x => println(x))
001
004
009
~~~

example 2 of `RDD.map()` with placeholder

~~~shell
scala> val alsoReversed = numbersSquared.map(_.toString.reverse)
alsoReversed: org.apache.spark.rdd.RDD[String] = MappedRDD[4] at map at <console>:16
scala> alsoReversed.first
res6: String = 001
scala> alsoReversed.top(2)
res7: Array[String] = Array(009, 004)
~~~

> placeholder: <br/> [http://mng.bz/c52S](http://mng.bz/c52S)
> e.g.: `_.toString.reverse` is identical with `x => x.toString.reverse`<br/>

### 2.3.2 Distinct and flatMap transformations

> RDD operation `distinct` and `flatMap` (RDD.distinct, RRR.flatMap) is similer with the same name function on Scala collection (e.g. `Array`), but work on distributed data (RDD) and are lazily evaluated

example: unique user count of a week<br/>
data file: 

~~~shell
$ # 1 line per day, format is ${user_id}, ${user_id}, ... , ${user_id}\n
$ echo "15,16,20,20
77,80,94
94,98,16,31
31,15,20" > ~/client-ids.log
~~~

code in spark shell

~~~shell 
scala> val lines = sc.textFile("/home/spark/client-ids.log") //4 string for 4 lines
lines: org.apache.spark.rdd.RDD[String] = client-ids.log
 MapPartitionsRDD[1] at textFile at <console>:21

scala> val idsStr = lines.map(line => line.split(","))  //4 array for 4 lines
idsStr: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at
 map at <console>:14

scala> idsStr.foreach(println(_)) //println(cur_array.toString())
[Ljava.lang.String;@65d795f9
[Ljava.lang.String;@2cb742ab
... 4 of these ...

scala> idsStr.first  //idsStr[0].toString()
res0: Array[String] = Array(15, 16, 20, 20)

scala> idsStr.collect
res1: Array[Array[String]] = Array(Array(15, 16, 20, 20), Array(77, 80,
 94), Array(94, 98, 16, 31), Array(31, 15, 20))
 
scala> val ids = lines.flatMap(_.split(",")) //flatten(concanate array) then map
ids: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[8] at flatMap at
 <console>:23
 
scala> ids.collect //put all RDD element into an array
res11: Array[String] = Array(15, 16, 20, 20, 77, 80, 94, 94, 98, 16, 31,
 31, 15, 20)

scala> ids.first //the element is Str not an array, because of flatMap
res12: String = 15

scala> ids.collect.mkString("; ") //Array.mkString(seperater)
res13: String = 15; 16; 20; 20; 77; 80; 94; 94; 98; 16; 31; 31; 15; 20

scala> val intIds = ids.map(_.toInt) //transform elem from Str to int
intIds: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[9] at map at
 <console>:25
scala> intIds.collect
res14: Array[Int] = Array(15, 16, 20, 20, 77, 80, 94, 94, 98, 16, 31, 31,
 15, 20)

scala> val uniqueIds = intIds.distinct //unique elements
uniqueIds: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[12] at distinct
 at <console>:27
scala> uniqueIds.collect
res15: Array[Int] = Array(16, 80, 98, 20, 94, 15, 77, 31)
scala> val finalCount = uniqueIds.count
finalCount: Long = 8

scala> val transactionCount = ids.count //unique count
transactionCount: Long = 14

~~~

<b>`RDD.collect`</b>: is an action that creates an array and then collect all elements of an RDD into it<br/>
<b>`RDD.flatMap`</b>: basically works the same as `map`, but concatenates multiple arrays into a collection (1 nesting level less than received)<br/>
<b>`RDD.distinct`</b>: returns a new RDD with duplicate elements removed

~~~scala
def flatMap[U](f: (T) => TraversableOnce[U]): RDD[U]
	...
def distinct(): RDD[T]
	...
~~~

> `TraversableOnce`: [http://mng.bz/OTvD](http://mng.bz/OTvD)

### 2.3.3 sample, take, takeSample

<b>`RDD.sample(withReplacement, percentage, seed)`</b>: create a new RDD with `${pecentage}*100%*${RDD.count}` random elements from the calling RDD<br/>
It's a operation, return a RDD. <br/>

~~~scala
// withReplacement: true will allow 1 element being sampled multiple times
def sample(withReplacement: Boolean, fraction: Double, seed: Long =
 Utils.random.nextLong): RDD[T]
~~~

~~~shell
scala> val s = uniqueIds.sample(false, 0.3) //withReplacement = false
s: org.apache.spark.rdd.RDD[String] = PartitionwiseSampledRDD[19] at sample
 at <console>:27
scala> s.count
res19: Long = 2
scala> s.collect
res20: Array[String] = Array(94, 21)

scala> val swr = uniqueIds.sample(true, 0.5) //withReplacement = true
swr: org.apache.spark.rdd.RDD[String] = PartitionwiseSampledRDD[20] at
 sample at <console>:27
scala> swr.count
res21: Long = 5
scala> swr.collect
res22: Array[String] = Array(16, 80, 80, 20, 94)
~~~

<b>`RDD.takeSample(withReplacement, num, seed)`</b>: create a new RDD with `${num}` random elements from the calling RDD<br/>
It's a action, return a array rather than a RDD. <br/>

~~~scala
def takeSample(withReplacement: Boolean, num: Int, seed: Long =
 Utils.random.nextLong): Array[T]
~~~

~~~shell
scala> val taken = uniqueIds.takeSample(false, 5)
taken: Array[String] = Array(80, 98, 77, 31, 15)
~~~

<b>`RDD.take(num)`</b>: a simple function to peek the data sample, it won't scans all partition thus has is very convient.

~~~shell
scala> uniqueIds.take(3)
res23: Array[String] = Array(80, 20, 15)
~~~

## 2.4. functions for RDD\<double\>

### 2.4.0 Scala's implicit conversion

Functions for RDD only contain elements with type Double, this is implemented by  Scala's implicit conversion<br/>
<b>Scala Implicit Conversion</b><br/>

~~~scala
class ClassOne[T](val input: T) { }
class ClassOneStr(val one: ClassOne[String]) {
    def duplicatedString() = one.input + one.input
}
class ClassOneInt(val one: ClassOne[Int]) {
    def duplicatedInt() = one.input.toString + one.input.toString
}
implicit def toStrMethods(one: ClassOne[String]) = new ClassOneStr(one)
implicit def toIntMethods(one: ClassOne[Int]) = new ClassOneInt(one)
~~~

if `input` above is a String, `ClassOne` has a function `duplicatedString()` <br/>
if `input` above is a Int, `classOne` has a function `duplicatedInt()`<br/>
example: <br/>

~~~shell
scala> val oneStrTest = new ClassOne("test")
oneStrTest: ClassOne[String] = ClassOne@516a4aef

scala> val oneIntTest = new ClassOne(123)
oneIntTest: ClassOne[Int] = ClassOne@f8caa36

scala> oneIntTest.duplicatedString()
      error: value duplicatedString is not a member of ClassOne[Int]
              oneIntTest.duplicatedString()

scala> oneStrTest.duplicatedString()
res0: String = testtest

scala> oneIntTest.duplicatedInt()
res1: 123123
~~~

RDDs containing only `Double` objects are automatically converted into instances  of `org.apache.spark.rdd.DoubleRDDFunctions` which contains all the double RDD functions described in this sections

### 2.4.1 Basic statistics with functions of RDD\<Double/>

Because `Int` objects can automatically convert to `Double`, `DoubleRDDFunctions` functions also can be implicity applied to RDD\<Int\><br/>

~~~shell
scala> intIds.mean
res0: Double = 44.785714285714285
scala> intIds.sum
res1: Double = 627.0
scala> intIds.variance
res2: Double = 1114.8826530612246
scala> intIds.stdev
res3: Double = 33.38985853610681
~~~

### 2.4.2 Visualizing data distribution with histograms

Histograms: X axis has value intervals, Y axix has data density or number<br/>

`DoubleRDDFunctions.histogram(val intervalLimits : Array)` (version 1)<br/>

* parameter: Array to represent interval limits
* return: elements counts of each interval 

~~~shell
scala> intIds.histogram(Array(1.0, 50.0, 100.0))
res4: Array[Long] = Array(9, 5)
~~~

`DoubleRDDFunctions.histogram(val intervalNum : Int)` (version 2)<br/>

* parameter: interval number
* return: tuple of <Array for interval limits calculated, Array for elements counts of each interval>

~~~shell
scala> intIds.histogram(3)
res5: (Array[Double], Array[Long]) = (Array(15.0, 42.66666666666667,
 70.33333333333334, 98.0),Array(9, 0, 5))
~~~ 

### 2.4.3 Approximate sum and mean 

won't visit all elements to save time

~~~scala
sumApprox(timeout: Long, confidence: Double = 0.95):
    PartialResult[BoundedDouble]
meanApprox(timeout: Long, confidence: Double = 0.95):
    PartialResult[BoundedDouble]
~~~    

<b>parameter</b>: `timeout` is in milliseconds, the function will stop and return the latest estimation when timeout</br>
<b>return</b>: Approximate actions return a `PartialResult` object with `finalValue` (a probable range, i.e. low and high, and associated confidence) and `failure` field (available only when exception)

## 2.5. Summary


