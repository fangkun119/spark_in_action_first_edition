# CHO5 Spark SQL

* 5.1. Working with DataFrames
* 5.2. Beyond DataFrames: introducing DataSets
* 5.3. Using SQL commands
* 5.4. Saving and loading DataFrame data
* 5.5. Catalyst optimizer
* 5.6. Performance improvements with Tungsten
* 5.7. Summary

Documents: </br>
[http://spark.apache.org/docs/latest/sql-programming-guide.html](http://spark.apache.org/docs/latest/sql-programming-guide.html) </br>
[http://spark.apache.org/examples.html](http://spark.apache.org/examples.html)</br>
[http://spark.apache.org/sql/](http://spark.apache.org/sql/)</br>
[https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples](https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples)</br>


## 5.1. Working with DataFrames

`DataFrame` is for structured data (distributed data in a table-like representation) with named columns and declared column types 

`RDD` is the low-level representation

> Since Spark 2.0, DataFrame is implemented as a special case of `DataSet`

`Spark SQL`: register DataFrames as tables to let applications query with data frame `names`

3 ways to create DataFrame 

	(1) creating Dataframes from RDDs 
	(2) running SQL 
	(3) loading external data</br>

> `DataFrame` 用于封装结构化数据，`RDD`是其底层数据结构（Spark 2.0之后改为使用`DataSet`作为`DataFrame`底层数据结构）。通过`Spark SQL`可以将`DataFrame`注册成一张表，并且在表名可以用在SQL中。有3种方式可以创建`DataFrame`：（1）用`RDD`创建（2）用SQL创建（3）从外部数据导入


### 5.1.1 creating Dataframes from RDDs 

Data Set: italianPosts.csv delimited with "~" (tilde signs)) （数据集）

> `commentCount` — Number of comments related to the question/answer</br>
> `lastActivityDate` — Date and time of the last modification</br>
> `ownerUserId` — User ID of the owner</br>
> `body`—Textual contents of the question/answer</br>
> `score`—Total score based on upvotes and downvotes</br>
> `creationDate` — Date and time of creation</br>
> `viewCount`—View count</br>
> `title`—Title of the question</br>
> `tags`—Set of tags the question has been marked with</br>
> `answerCount`—Number of related answers</br>
> `acceptedAnswerId`—If a question contains the ID of its accepted answer</br>
> `postTypeId`—Type of the post; 1 is for questions, 2 for answers</br>
> `id`—Post’s unique ID</br>

3 Approaches:
> 1. using `RDDs` containing row data as tuples (can not specify all the schema attributes) </br>
> 2. Converting RDDs to DataFrames using `case` classes</br>
> 3. specifying a schema (standard approach in Spark) </br>

#### Approach 1: using `RDDs` containing row data as tuples

example is as below, including: 

> 1. `SparkSession` and `implicit` methods</br>
> 2. load data set into RDD</br>
> 3. convert each Array[String] to a tuple</br>
> 4. option 1: simply invoke `toDF` to convert tuples to DataFrame</br>
> 5. option 2: assign column names when invoking `toDF`</br>

limitation: `toDF()` conversion is that all columns can only be `String` and `nullable`

scala example: 

~~~scala
// 1. `SparkSession` and `implicit` methods
// wrapper around SparkContext and SQLContext
import org.apache.spark.sql.SparkSession 
// for specifying `master`,`appName`, ...
val spark = SparkSession.builder().getOrElse() 
// for automatically type converting
// implicits._ add method toDF to RDD if the RDD contains objects with DataSet Encoder defined
// Encoders convert JVM object to Spark SQL representations 
// Encoder list: http://mng.bz/Wa45
import spark.implicits._ 

// 2. load data set into RDD
val itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv")
val itPostsSplit = itPostsRows.map(x => x.split("~")) 
//  itPostsSplit: org.apache.spark.rdd.RDD[Array[String]]

// 3. convert each Array[String] to a tuple
val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
//  itPostsRDD: org.apache.spark.rdd.RDD[(String, String, ...
//  there’s no elegant way to convert an array to a tuple, so you have to resort to this ugly expression

// 4. option 1: use `toDF` function convert the tuples to DataFrame
// all columns can only be `String` and `nullable`
val itPostsDFrame = itPostsRDD.toDF() // org.apache.spark.sql.DataFrame
itPostsDFrame.show(3) //default 20
// +---+--------------------+---+--------------------+---+--------------------
// | _1|                  _2| _3|                  _4| _5|                  _6
// +---+--------------------+---+--------------------+---+--------------------
// |  4|2013-11-11 18:21:...| 17|&lt;p&gt;The infi...| 23|2013-11-10 19:37:...
// |  5|2013-11-10 20:31:...| 12|&lt;p&gt;Come cre...|  1|2013-11-10 19:44:...
// |  2|2013-11-10 20:31:...| 17|&lt;p&gt;Il verbo...|  5|2013-11-10 19:58:...
// +---+--------------------+---+--------------------+---+--------------------

// 5. option 2: assign columns names when invoking `toDF()` 
// all columns can only be `String` and `nullable`
val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
itPostsDF.printSchema
// root
//  |-- commentCount: string (nullable = true)
//  |-- lastActivityDate: string (nullable = true)
//  |-- ownerUserId: string (nullable = true)
//  |-- body: string (nullable = true)
//  |-- score: string (nullable = true)
//  |-- creationDate: string (nullable = true)
//  |-- viewCount: string (nullable = true)
//  |-- title: string (nullable = true)
//  |-- tags: string (nullable = true)
//  |-- answerCount: string (nullable = true)
//  |-- acceptedAnswerId: string (nullable = true)
//  |-- postTypeId: string (nullable = true)
//  |-- id: string (nullable = true)
~~~

pyspark example: 

~~~python
from __future__ import print_function

# 1. `SparkSession` and `implicit` methods
# sc is SparkSession, this variable is initialized when starting PySpark shell, please refer to related docs or previouse chapter of this book

# 2. load data into RDD
itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv")
itPostsSplit = itPostsRows.map(lambda x: x.split("~"))

# 3. convert each list<String> to a tuple
itPostsRDD = itPostsSplit.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12]))

# 4. option 1: use `toDF` function convert the tuples to DataFrame
# all columns can only be `String` and `nullable`
itPostsDFrame = itPostsRDD.toDF()
itPostsDFrame.show(10)
# +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
# | _1|                  _2| _3|                  _4| _5|                  _6|  _7|                  _8|                  _9| _10| _11|_12| _13|
# +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
# |  4|2013-11-11 18:21:...| 17|&lt;p&gt;The infi...| 23|2013-11-10 19:37:...|null|                    |                    |null|null|  2|1165|
# |  5|2013-11-10 20:31:...| 12|&lt;p&gt;Come cre...|  1|2013-11-10 19:44:...|  61|Cosa sapreste dir...| &lt;word-choice&gt;|   1|null|  1|1166|
# |  2|2013-11-10 20:31:...| 17|&lt;p&gt;Il verbo...|  5|2013-11-10 19:58:...|null|                    |                    |null|null|  2|1167|
# |  1|2014-07-25 13:15:...|154|&lt;p&gt;As part ...| 11|2013-11-10 22:03:...| 187|Ironic constructi...|&lt;english-compa...|   4|1170|  1|1168|
# |  0|2013-11-10 22:15:...| 70|&lt;p&gt;&lt;em&g...|  3|2013-11-10 22:15:...|null|                    |                    |null|null|  2|1169|
# |  2|2013-11-10 22:17:...| 17|&lt;p&gt;There's ...|  8|2013-11-10 22:17:...|null|                    |                    |null|null|  2|1170|
# |  1|2013-11-11 09:51:...| 63|&lt;p&gt;As other...|  3|2013-11-11 09:51:...|null|                    |                    |null|null|  2|1171|
# |  1|2013-11-12 23:57:...| 63|&lt;p&gt;The expr...|  1|2013-11-11 10:09:...|null|                    |                    |null|null|  2|1172|
# |  9|2014-01-05 11:13:...| 63|&lt;p&gt;When I w...|  5|2013-11-11 10:28:...| 122|Is &quot;scancell...|&lt;usage&gt;&lt;...|   3|1181|  1|1173|
# |  0|2013-11-11 10:58:...| 18|&lt;p&gt;Wow, wha...|  5|2013-11-11 10:58:...|null|                    |                    |null|null|  2|1174|
# +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
# only showing top 10 rows

# 5. option 2: assign columns names when invoking `toDF()` 
# all columns can only be `String` and `nullable`
itPostsDF = itPostsRDD.toDF(["commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id"])

itPostsDF.printSchema()
# root
#  |-- commentCount: string (nullable = true)
#  |-- lastActivityDate: string (nullable = true)
#  |-- ownerUserId: string (nullable = true)
#  |-- body: string (nullable = true)
#  |-- score: string (nullable = true)
#  |-- creationDate: string (nullable = true)
#  |-- viewCount: string (nullable = true)
#  |-- title: string (nullable = true)
#  |-- tags: string (nullable = true)
#  |-- answerCount: string (nullable = true)
#  |-- acceptedAnswerId: string (nullable = true)
#  |-- postTypeId: string (nullable = true)
#  |-- id: string (nullable = true)
~~~

#### Approach 2: Converting RDDs to DataFrames using case classes

map each `row` to a `case class` and then use the `toDF` method<br/>
below is an example, including: 

> 1. `SparkSession` and `implicit` methods</br>
> 2. define a `case class` to specify the schema</br>
> 3. define an `implicit class` to make `RDD->case mapping` more elegantly, the idea is from [http://mng.bz/ih7n](http://mng.bz/ih7n)</br>
> 4. use the functions of the `implicit class StringImprovements` in `StringImplicits` to convert the RDD to `case object`</br>
> 5. now the `DataFrame` contains the proper types and `nullable` flags
itPostsDFCase.printSchema</br>

scala example:

~~~scala
// 1. SparkSession and implicit methods
// wrapper around SparkContext and SQLContext
import org.apache.spark.sql.SparkSession 
// for specifying `master`,`appName`, ...
val spark = SparkSession.builder().getOrElse() 
// for automatically type converting
// implicits._ add method toDF to RDD if the RDD contains objects with DataSet Encoder defined
// Encoders convert JVM object to Spark SQL representations 
// Encoder list: http://mng.bz/Wa45
import spark.implicits._ 

// 2. define a `case class` to specify the schema
import java.sql.Timestamp
case class Post (
    commentCount:Option[Int],
    lastActivityDate:Option[java.sql.Timestamp],
    ownerUserId:Option[Long], 
    body:String, score:Option[Int],
    creationDate:Option[java.sql.Timestamp],
    viewCount:Option[Int], 
    title:String, 
    tags:String, 
    answerCount:Option[Int],
    acceptedAnswerId:Option[Long], 
    postTypeId:Option[Long], 
    id:Long)

// 3. define an `implicit class` to make `RDD->case mapping` more elegantly
// "*Safe" means if a string can’t be converted to the type, instead of throwing an exception, the methods return `None`
// "catching" returns an object of type `scala.util.control.Exception.Catch`
// "opt" method (of the object returned by function "catching") execute the mapping with the specified function passed in (such as `s.toInt`, `s.toLong`, `Timestamp.valueOf(s)` above) or return `None` if exception occurs
object StringImplicits {
   implicit class StringImprovements(val s: String) {
      import scala.util.control.Exception.catching
      def toIntSafe  = catching(classOf[NumberFormatException]) opt s.toInt
      def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
      def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
   }
}

// 4. use the functions of the `implicit class StringImprovements` in `StringImplicits` to convert the RDD to `case object`
import StringImplicits._
def stringToPost(row:String):Post = {
  val r = row.split("~")
  Post(r(0).toIntSafe,
    r(1).toTimestampSafe,
    r(2).toLongSafe,
    r(3),
    r(4).toIntSafe,
    r(5).toTimestampSafe,
    r(6).toIntSafe,
    r(7),
    r(8),
    r(9).toIntSafe,
    r(10).toLongSafe,
    r(11).toLongSafe,
    r(12).toLong)
}
val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()

// 5. now the `DataFrame` contains the proper types and `nullable` flags
itPostsDFCase.printSchema
// root
//  |-- commentCount: integer (nullable = true)
//  |-- lastActivityDate: timestamp (nullable = true)
//  |-- ownerUserId: long (nullable = true)
//  |-- body: string (nullable = true)
//  |-- score: integer (nullable = true)
//  |-- creationDate: timestamp (nullable = true)
//  |-- viewCount: integer (nullable = true)
//  |-- title: string (nullable = true)
//  |-- tags: string (nullable = true)
//  |-- answerCount: integer (nullable = true)
//  |-- acceptedAnswerId: long (nullable = true)
//  |-- postTypeId: long (nullable = true)
//  |-- id: long (nullable = false) # nullable is false
~~~

> no pyspark example

#### Converting RDDs to DataFrames by specifying a schema

use `SparkSession`'s `createDataFrame` method, parameters includes `Row` (object in RDD) and `StructType` (represents a schema, contains 1~N `StructField`s for represent columns), this is the standard approach of Spark

code is as below, including: 

> 1. SparkSession and implicit methods</br>
> 2. define the schema</br>
> <b>Types used in schema</b>: `org.apache.spark.sql.types`</br>
> <b>simple types</b>: strings, integers, shorts, floats, doubles, bytes, dates, timestamps, and binary values </br>
> <b>complex types</b>: Arrays, Maps, Structs (with nested column definations)</br>
> 3. define function convert the `String`(a element/record) to `Row`(with schema)</br>
> 4. create an RDD and the DataFrame</br>
> 5. get schema information</br>

scala example:

~~~scala
// 1. SparkSession and implicit methods
// wrapper around SparkContext and SQLContext
import org.apache.spark.sql.SparkSession 
// for specifying `master`,`appName`, ...
val spark = SparkSession.builder().getOrElse() 
// for automatically type converting
// implicits._ add method toDF to RDD if the RDD contains objects with DataSet Encoder defined
// Encoders convert JVM object to Spark SQL representations 
// Encoder list: http://mng.bz/Wa45
import spark.implicits._ 

// 2. define the schema
import org.apache.spark.sql.types._
val postSchema = StructType(Seq(
    StructField("commentCount", IntegerType, true),
    StructField("lastActivityDate", TimestampType, true),
    StructField("ownerUserId", LongType, true),
    StructField("body", StringType, true),
    StructField("score", IntegerType, true),
    StructField("creationDate", TimestampType, true),
    StructField("viewCount", IntegerType, true),
    StructField("title", StringType, true),
    StructField("tags", StringType, true),
    StructField("answerCount", IntegerType, true),
    StructField("acceptedAnswerId", LongType, true),
    StructField("postTypeId", LongType, true),
    StructField("id", LongType, false))
)

// 3. define function convert the `String`(a element/record) to `Row`(with schema) 
import org.apache.spark.sql.Row
def stringToRow(row:String):Row = {
  val r = row.split("~")
  Row(r(0).toIntSafe.getOrElse(null),
    r(1).toTimestampSafe.getOrElse(null),
    r(2).toLongSafe.getOrElse(null),
    r(3),
    r(4).toIntSafe.getOrElse(null),
    r(5).toTimestampSafe.getOrElse(null),
    r(6).toIntSafe.getOrElse(null),
    r(7),
    r(8),
    r(9).toIntSafe.getOrElse(null),
    r(10).toLongSafe.getOrElse(null),
    r(11).toLongSafe.getOrElse(null),
    r(12).toLong)
}

// 4. create a RDD as input example
val rowRDD = itPostsRows.map(row => stringToRow(row))

// 5. convert RDD to DataFrame
val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)
itPostsDFStruct.columns
// 	res0: Array[String] = Array(
//		commentCount, lastActivityDate, ownerUserId, body, score, creationDate, viewCount, title, tags, answerCount, acceptedAnswerId, postTypeId, id)
itPostsDFStruct.dtypes
//	res1: Array[(String, String)] = Array(
//		(commentCount,IntegerType), (lastActivityDate,TimestampType), (ownerUserId,LongType), (body,StringType), (score,IntegerType), (creationDate,TimestampType), (viewCount,IntegerType), (title,StringType), (tags,StringType), (answerCount,IntegerType), (acceptedAnswerId,LongType), (postTypeId,LongType), (id,LongType))
~~~

pyspark example:

~~~python
from __future__ import print_function

# 1. `SparkSession` and `implicit` methods
# sc is SparkSession, this variable is initialized when starting PySpark shell, please refer to related docs or previouse chapter of this book

# 2. util functions for converting data types
from pyspark.sql import Row
from datetime import datetime
def toIntSafe(inval):
  try:
    return int(inval)
  except ValueError:
    return None

def toTimeSafe(inval):
  try:
    return datetime.strptime(inval, "%Y-%m-%d %H:%M:%S.%f")
  except ValueError:
    return None

def toLongSafe(inval):
  try:
    return long(inval)
  except ValueError:
    return None

# 3. function to convert type of fields in a row
def stringToPost(row):
  r = row.encode('utf8').split("~")
  return Row(
    toIntSafe(r[0]),
    toTimeSafe(r[1]),
    toIntSafe(r[2]),
    r[3],
    toIntSafe(r[4]),
    toTimeSafe(r[5]),
    toIntSafe(r[6]),
    toIntSafe(r[7]),
    r[8],
    toIntSafe(r[9]),
    toLongSafe(r[10]),
    toLongSafe(r[11]),
    long(r[12]))

# 4. define the DataFrame schema
from pyspark.sql.types import *
postSchema = StructType([
  StructField("commentCount", IntegerType(), True),
  StructField("lastActivityDate", TimestampType(), True),
  StructField("ownerUserId", LongType(), True),
  StructField("body", StringType(), True),
  StructField("score", IntegerType(), True),
  StructField("creationDate", TimestampType(), True),
  StructField("viewCount", IntegerType(), True),
  StructField("title", StringType(), True),
  StructField("tags", StringType(), True),
  StructField("answerCount", IntegerType(), True),
  StructField("acceptedAnswerId", LongType(), True),
  StructField("postTypeId", LongType(), True),
  StructField("id", LongType(), False)
  ])

# 5. create RDD as the input example
rowRDD = itPostsRows.map(lambda x: stringToPost(x))

# 6. convert the RDD into DataFrame
itPostsDFStruct = sqlContext.createDataFrame(rowRDD, postSchema)
itPostsDFStruct.printSchema()
itPostsDFStruct.columns
itPostsDFStruct.dtypes
~~~

### 5.1.2. DataFrame API basics

`DataFrame` works like RDD: `immutable` and `lazy`

code examples are listed below, including: 

> 1. select data of specified columns</br>
> 2. select data except some columns</br>
> 3. filter data with `where` or `filter` functions</br>
> (1) the variant taking a string for parsing SQL expressions will be introducted in section 5.2</br>
> (2) complete set of operators: [http://mng.bz/a2Xt](http://mng.bz/a2Xt)</br>
> 4. select only first `n` rows with `limit` function</br>
> 5. add new column with `withColumnRenamed` or `withColumn` function</br>
> 6. sorting data with `orderBy` or `sort` function

~~~scala
// 1. select data of specified columns
val postsDf = itPostsDFStruct
// select by column names
val postsIdBody = postsDf.select("id", "body") // org.apache.spark.sql.DataFrame = [id: bigint, body: string]
// select by Column object created with methods like DataFrame.col
val postsIdBody = postsDf.select(postsDf.col("id"), postsDf.col("body"))
// implicit methods imported (introducted in $5.1.1) to covert scala Symbol to Column.Symbol
val postsIdBody = postsDf.select(Symbol("id"), Symbol("body")) 
// scala’s built-in quote mechanism do the same thing
val postsIdBody = postsDf.select('id, 'body) 
// another implicit method called $ which convert String to ColumnName object
val postsIdBody = postsDf.select($"id", $"body")

// 2. select data except some columns
val postIds = postsIdBody.drop("body")

// 3. filter data with `where` or `filter` functions
// the variant taking a string for parsing SQL expressions will be introducted in section 5.2
// complete set of operators: http://mng.bz/a2Xt
postsIdBody.filter('body contains "Italiano").count // res0: Long = 46
val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))

// 4. select only first `n` rows with `limit` function  
val firstTenQs = postsDf.filter('postTypeId === 1).limit(10)

// 5. add new column with `withColumnRenamed` or `withColumn` function
val firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")
postsDf.filter('postTypeId === 1)
    .withColumn("ratio", 'viewCount / 'score) // column name is "ratio"
    .where('ratio < 35)
    .show()


// 6. sorting data with `orderBy` or `sort` function
// the function take 1 or more column names (or `Column` expressions)
// the `Column` class has `asc` and `desc` operators for specifiying sorting order (default is ascending)
postsDf.filter('postTypeId === 1)
    .orderBy('lastActivityDate desc)
    .limit(10)
    .show
~~~

pyspark example:

~~~python
from __future__ import print_function

# 1. select data of specified columns
postsDf = itPostsDFStruct
postsIdBody = postsDf.select("id", "body")

postsIdBody = postsDf.select(postsDf["id"], postsDf["body"])

# 2. select data except some columns
postIds = postsIdBody.drop("body")

# 3. filter data with `where` or `filter` functions
# the variant taking a string for parsing SQL expressions will be introducted in section 5.2
from pyspark.sql.functions import *
postsIdBody.filter(instr(postsIdBody["body"], "Italiano") > 0).count()

# 4. select only first `n` rows with `limit` function  
noAnswer = postsDf.filter((postsDf["postTypeId"] == 1) & isnull(postsDf["acceptedAnswerId"]))

# 5. add new column with `withColumnRenamed` or `withColumn` function
firstTenQs = postsDf.filter(postsDf["postTypeId"] == 1).limit(10)
firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")

# 6. sorting data with `orderBy` or `sort` function
# the function take 1 or more column names (or `Column` expressions)
# the `Column` class has `asc` and `desc` operators for specifiying sorting order (default is ascending)
postsDf.filter(postsDf.postTypeId == 1).withColumn("ratio", postsDf.viewCount / postsDf.score).where("ratio < 35").show()
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
#|commentCount|    lastActivityDate|ownerUserId|                body|score|        creationDate|viewCount|title|                tags|answerCount|acceptedAnswerId|postTypeId|  id|              ratio|
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
#|           5|2013-11-21 14:04:...|          8|&lt;p&gt;The use ...|   13|2013-11-11 21:01:...|      142| null|&lt;prepositions&...|          2|            1212|         1|1192| 10.923076923076923|
#|           0|2013-11-12 09:26:...|         17|&lt;p&gt;When wri...|    5|2013-11-11 21:01:...|       70| null|&lt;punctuation&g...|          4|            1195|         1|1193|               14.0|
#|           1|2013-11-12 12:53:...|         99|&lt;p&gt;I can't ...|   -3|2013-11-12 10:57:...|       68| null|&lt;grammar&gt;&l...|          3|            1216|         1|1203|-22.666666666666668|
#|           3|2014-09-11 14:37:...|         63|&lt;p&gt;The plur...|    5|2013-11-12 13:34:...|       59| null|&lt;plural&gt;&lt...|          1|            1227|         1|1221|               11.8|
#|           1|2013-11-12 13:49:...|         63|&lt;p&gt;I rememb...|    6|2013-11-12 13:38:...|       53| null|&lt;usage&gt;&lt;...|          1|            1223|         1|1222|  8.833333333333334|
#|           5|2013-11-13 00:32:...|        159|&lt;p&gt;Girando ...|    6|2013-11-12 23:50:...|       88| null|&lt;grammar&gt;&l...|          1|            1247|         1|1246| 14.666666666666666|
#|           0|2013-11-14 00:54:...|        159|&lt;p&gt;Mi A� ca...|    7|2013-11-14 00:19:...|       70| null|       &lt;verbs&gt;|          1|            null|         1|1258|               10.0|
#|           1|2013-11-15 12:17:...|         18|&lt;p&gt;Clearly ...|    7|2013-11-14 01:21:...|       68| null|&lt;grammar&gt;&l...|          2|            null|         1|1262|  9.714285714285714|
#|           0|2013-11-14 21:14:...|         79|&lt;p&gt;Alle ele...|    8|2013-11-14 20:16:...|       96| null|&lt;grammar&gt;&l...|          1|            1271|         1|1270|               12.0|
#|           0|2013-11-15 17:12:...|         63|&lt;p&gt;In Itali...|    8|2013-11-15 14:54:...|       68| null|&lt;usage&gt;&lt;...|          1|            1277|         1|1275|                8.5|
#|           3|2013-11-19 18:08:...|          8|&lt;p&gt;The Ital...|    6|2013-11-15 16:09:...|       87| null|&lt;grammar&gt;&l...|          1|            null|         1|1276|               14.5|
#|           1|2014-08-14 13:13:...|         12|&lt;p&gt;When I s...|    5|2013-11-16 09:36:...|       74| null|&lt;regional&gt;&...|          3|            null|         1|1279|               14.8|
#|          10|2014-03-15 08:25:...|        176|&lt;p&gt;In Engli...|   12|2013-11-16 11:13:...|      148| null|&lt;punctuation&g...|          2|            1286|         1|1285| 12.333333333333334|
#|           2|2013-11-17 15:54:...|         79|&lt;p&gt;Al di fu...|    7|2013-11-16 13:16:...|       70| null|     &lt;accents&gt;|          2|            null|         1|1287|               10.0|
#|           1|2013-11-16 19:05:...|        176|&lt;p&gt;Often ti...|   12|2013-11-16 14:16:...|      106| null|&lt;verbs&gt;&lt;...|          1|            null|         1|1290|  8.833333333333334|
#|           4|2013-11-17 15:50:...|         22|&lt;p&gt;The verb...|    6|2013-11-17 14:30:...|       66| null|&lt;verbs&gt;&lt;...|          1|            null|         1|1298|               11.0|
#|           0|2014-09-12 10:55:...|          8|&lt;p&gt;Wikipedi...|   10|2013-11-20 16:42:...|      145| null|&lt;orthography&g...|          5|            1336|         1|1321|               14.5|
#|           2|2013-11-21 12:09:...|         22|&lt;p&gt;La parol...|    5|2013-11-20 20:48:...|       49| null|&lt;usage&gt;&lt;...|          1|            1338|         1|1324|                9.8|
#|           0|2013-11-22 13:34:...|        114|&lt;p&gt;There ar...|    7|2013-11-20 20:53:...|       69| null|   &lt;homograph&gt;|          2|            1330|         1|1325|  9.857142857142858|
#|           6|2013-11-26 19:12:...|         12|&lt;p&gt;Sento ch...|   -3|2013-11-21 21:12:...|       79| null|  &lt;word-usage&gt;|          2|            null|         1|1347|-26.333333333333332|
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
# only showing top 20 rows

#The 10 most recently modified questions:
postsDf.filter(postsDf.postTypeId == 1).orderBy(postsDf.lastActivityDate.desc()).limit(10).show()
~~~

### 5.1.3. Using SQL functions to perform calculations on data

4 types of SQL functions: <br/>

(1) `Scalar Functions` return a single value for each row based on calculations on one or more columns <br/>

> `abs`, `exp`, `substring`, `hypot`(hypotenuse, (直角三角形的) 斜边，弦), `log`, `cbrt`(cube root), `length`, `trim`, `concat`(concatenates several string), `year`, `date_add`, `datediff`... 

(2) `Aggregate Functions` return a single value for a group of rows<br/>

> `avg`, `min`, `max`, `count`, `sum`, ..., combined with `groupBy` (introduced in section 5.1.4)

(3) `Window functions` return several values for a group of rows<br/>

> Define a “moving group” of rows (called frames) based on some calculations with current row.  They don’t group rows into a single output row per group (different with ` aggregate functions`) 

(4) `User-defined functions` include custom scalar or aggregate functions

examples are as below, including:

> 1. example 1: scalar function</br>
> 2. example 2: aggregation function</br>
> 3. example 3: window function</br>
> 4. example 4: UDF, user defined function</br>

scala example:

~~~scala
// 1. example 1: scalar function 
// return a single value for each row based on calculations on one or more columns
import org.apache.spark.sql.functions._
postsDf.filter('postTypeId === 1)
	.withColumn( // add new culumn activePeriod
		"activePeriod", 
		datediff('lastActivityDate, 'creationDate)) 
	.orderBy('activePeriod desc) // orderby activePeriod desc
	.head 
	.getString(3)
	.replace("&lt;","<")
	.replace("&gt;",">")
//res0: String = <p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>

// 2. example 2: window function
// return a single value for a group of rows, also can be used with with `groupBy` (introduced in section 5.1.4)
import org.apache.spark.sql.functions._
postsDf.select(avg('score), max('score), count('score)).show
// +-----------------+----------+------------+
// |       avg(score)|max(score)|count(score)|
// +-----------------+----------+------------+
// |4.159397303727201|        24|        1261|
// +-----------------+----------+------------+

// 3. example 3: window function
// define a “moving group” of rows (called frames) based on some calculations with current row.  They don’t group rows into a single output row per group (different with ` aggregate functions`) 
import org.apache.spark.sql.expressions.Window
postsDf.filter('postTypeId === 1)
    .select(
        'ownerUserId, 
        'acceptedAnswerId, 
        'score,
        // use window function to aggregate
        max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser")
    .withColumn("toMax", 'maxPerUser - 'score) //add new column
    .show(10)
// +-----------+----------------+-----+----------+-----+
// |ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
// +-----------+----------------+-----+----------+-----+
// |        232|            2185|    6|         6|    0|
// |        833|            2277|    4|         4|    0|
// |        833|            null|    1|         4|    3|
// |        235|            2004|   10|        10|    0|
// |        835|            2280|    3|         3|    0|
// |         37|            null|    4|        13|    9|
// |         37|            null|   13|        13|    0|
// |         37|            2313|    8|        13|    5|
// |         37|              20|   13|        13|    0|
// |         37|            null|    4|        13|    9|
// +-----------+----------------+-----+----------+-----+

postsDf.filter('postTypeId === 1)
    .select(
        'ownerUserId, 
        'id, 
        'creationDate, 
        // use window function to aggregate
        lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev", 
        // use window function to aggregate
        lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next")
    .orderBy('ownerUserId, 'id)
    .show()

// 4. example: UDF, user defined function
// background: question tags are stored in the format of '&lt;tag_name&gt;', we want to know how many tags each question has
// step 1: register udf
// (1) approach 1: create udf with `udf` function
val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).length) // org.apache.spark.sql.UserDefinedFunction
// (2) appraoch 2: using `SparkSession.udf.register` function, and UDF will get a name which can also used in SQL expressions (section $5.2)
val countTags = spark.udf.register("countTags", (tags: String) => "&lt;".r.findAllMatchIn(tags).length) 
// step 2: use udf name `countTags` in SQL expressions
postsDf.filter('postTypeId === 1)
    .select('tags, countTags('tags) as "tagCnt")
    .show(10, false)
// +------------------------------------------------------------------+------+
// |tags                                                              |tagCnt|
// +------------------------------------------------------------------+------+
// |&lt;word-choice&gt;                                               |1     |
// |&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt|3     |
// |&lt;usage&gt;&lt;verbs&gt;                                        |2     |
// |&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;             |3     |
// |&lt;usage&gt;&lt;punctuation&gt;                                  |2     |
// |&lt;usage&gt;&lt;tenses&gt;                                       |2     |
// |&lt;history&gt;&lt;english-comparison&gt;                         |2     |
// |&lt;idioms&gt;&lt;etymology&gt;                                   |2     |
// |&lt;idioms&gt;&lt;regional&gt;                                    |2     |
// |&lt;grammar&gt;                                                   |1     |
// +------------------------------------------------------------------+------+
~~~

pyspark example:

~~~python
# section 5.1.3
from __future__ import print_function
from pyspark.sql.functions import *

# 1. example 1: scalar function 
# return a single value for each row based on calculations on one or more columns
postsDf.filter(postsDf.postTypeId == 1).withColumn("activePeriod", datediff(postsDf.lastActivityDate, postsDf.creationDate)).orderBy(desc("activePeriod")).head().body.replace("&lt;","<").replace("&gt;",">")
#<p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>

# 2. example 2: window function
# return a single value for a group of rows, also can be used with with `groupBy` (introduced in section 5.1.4)
postsDf.select(avg(postsDf.score), max(postsDf.score), count(postsDf.score)).show()

# 3. example 3: window function
# define a “moving group” of rows (called frames) based on some calculations with current row.  
# They don’t group rows into a single output row per group (different with ` aggregate functions`) 
from pyspark.sql.window import Window
winDf = postsDf.filter(
		postsDf.postTypeId == 1
	).select(
		postsDf.ownerUserId, 
		postsDf.acceptedAnswerId, 
		postsDf.score,
		max(
			postsDf.score
		).over(
			Window.partitionBy(postsDf.ownerUserId)
		).alias("maxPerUser")
	)
winDf.withColumn("toMax", winDf.maxPerUser - winDf.score).show(10)
# +-----------+----------------+-----+----------+-----+
# |ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
# +-----------+----------------+-----+----------+-----+
# |        232|            2185|    6|         6|    0|
# |        833|            2277|    4|         4|    0|
# |        833|            null|    1|         4|    3|
# |        235|            2004|   10|        10|    0|
# |        835|            2280|    3|         3|    0|
# |         37|            null|    4|        13|    9|
# |         37|            null|   13|        13|    0|
# |         37|            2313|    8|        13|    5|
# |         37|              20|   13|        13|    0|
# |         37|            null|    4|        13|    9|
# +-----------+----------------+-----+----------+-----+

postsDf.filter(
		postsDf.postTypeId == 1
	).select(
		postsDf.ownerUserId, 
		postsDf.id, 
		postsDf.creationDate, 
		lag(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("prev"), 
		lead(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("next")
	).orderBy(
		postsDf.ownerUserId, postsDf.id
	).show()
# +-----------+----+--------------------+----+----+
# |ownerUserId|  id|        creationDate|prev|next|
# +-----------+----+--------------------+----+----+
# |          4|1637|2014-01-24 06:51:...|null|null|
# |          8|   1|2013-11-05 20:22:...|null| 112|
# |          8| 112|2013-11-08 13:14:...|   1|1192|
# |          8|1192|2013-11-11 21:01:...| 112|1276|
# |          8|1276|2013-11-15 16:09:...|1192|1321|
# |          8|1321|2013-11-20 16:42:...|1276|1365|
# |          8|1365|2013-11-23 09:09:...|1321|null|
# |         12|  11|2013-11-05 21:30:...|null|  17|
# |         12|  17|2013-11-05 22:17:...|  11|  18|
# |         12|  18|2013-11-05 22:34:...|  17|  19|
# |         12|  19|2013-11-05 22:38:...|  18|  63|
# |         12|  63|2013-11-06 17:54:...|  19|  65|
# |         12|  65|2013-11-06 18:07:...|  63|  69|
# |         12|  69|2013-11-06 19:41:...|  65|  70|
# |         12|  70|2013-11-06 20:35:...|  69|  89|
# |         12|  89|2013-11-07 19:22:...|  70|  94|
# |         12|  94|2013-11-07 20:42:...|  89| 107|
# |         12| 107|2013-11-08 08:27:...|  94| 122|
# |         12| 122|2013-11-08 20:55:...| 107|1141|
# |         12|1141|2013-11-09 20:50:...| 122|1142|
# +-----------+----+--------------------+----+----+

# 4. example: UDF, user defined function
# background: question tags are stored in the format of '&lt;tag_name&gt;', we want to know how many tags each question has
countTags = udf(lambda (tags): tags.count("&lt;"), IntegerType())
postsDf.filter(
		postsDf.postTypeId == 1
	).select(
		"tags", countTags(postsDf.tags).alias("tagCnt")
	).show(10, False)
# +-------------------------------------------------------------------+------+
# |tags                                                               |tagCnt|
# +-------------------------------------------------------------------+------+
# |&lt;word-choice&gt;                                                |1     |
# |&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt;|3     |
# |&lt;usage&gt;&lt;verbs&gt;                                         |2     |
# |&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;              |3     |
# |&lt;usage&gt;&lt;punctuation&gt;                                   |2     |
# |&lt;usage&gt;&lt;tenses&gt;                                        |2     |
# |&lt;history&gt;&lt;english-comparison&gt;                          |2     |
# |&lt;idioms&gt;&lt;etymology&gt;                                    |2     |
# |&lt;idioms&gt;&lt;regional&gt;                                     |2     |
# |&lt;grammar&gt;                                                    |1     |
# +-------------------------------------------------------------------+------+
~~~

<b>something more about `window function`: </b> </br>

in the example above: `max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser"`</br>

1. <b>`max('score)`</b>: agg-function for construct agg-column: 

	> other agg-functions are: </br>
	> `min`,`max`,`sum`,`avg`,`count`, `first(column)`, `last(column)`, `lag(column, offset, [default])`, `lead (column, offset, [default])`, `ntile(n)`, `cumeDist`, `Rank`, `denseRank`, ...</br>
	> reference: [table 5.1](https://dpzbhybb2pdcj.cloudfront.net/bonaci/HighResolutionFigures/table_5-1.png)

2. <b>`Window.partitionBy('ownerUserId)`</b>: WindowSpec object: 

	> build with static function `org.apache.spark.sql.expressions.Window`, this function: <br/>
	> (1) need to specify partition columns using `partitionBy` function</br>
	> (2) optionally specify ordering in the partition using `orderBy` function</br>
	> (3) optionally further restrict which rows appear in frames by `rowsBetween` (according to index), `rangeBetween` (according to value) functions

3. <b>`over(Window.partitionBy('ownerUserId))`</b>: for build column


### 5.1.4. Working with missing values

example is as below, including: </br>

> 1. removes rows that have `null` values in all of the columns</br>
> 2. remove the rows that don’t have an `acceptedAnswerId`</br>
> 3. replace null and NaN values with a constant</br>
> 4. replace certain values in specific columns with different ones</br>

scala example: 

~~~scala
// 1. removes rows that have `null` values in all of the columns
val cleanPosts = postsDf.na.drop()
cleanPosts.count()  // res0: Long = 222

// 2. remove the rows that don’t have an `acceptedAnswerId`
postsDf.na.drop(Array("acceptedAnswerId"))

// 3. replace null and NaN values with a constant
postsDf.na.fill(Map("viewCount" -> 0))

// 4. replace certain values in specific columns with different ones
val postsDfCorrected = postsDf.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))
~~~

pyspark example:

~~~python
from __future__ import print_function

# 1. removes rows that have `null` values in all of the columns
cleanPosts = postsDf.na.drop()
cleanPosts.count()

# 2. replace null and NaN values with a constant
postsDf.na.fill({"viewCount": 0}).show()

# 3. replace certain values in specific columns with different ones
postsDf.na.replace(1177, 3000, ["id", "acceptedAnswerId"]).show()
~~~

### 5.1.5. Converting DataFrames to RDDs

example is as below, including: 

> 1. use `rdd` method of `DataFrame` to convert</br>
> 2. use `map`, `flatMap`, `mapPartitions` method to covert</br>
> 3. keep the `DataFrame` schema and convert the RDD back to `DataFrame`</br>

scala example: 

~~~scala
// 1. use `rdd` method of `DataFrame` to convert
// return type of `rdd` method is org.apache.spark.sql.Row
// it will be intorduced in section 5.11, 
// it have variouse get* method, such as `getString(index)`, `getInt(index)`, ... for access column indexes
// it also has mkString(delimiter) method for converting to strin
val postsRdd = postsDf.rdd 

// 2. use `map`, `flatMap`, `mapPartitions` method to covert 
val postsMapped = postsDf.rdd.map(row => 
       Row.fromSeq(
           row.toSeq
           .updated(
               3,
               row.getString(3)
                  .replace("&lt;","<")
                  .replace("&gt;",">"))
           .updated(
               8,
               row.getString(8)
                  .replace("&lt;","<")
                  .replace("&gt;",">"))
           )
       )
       
// 3. keep the `DataFrame` schema and convert the RDD back to `DataFrame`
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder().getOrElse() 
val postsDfNew = spark.createDataFrame(postsMapped, postsDf.schema)
~~~

pyspark example:

~~~python
from __future__ import print_function

# 1. use `rdd` method of `DataFrame` to convert
# return type of `rdd` method is org.apache.spark.sql.Row
# it will be intorduced in section 5.11, 
# it have variouse get* method, such as `getString(index)`, `getInt(index)`, ... for access column indexes
# it also has mkString(delimiter) method for converting to strin
postsRdd = postsDf.rdd

# 2. use `map`, `flatMap`, `mapPartitions` method to covert 
def replaceLtGt(row):
	return Row(
		commentCount = row.commentCount,
		lastActivityDate = row.lastActivityDate,
		ownerUserId = row.ownerUserId,
		body = row.body.replace("&lt;","<").replace("&gt;",">"),
		score = row.score,
		creationDate = row.creationDate,
		viewCount = row.viewCount,
		title = row.title,
		tags = row.tags.replace("&lt;","<").replace("&gt;",">"),
		answerCount = row.answerCount,
		acceptedAnswerId = row.acceptedAnswerId,
		postTypeId = row.postTypeId,
		id = row.id)
postsMapped = postsRdd.map(replaceLtGt)

# 3. keep the `DataFrame` schema and convert the RDD back to `DataFrame`
def sortSchema(schema):
	fields = {f.name: f for f in schema.fields}
	names = sorted(fields.keys())
	return StructType([fields[f] for f in names])
postsDfNew = sqlContext.createDataFrame(postsMapped, sortSchema(postsDf.schema))
~~~

### 5.1.6. Grouping and joining data

#### 5.1.6.1. group by

groupBy function returns a `GroupedData` object

> it can take one or more column expressions using aggregate funcstions </br>
> it also can take a map with column names to function name mappings

aggragate functions

(1) for build in aggregation function: </br>
> can be found in `org.apache.spark.sql.functions` such as `count`,`sum`,`max`,`min`,`avg` </br>

(2) for user defined aggreation, approach is as below: </br>

> 1. create a class that extends `org.apache.spark.sql.expressions.UserDefinedAggregateFunction`</br>
> 2. define input and buffer schemas</br>
> 3. implement the initialize, update, merge, and evaluate functions</br>

(3) details: [http://mng.bz/Gbt3](http://mng.bz/Gbt3)</br>
(4) Java example: [http://mng.bz/5bOb](http://mng.bz/5bOb)

scala example: 

~~~scala
postsDfNew
    .groupBy('ownerUserId, 'tags, 'postTypeId)
    // take 1 colume expr from agg-function 'count'
    .count 
    .orderBy('ownerUserId desc)
    .show(10)
// +-----------+--------------------+----------+-----+
// |ownerUserId|                tags|postTypeId|count|
// +-----------+--------------------+----------+-----+
// |        862|                    |         2|    1|
// |        855|         <resources>|         1|    1|
// |        846|<translation><eng...|         1|    1|
// |        845|<word-meaning><tr...|         1|    1|
// |        842|  <verbs><resources>|         1|    1|
// |        835|    <grammar><verbs>|         1|    1|
// |        833|                    |         2|    1|
// |        833|           <meaning>|         1|    1|
// |        833|<meaning><article...|         1|    1|
// |        814|                    |         2|    1|
// +-----------+--------------------+----------+-----+

postsDfNew
    .groupBy('ownerUserId)
    // take 2 columes expr from agg-function 'max'
    .agg(max('lastActivityDate), max('score))
    .show(10)

postsDfNew
    .groupBy('ownerUserId)
    // take a map with column names to function name mappings
    .agg(Map("lastActivityDate" -> "max", "score" -> "max"))
    .show(10)
// +-----------+---------------------+----------+
// |ownerUserId|max(lastActivityDate)|max(score)|
// +-----------+---------------------+----------+
// |        431| 2014-02-16 14:16:...|         1|
// |        232| 2014-08-18 20:25:...|         6|
// |        833| 2014-09-03 19:53:...|         4|
// |        633| 2014-05-15 22:22:...|         1|
// |        634| 2014-05-27 09:22:...|         6|
// |        234| 2014-07-12 17:56:...|         5|
// |        235| 2014-08-28 19:30:...|        10|
// |        435| 2014-02-18 13:10:...|        -2|
// |        835| 2014-08-26 15:35:...|         3|
// |         37| 2014-09-13 13:29:...|        23|
// +-----------+---------------------+----------+    

postsDfNew
    .groupBy('ownerUserId)
    .agg(max('lastActivityDate), max('score).gt(5))
    .show(10)
// +-----------+---------------------+----------------+
// |ownerUserId|max(lastActivityDate)|(max(score) > 5)|
// +-----------+---------------------+----------------+
// |        431| 2014-02-16 14:16:...|           false|
// |        232| 2014-08-18 20:25:...|            true|
// |        833| 2014-09-03 19:53:...|           false|
// |        633| 2014-05-15 22:22:...|           false|
// |        634| 2014-05-27 09:22:...|            true|
// |        234| 2014-07-12 17:56:...|           false|
// |        235| 2014-08-28 19:30:...|            true|
// |        435| 2014-02-18 13:10:...|           false|
// |        835| 2014-08-26 15:35:...|           false|
// |         37| 2014-09-13 13:29:...|            true|
// +-----------+---------------------+----------------+
~~~

pyspark example:

~~~python
from __future__ import print_function

postsDfNew.groupBy(postsDfNew.ownerUserId, postsDfNew.tags, postsDfNew.postTypeId).count().orderBy(postsDfNew.ownerUserId.desc()).show(10)
#+-----------+--------------------+----------+-----+
#|ownerUserId|                tags|postTypeId|count|
#+-----------+--------------------+----------+-----+
#|        862|                    |         2|    1|
#|        855|         <resources>|         1|    1|
#|        846|<translation><eng...|         1|    1|
#|        845|<word-meaning><tr...|         1|    1|
#|        842|  <verbs><resources>|         1|    1|
#|        835|    <grammar><verbs>|         1|    1|
#|        833|                    |         2|    1|
#|        833|           <meaning>|         1|    1|
#|        833|<meaning><article...|         1|    1|
#|        814|                    |         2|    1|
#+-----------+--------------------+----------+-----+

postsDfNew.groupBy(postsDfNew.ownerUserId).agg(max(postsDfNew.lastActivityDate), max(postsDfNew.score)).show(10)
postsDfNew.groupBy(postsDfNew.ownerUserId).agg({"lastActivityDate": "max", "score": "max"}).show(10)
# +-----------+---------------------+----------+
# |ownerUserId|max(lastActivityDate)|max(score)|
# +-----------+---------------------+----------+
# |        431| 2014-02-16 14:16:...|         1|
# |        232| 2014-08-18 20:25:...|         6|
# |        833| 2014-09-03 19:53:...|         4|
# |        633| 2014-05-15 22:22:...|         1|
# |        634| 2014-05-27 09:22:...|         6|
# |        234| 2014-07-12 17:56:...|         5|
# |        235| 2014-08-28 19:30:...|        10|
# |        435| 2014-02-18 13:10:...|        -2|
# |        835| 2014-08-26 15:35:...|         3|
# |         37| 2014-09-13 13:29:...|        23|
# +-----------+---------------------+----------+
postsDfNew.groupBy(postsDfNew.ownerUserId).agg(max(postsDfNew.lastActivityDate), max(postsDfNew.score) > 5).show(10)
# +-----------+---------------------+----------------+
# |ownerUserId|max(lastActivityDate)|(max(score) > 5)|
# +-----------+---------------------+----------------+
# |        431| 2014-02-16 14:16:...|           false|
# |        232| 2014-08-18 20:25:...|            true|
# |        833| 2014-09-03 19:53:...|           false|
# |        633| 2014-05-15 22:22:...|           false|
# |        634| 2014-05-27 09:22:...|            true|
# |        234| 2014-07-12 17:56:...|           false|
# |        235| 2014-08-28 19:30:...|            true|
# |        435| 2014-02-18 13:10:...|           false|
# |        835| 2014-08-26 15:35:...|           false|
# |         37| 2014-09-13 13:29:...|            true|
# +-----------+---------------------+----------------+
~~~

#### 5.1.6.2. rollup function and cube function

<b>rollup function</b>

> compared with `groupBy`, `rollup` respects the hierarchy of the input columns and always groups by the first column

scala example: 

~~~scala
// 1. sample data frame
val smplDf = postsDfNew
            .where('ownerUserId >= 13 and 'ownerUserId <= 15)
smplDf.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()
// +-----------+----+----------+-----+
// |ownerUserId|tags|postTypeId|count|
// +-----------+----+----------+-----+
// |         15|    |         2|    2|
// |         14|    |         2|    2|
// |         13|    |         2|    1|
// +-----------+----+----------+-----+

// 2. rollup
smplDf.rollup('ownerUserId, 'tags, 'postTypeId)
      .count
      .show()
// +-----------+----+----------+-----+
// |ownerUserId|tags|postTypeId|count|
// +-----------+----+----------+-----+
// |         15|    |         2|    2|
// |         13|    |      null|    1|
// |         13|null|      null|    1|
// |         14|    |      null|    2|
// |         13|    |         2|    1|
// |         14|null|      null|    2|
// |         15|    |      null|    2|
// |         14|    |         2|    2|
// |         15|null|      null|    2|
// |       null|null|      null|    5|
// +-----------+----+----------+-----+
~~~

pyspark example:

~~~python
from __future__ import print_function

# 1. `SparkSession` and `implicit` methods
# sc is SparkSession, this variable is initialized when starting PySpark shell, please refer to related docs or previouse chapter of this book


~~~

<b>cube function</b>

> `cube` function returns all of these results, but also adds other possible subtotals (per post type, per tags, per post type and tags, per post type and user)

~~~scala
smplDf.cube('ownerUserId, 'tags, 'postTypeId).count.show()
~~~

#### 5.1.6.3. Spark SQL Configuration

these configuration will affect the execution of DataFrame operations and SQL commands

> Spark Configuration (introducted in chapter 10) can not changed in run-time, </br>
> but Spark SQL Configuration can</br>
> example is as below: 

~~~scala
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder().getOrElse() 

// case sensitivity for query analysis (table and column names)
// set configuration by SQL command
spark.sql("SET spark.sql.caseSensitive=true")
// set configuration by invoking method
spark.conf.set("spark.sql.caseSensitive", "true")
~~~

another configuration: `spark.sql.eagerAnalysis`

> if set it to true, spark throws an exception as soon as you mention a non-existent column in a DataFrame, instead of waiting for you to perform an action on the DataFrame that fetches the results.

### 5.1.7. Performing joins

join types: `inner`, `outer`, `left_outer`, `right_outer`, or `leftsemi`, `leftanti`, ...

example: 

~~~scala
// 1. load the raw data
val itVotesRaw = 
	sc.textFile("first-edition/ch05/italianVotes.csv").
  	map(x => x.split("~"))

// 2. convert to Rows
val itVotesRows = 
	itVotesRaw.map(row => Row(
		row(0).toLong, row(1).toLong, row(2).toInt, 
		Timestamp.valueOf(row(3)))
	)

// 3. define Schema
val votesSchema = StructType(Seq(
	StructField("id", LongType, false),
	StructField("postId", LongType, false),
	StructField("voteTypeId", IntegerType, false),
	StructField("creationDate", TimestampType, false)) )

// 3. create the DataFrame based on the Rows and Schema
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder().getOrElse() 
val votesDf = spark.createDataFrame(itVotesRows, votesSchema)

// 4. inner Join by postId
val postsVotes 
	= postsDf.join(votesDf, postsDf("id") === 'postId)

// 5 outer Join by postId
val postsVotesOuter
	= postsDf.join(votesDf, postsDf("id") === 'postId, "outer")

// postId is unique across both DataFrames, 
// which make it Ok to use implicit conversion from Scala's Symbol ('postaId)
~~~

pyspark example:

~~~python
from __future__ import print_function

# 1. `SparkSession` and `implicit` methods
# sc is SparkSession, this variable is initialized when starting PySpark shell, please refer to related docs or previouse chapter of this book


~~~

> Notice: `spark.sql.shuffle.partitions` has influence on performance, currently it is a fixed parameter rather then depending on the data and runtime environment </br>
> Spark JIRA tickets to following this problem: [SPARK-9872](https://issues.apache.org/jira/browse/SPARK-9872), [SPARK-9850](https://issues.apache.org/jira/browse/SPARK-9850)

## 5.2. Beyond DataFrames: introducing DataSets

> `DataSets` represent a competition to RDDs (have overlapping functions). </br>
> `DataFrames` are now simply implemented as `DataSets` containing `Row` objects.

convert `DataFrame` to `DataSet` 

~~~scala
// convert `DataFrame` to `DataSet`: using `DataFrame.as(u:Encoder)` function
// String is a Encoder
// Also you can write your own encoders or use encoders from ordinary Java bean classe
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder().getOrElse() 
val stringDataSet = spark.read.text("path/to/file").as[String]
~~~

DataSet documents: [http://mng.bz/3EQc](http://mng.bz/3EQc)

## 5.3. Using SQL commands (Spark SQL)

* The DataFrame DSL functionalities presented in the previous sections are also accessible through SQL commands as an alternative interface for programming Spark SQL</br>

	> 5.2用DataFrame DSL进行的数据操作，同样也可以用SQL来实现</br>

* Spark supports two SQL dialects: Spark’s SQL dialect and Hive Query Language (HQL) 

	> Spark支持两种SQL Dialect：Spark SQL和Hive SQL（HQL）, 官方更加推荐HQL（需要使用支持HQL的Spark Distribution）

~~~scala
// Initial SparkSession with HQL support
// 初始化SparkSession时开启HQL
val spark = SparkSession.builder().
    enableHiveSupport().
    getOrCreate()
~~~    

### 5.3.1 Table catalog and Hive metastore（元数据）

> Spark stores the `table definition` in the `table catalog`, thus you can reference a DataFrame by its `name` by registering `the DataFrame as a table`.  A `Hive metastore` is a persistent database. Spark with HQL support use Hive metastore to implement the `table catalog` 

>`table catalog` 用于存储`table name` 到`DataFrame`的映射：如果Spark没有开启HQL Support、这些映射只能存储在内存中；如果开启了HQL、这些映射还可以在DB中持久保存，数据库持久化文件的路径配置在`hive-site.xml`的`hive.metastore.warehouse.dir`中 

~~~scala
// create temporary table definitions
// DataFrame定义为临时表（随Spark关闭后会失效）
postsDf.createOrReplaceTempView("posts_temp") // DataFrame: postsDf -> Table Name: "posts_temp"

// registering tables permanently
// DataFrame定义为持久表（需要Spark支持HQL）
postsDf.write.saveAsTable("posts")   // DataFrame: postsDf -> Table Name: "posts"
votesDf.write.saveAsTable("votes")   // DataFrame: votesDf -> Table Name: "votes"

// check tables: the last field `isTemporary` indicate whether they will un-registered when restarting Spark
// 检查数据表，
// 最后一列isTemporary表示是否是持久表；
// 第三列tableType为MANAGED表示是Spark的表、为EXTERNAL时表示这张表来自外部数据库例如某个RMDB
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder().getOrElse() 
spark.catalog.listTables().show()
// +----------+--------+-----------+---------+-----------+
// |      name|database|description|tableType|isTemporary|
// +----------+--------+-----------+---------+-----------+
// |     posts| default|       null|  MANAGED|      false|
// |     votes| default|       null|  MANAGED|      false|
// |posts_temp|    null|       null|TEMPORARY|       true|
// +----------+--------+-----------+---------+-----------+

// check schema of a table
// 检查一张表的schema
spark.catalog.listColumns("votes").show()
// +------------+-----------+---------+--------+-----------+--------+
// |        name|description| dataType|nullable|isPartition|isBucket|
// +------------+-----------+---------+--------+-----------+--------+
// |          id|       null|   bigint|    true|      false|   false|
// |      postid|       null|   bigint|    true|      false|   false|
// |  votetypeid|       null|      int|    true|      false|   false|
// |creationdate|       null|timestamp|    true|      false|   false|
// +------------+-----------+---------+--------+-----------+--------+

// get a list of all available SQL functions call
spark.catalog.listFunctions.show()
~~~

pyspark example:

~~~python
from __future__ import print_function

# 1. `SparkSession` and `implicit` methods
# sc is SparkSession, this variable is initialized when starting PySpark shell, please refer to related docs or previouse chapter of this book


~~~

> other functions of catalog: `cacheTable`, `uncacheTable`, `isCached`, `clearCache`, ...

#### Configuring a remote Hive metastore

> configure Spark to use remote Hive table in `hive-site.xml`

configuration properties: 

> `spark.sql.warehouse.dir`: path for storing meta data</br>
> `javax.jdo.option.ConnectionURL`: JDBC url</br>
> `javax.jdo.option.ConnectionDriverName`: JDBC driver class name, must in classpath in the driver and all executors, one approach is use `--jars` when submitting application or staring spark shell</br>
> `javax.jdo.option.ConnectionUserName`: DB username</br>
> `javax.jdo.option.ConnectionPassword`: DB passwd</br>

configuration sample:

~~~xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>/hive/metastore/directory</value>
</property>
</configuration>
~~~

initialize the metastore database and create the necessary tables</br>

> use Hive `schematool`</br>
> document: [http://mng.bz/3HJ5](http://mng.bz/3HJ5)

### 5.3.2. Executing SQL queries

#### Execute SQL

~~~scala
import spark.sql

// need register DataFrame as table first as in previouse section
val resultDf = sql("select * from posts") //result is a DataFrame
~~~

If using Spark Session with Hive Support, all Hive DLL Command like `ALTER TALBE`, `DROP TABLE` are supported

> complete list of supported Hive features in the Spark documentation: [http://mng.bz/8AFz](http://mng.bz/8AFz) </br>
> Hive language manual: [http://mng.bz/x7k2](http://mng.bz/x7k2)
 
#### Using the Spark SQL shell

start spark-sql in shell

~~~bash
$ # argument is the same as spark-shell or spark-submit
$ # no parameter will start Spark in local mode
$ spark-sql 
~~~

execute SQL in spark-shell

~~~bash
spark-sql> select substring(title, 0, 70) from posts where
  postTypeId = 1 order by creationDate desc limit 3;
# Verbo impersonale che regge verbo impersonale: costruzione implicita? Perch?Š si chiama &quot;saracinesca&quot; la chiusura metallica scorren Perch?Š a volte si scrive l'accento acuto sulla &quot;i&quot; o sulla & 
# Time taken: 0.375 seconds, Fetched 3 row(s)
~~~

execute SQL when start spark-shell

~~~bash
$ spark-sql -e "select substring(title, 0, 70) from posts where
 postTypeId= 1 order by creationDate desc limit 3"
~~~

### 5.3.3. Connecting to Spark SQL through the Thrift server

> for running spark SQL remotely through JDBC(and ODBC) server called Thrift

#### Starting a Thrift server

> from Spark’s sbin directory

~~~bash
$ # the same parameter as for spark-shell or spark-sql
$ # using `--jar` could indicate the JDBC driver JAR
$ sbin/start-thriftserver.sh --jars /usr/share/java/postgresql-jdbc4.jar
~~~

> default port: 10000</br>

change listening port and host name by `Environment Variable`
> `HIVE_SERVER2_THRIFT_PORT`, `HIVE_SERVER2_THRIFT_BIND_HOST`

change listening port and host name by `Hive configuration variables` specified with parameter `--hiveconf`

> `hive.server2 .thrift.port`, `hive.server2.thrift.bind.host`

#### Connecting to the Thrift server with Beeline

test connection of Thrift server with Beeline

~~~bash
$ beeline -u jdbc:hive2://<server_name>:<port> -n <username> -p <password>
Connecting to jdbc:hive2://<server_name>:<port>
Connected to: Spark SQL (version 1.5.0)
Driver: Spark Project Core (version 1.5.0)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 1.5.0 by Apache Hive
0: jdbc:hive2://<server_name>:<port>>
~~~

#### Connecting from third-party JDBC clients

> basically by configuring the JDBC client in the 3rd-party application

> details: [https://livebook.manning.com/book/spark-in-action/chapter-5/331](https://livebook.manning.com/book/spark-in-action/chapter-5/331)

## 5.4. Saving and loading DataFrame data

> Build-in Support: `Hive`, `JDBC`, `JSON`, `ORC`, `Parquet`</br>
> External Support (can download and use）:  [CSV](https://github.com/databricks/spark-csv), [Avro](https://github.com/databricks/spark-avro), [Amazon-Redshift](https://github.com/databricks/spark-redshift)

### 5.4.1. Built-in data sources

> basically is a brief introduction of `JSON`,`ORC`,`Parquet` </br>
> details: [https://livebook.manning.com/book/spark-in-action/chapter-5/354](https://livebook.manning.com/book/spark-in-action/chapter-5/354)

### 5.4.2. Saving data

~~~
// postsDf: DataFrame
// postsDf.write: DataFrameWriter
postsDf.write.saveAsTable("posts")
~~~

functions of `DataFrameWriter`: 

> `insertInto`, `saveAsTable`: write into table (Temperary Table by Default)</br>
> `save`: write into others

#### configuring the writer

`DataFrameWriter` has functions to configure the write behavior like below

~~~scala
postsDf.write
    .format("orc")
    .mode("overwrite")
    .option(...)
~~~

functions: 

name | usage | parameter values |
:-: | :-: | :-: |
format |  file format of the data | `json`,`parquet`(default),`orc` | 
mode | save mode when a table or a file already exists | `overwrite`, `append`, `ignore`, `error`(default) | 
option | for datasource configuration | parameter name and value |
options | for datasource configuration | parameter name-value map |

#### using the `saveAsTable` method

~~~scala
// save into table with name "postsjson" by "json" format
// table type: 
//  if is using Hive support, the table will registere Hive metastore 
//  if not, the table will be a temperary table
// data format: 
//  if Hive SerDe exists, Hive lib will take care the format
//  if not exists, will format as the parameter "json"
postsDf.write
    .format("json") 
    .saveAsTable("postsjson")
~~~

~~~bash
# each line will be a json object
scala> sql("select * from postsjson")
~~~

#### using the `insertInto` method

> * `the table` should be already exists in the Hive metastore, with the same `schema` as the `DataFrame` being inserted</br>
> * configuration `format`, `options` will be ignore</br>
> * if `mode` is set to `overwrite`, table will be cleared before insert</br>

#### using the `save` method

> save to the filesystem rather than using `Hive metastore`</br>
> <b>parameter</b>: a `direct path`, including `HDFS path`, `S3 path`, `local path URL`(will save locally on every executor node) 

#### using the `shortcut methods`

shortcut | identical with |
:-: | :-: | 
json(...) | format("json").save(...) |
ocr(...) | format("ocr").save(...) | 
parquet(...) | format("parquet").save(...) | 

#### saving data to relational databases with JDBC

~~~scala
val props = new java.util.Properties()
props.setProperty("user", "user")
props.setProperty("password", "password")
postsDf.write
    jdbc("jdbc:postgresql://postgresrv/mydb", "posts", props)
~~~

should not have too many `partition`s in the dataframe 

> they will connect to database and save data at the same time 

set `spark.executor.extraClassPath` in Spark configuration (chapter 10) will make sure all Spark executor have the db-driver JAR 

### 5.4.3. Loading data

#### load a DataFrame from a table registered in the Hive metastore

~~~scala
// spark session
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder().getOrElse() 
// read data
val postsDf1 = spark   // SparkSession
              .read    // DataFrameReader
              .table("posts")
// or 
val postsDf2 = spark.table("posts")
~~~

#### Loading data from relational databases using JDBC

~~~scala
// spark session
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder().getOrElse() 
// jdbc properties
val props = new java.util.Properties()
props.setProperty("user", "user")
props.setProperty("password", "password")
// read data
val result = spark  # SparkSession
            .read   # read data
            .jdbc(
                "jdbc:postgresql://postgresrv/mydb", // jdbc 
                "posts",                // table name
                Array("viewCount > 3"), // expressions add into where clause
                props                   // jdbc properties
            )
~~~

#### Loading data from data sources registered using SQL

register temporary tables from `jdbc` data source and load it

~~~bash
scala> sql("CREATE TEMPORARY TABLE postsjdbc "+
  "USING org.apache.spark.sql.jdbc "+
  "OPTIONS ("+
    "url 'jdbc:postgresql://postgresrv/mydb',"+
    "dbtable 'posts',"+

    "user 'user',"+
    "password 'password')")
scala> val result = sql("select * from postsjdbc")
~~~

register a Parquet file and load it's content

~~~bash
scala> sql("CREATE TEMPORARY TABLE postsParquet "+
  "USING org.apache.spark.sql.parquet "+
  "OPTIONS (path '/path/to/parquet_file')")
scala> val resParq = sql("select * from postsParquet")
~~~

## 5.5. Catalyst optimizer

#### Examining the execution plan

[https://livebook.manning.com/book/spark-in-action/chapter-5/407](https://livebook.manning.com/book/spark-in-action/chapter-5/407)

#### Taking advantage of partition statistics

[https://livebook.manning.com/book/spark-in-action/chapter-5/415](https://livebook.manning.com/book/spark-in-action/chapter-5/415)

## 5.6. Performance improvements with Tungsten

[https://livebook.manning.com/book/spark-in-action/chapter-5/417](https://livebook.manning.com/book/spark-in-action/chapter-5/417)

