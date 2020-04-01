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

#### Approach 2: Converting RDDs to DataFrames using case classes

map each `row` to a `case class` and then use the `toDF` method<br/>
below is an example, including: 

> 1. `SparkSession` and `implicit` methods</br>
> 2. define a `case class` to specify the schema</br>
> 3. define an `implicit class` to make `RDD->case mapping` more elegantly, the idea is from [http://mng.bz/ih7n](http://mng.bz/ih7n)</br>
> 4. use the functions of the `implicit class StringImprovements` in `StringImplicits` to convert the RDD to `case object`</br>
> 5. now the `DataFrame` contains the proper types and `nullable` flags
itPostsDFCase.printSchema</br>

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

// 4. create an RDD and the DataFrame
val rowRDD = itPostsRows.map(row => stringToRow(row))

// 5. get schema information
val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)
itPostsDFStruct.columns
// 	res0: Array[String] = Array(
//		commentCount, lastActivityDate, ownerUserId, body, score, creationDate, viewCount, title, tags, answerCount, acceptedAnswerId, postTypeId, id)
itPostsDFStruct.dtypes
//	res1: Array[(String, String)] = Array(
//		(commentCount,IntegerType), (lastActivityDate,TimestampType), (ownerUserId,LongType), (body,StringType), (score,IntegerType), (creationDate,TimestampType), (viewCount,IntegerType), (title,StringType), (tags,StringType), (answerCount,IntegerType), (acceptedAnswerId,LongType), (postTypeId,LongType), (id,LongType))
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

### 5.1.5. Converting DataFrames to RDDs

example is as below, including: 

> 1. use `rdd` method of `DataFrame` to convert</br>
> 2. use `map`, `flatMap`, `mapPartitions` method to covert</br>
> 3. keep the `DataFrame` schema and convert the RDD back to `DataFrame`</br>

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

example: 

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

#### 5.1.6.2. rollup function and cube function

<b>rollup function</b>

> compared with `groupBy`, `rollup` respects the hierarchy of the input columns and always groups by the first column

example 

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

