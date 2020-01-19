# CHO5 Spark SQL

* 5.1. Working with DataFrames
* 5.2. Beyond DataFrames: introducing DataSets
* 5.3. Using SQL commands
* 5.4. Saving and loading DataFrame data
* 5.5. Catalyst optimizer
* 5.6. Performance improvements with Tungsten
* 5.7. Summary

## 5.1. Working with DataFrames

`DataFrame` is for structured data (distributed data in a table-like representation) with named columns and declared column types.

`RDD` is the low-level representation

> Since Spark 2.0, DataFrame is implemented as a special case of `DataSet`

Spark SQL also lets you register DataFrames as tables to let applications (spark-application and JDBC/ODBC application throw thrift server) query with the `DataFrames' names`

3 ways to create `DataFrame`: 

* converting existing RDDs
* running SQL
* loading external data

### 5.1.1 creating Dataframes from RDDs 

> use RDDs to load and transform unstructured data and then create DataFrames from RDDs if you want to use the DataFrame API

three ways:

* using `RDDs` containing row data as tuples: can not specify all the schema attributes
* using `case` classes
* specifying a schema: standard in Spark by explicitly specifying a schema

prerequisites: `SparkSession` object, some implicit methods, `dataset`

#### Creating SparkSession and importing implicit methods

~~~scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrElse()

// import implicit methods for automatically converting RDDs to DataFrames
import spark.implicits._
~~~

> `SparkSession` is a wrapper around `SparkContext` and `SQLContext` <br/>
> `Builder` object lets you specify `master`, `appName`, ...<br/>
> `import spark.implicits._`: import implicit methods for automatically converting RDDs to DataFrames <br/>

These `implicits` add one method, called toDF, to your RDD if the RDD contains objects for which a `DataSet Encoder` is defined

`Encoders` are used for converting JVM objects to internal Spark SQL representation. The list of Encoders can be found in [http://mng.bz/Wa45](http://mng.bz/Wa45)

#### example dataset

format of the data (italianPosts.csv delimited with "~" (tilde signs)) of the example 

* `commentCount` — Number of comments related to the question/answer
* `lastActivityDate` — Date and time of the last modification
* `ownerUserId` — User ID of the owner
* `body`—Textual contents of the question/answer
* `score`—Total score based on upvotes and downvotes
* `creationDate` — Date and time of creation
* `viewCount`—View count
* `title`—Title of the question
* `tags`—Set of tags the question has been marked with
* `answerCount`—Number of related answers
* `acceptedAnswerId`—If a question contains the ID of its accepted answer
* `postTypeId`—Type of the post; 1 is for questions, 2 for answers
* `id`—Post’s unique ID

load the dataset

~~~scala
val itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv")
val itPostsSplit = itPostsRows.map(x => x.split("~"))
//	itPostsSplit: org.apache.spark.rdd.RDD[Array[String]] = ...
~~~

#### Creating a DataFrame from an RDD of tuples

step 1: convert each Array[String] to a tuple

~~~scala
val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),
  x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
//		itPostsRDD: org.apache.spark.rdd.RDD[(String, String, ...
~~~

> There’s no elegant way to convert an array to a tuple, so you have to resort to this ugly expression

step 2: use `toDF` function convert the tuples to DataFrame

~~~scala
val itPostsDFrame = itPostsRDD.toDF()
//		itPostsDF: org.apache.spark.sql.DataFrame = [_1: string, ...
~~~

~~~shell
scala> itPostsDFrame.show(3) # default 20
+---+--------------------+---+--------------------+---+--------------------
| _1|                  _2| _3|                  _4| _5|                  _6
+---+--------------------+---+--------------------+---+--------------------
|  4|2013-11-11 18:21:...| 17|&lt;p&gt;The infi...| 23|2013-11-10 19:37:...
|  5|2013-11-10 20:31:...| 12|&lt;p&gt;Come cre...|  1|2013-11-10 19:44:...
|  2|2013-11-10 20:31:...| 17|&lt;p&gt;Il verbo...|  5|2013-11-10 19:58:...
+---+--------------------+---+--------------------+---+--------------------
~~~

alternatively, column names can be assigned when invoking `toDF()` and can be displayed by invoking `printSchema`

~~~scala
val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
~~~

~~~shell
scala> itPostsDF.printSchema
root
 |-- commentCount: string (nullable = true)
 |-- lastActivityDate: string (nullable = true)
 |-- ownerUserId: string (nullable = true)
 |-- body: string (nullable = true)
 |-- score: string (nullable = true)
 |-- creationDate: string (nullable = true)
 |-- viewCount: string (nullable = true)
 |-- title: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- answerCount: string (nullable = true)
 |-- acceptedAnswerId: string (nullable = true)
 |-- postTypeId: string (nullable = true)
 |-- id: string (nullable = true)
~~~

the <b>problem</b> of `toDF()` conversion is that all columns can only be `String` and `nullable`

#### Converting RDDs to DataFrames using case classes

map each `row` to a `case class` and then use the `toDF` method, below is an example 

Firstly define a `case class` named `Post`

~~~scala
import java.sql.Timestamp
case class Post(
  commentCount:Option[Int],
  lastActivityDate:Option[java.sql.Timestamp],
  ownerUserId:Option[Long],
  body:String,
  score:Option[Int],
  creationDate:Option[java.sql.Timestamp],
  viewCount:Option[Int],
  title:String,
  tags:String,
  answerCount:Option[Int],
  acceptedAnswerId:Option[Long],
  postTypeId:Option[Long],
  id:Long)
~~~

Secondly declare an implicit class help to make the `RDD->case` mapping code more elegantly (the idea is from [http://mng.bz/ih7n](http://mng.bz/ih7n)

~~~scala
object StringImplicits {
  // implicit class
  implicit class StringImprovements(val s: String) {
    import scala.util.control.Exception.catching
    def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
    def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
    def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
  }
}
~~~

> `*Safe` means if a string can’t be converted to the type, instead of throwing an exception, the methods return `None` <br/>
> `*catching` function returns an object of type `scala.util.control.Exception.Catch`, whose `opt` method can execute the mapping with the specified function passed in (such as `s.toInt`, `s.toLong`, `Timestamp.valueOf(s)` above) or return `None` if exception occurs

Thirdly, use the functions of the `implicit class StringImprovements` in `StringImplicits` to convert the RDD to `case object`

~~~scala
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
    r(12).toLong  //the last column (id) can't be null
  )
}
val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()
~~~

Noew the `DataFrame` contains the proper types and `nullable` flags

~~~shell
scala> itPostsDFCase.printSchema
root
 |-- commentCount: integer (nullable = true)
 |-- lastActivityDate: timestamp (nullable = true)
 |-- ownerUserId: long (nullable = true)
 |-- body: string (nullable = true)
 |-- score: integer (nullable = true)
 |-- creationDate: timestamp (nullable = true)
 |-- viewCount: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- answerCount: integer (nullable = true)
 |-- acceptedAnswerId: long (nullable = true)
 |-- postTypeId: long (nullable = true)
 |-- id: long (nullable = false) # nullable is false
~~~

#### Converting RDDs to DataFrames by specifying a schema

use `SparkSession`'s `createDataFrame` method, parameters includes `Row` (object in RDD) and `StructType` (represents a schema, contains 1~N `StructField`s for represent columns)

Firstly, define the schema

~~~scala
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
~~~

Types used in schema: `org.apache.spark.sql.types`

> <b>simple types</b>: strings, integers, shorts, floats, doubles, bytes, dates, timestamps, and binary values </br>
> <b>complex types</b>: Arrays, Maps, Structs (with nested column definations)

Secondly, define function convert the `String`(a element/record) to `Row`(with schema) 

~~~
def stringToRow(row:String):Row = {
    val r = row.split("~")
    // because Spark 2.0 doesn't handle scala Option objects well when creating DataFrame in this way, we use null instead of Option
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
~~~

Finally, create an RDD and the DataFrame

~~~scala
val rowRDD = itPostsRows.map(row => stringToRow(row))
val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)
~~~ 

#### Getting schema information

two examples above has the same schema (althrough use differet approach), this can be verified by `DataFrame.schema` function

besides `DataFrame.schema`, other functions can also give schema informastions

~~~scala
itPostsDFCase.columns
// 	res0: Array[String] = Array(
//		commentCount, lastActivityDate, ownerUserId, body, score, creationDate, viewCount, title, tags, answerCount, acceptedAnswerId, postTypeId, id)

itPostsDFStruct.dtypes
//	res1: Array[(String, String)] = Array(
//		(commentCount,IntegerType), (lastActivityDate,TimestampType), (ownerUserId,LongType), (body,StringType), (score,IntegerType), (creationDate,TimestampType), (viewCount,IntegerType), (title,StringType), (tags,StringType), (answerCount,IntegerType), (acceptedAnswerId,LongType), (postTypeId,LongType), (id,LongType))
~~~

### 5.1.2. DataFrame API basics

`DataFrame`s work like RDDs: they’re immutable and lazy

#### Selecting Columns

select part of the `Column`s of the `DataFrame`

~~~scala
val postsDf = itPostsDFStruct
val postsIdBody = postsDf.select("id", "body")
//	postsIdBody: org.apache.spark.sql.DataFrame = [id: bigint, body: string]
~~~

identical approach by creating `Column` object with some methods

~~~scala
// DataFrame.col
val postsIdBody = postsDf.select(
	postsDf.col("id"), postsDf.col("body"))

// implicit methods imported (introducted in $5.1.1) to covert scala Symbol to Column.Symbol
val postsIdBody = postsDf.select(Symbol("id"), Symbol("body")) 
val postsIdBody = postsDf.select('id, 'body) //Scala’s built-in quote mechanism do the same thing

// another implicit method called $ which convert String to ColumnName object
val postsIdBody = postsDf.select($"id", $"body")
~~~

select the data expect some columns

~~~scala
// select all columns except "bocy" column
val postIds = postsIdBody.drop("body")
~~~

#### Filtering data

`where` and `filter` functions (they're synonymous) can filter `DataFrame`

~~~scala
postsIdBody.filter('body contains "Italiano").count
// 		res0: Long = 46

val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))
~~~

> The variant taking a string is used for parsing SQL expressions. Again, we’ll get to that in section 5.2. 

<b>complete set of operators</b>: [http://mng.bz/a2Xt](http://mng.bz/a2Xt)

select only first *n* rows with `limit` function 

~~~scala
val firstTenQs = postsDf.filter('postTypeId === 1).limit(10)
~~~

#### Adding columns

use `withColumnRenamed` function to rename a column to give it a shorter or a more meaningful name

~~~scala
val firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")
~~~

#### renaming columns

use `withColumn` function to add column based on the existed column names and the `Column` expression

~~~scala
postsDf.filter('postTypeId === 1).
  withColumn("ratio", 'viewCount / 'score).
  where('ratio < 35).
  show()
~~~

#### Sorting data

use `orderBy` or `sort` function

> They take one or more column names or one or more `Column` expressions. The `Column` class has `asc` and `desc` operators, which are used for specifying the sort order. The default is to sort in ascending order

### 5.1.3. Using SQL functions to perform calculations on data

> SQL functions fit into four categories:<br/>
> 1. Scalar functions return a single value for each row based on calculations on one or more columns<br/>
> 2. Aggregate functions return a single value for a group of rows<br/>
> 3. Window functions return several values for a group of rows<br/>
> 4. User-defined functions include custom scalar or aggregate functions

#### built-in scalar functions and aggregate functions

<b>built-in scalar functions</b>: `abs`, `exp`, `substring`, `hypot`(hypotenuse), `log`, `cbrt`(cube root), `length`, `trim`, `concat`, `year`, `date_add`, `date_diff`... 

<b>built-in aggregate functions</b>: `avg`, `min`, `max`, `count`, `sum`, ..., combined with `groupBy` ($5.1.4)

~~~scala
import org.apache.spark.sql.functions._
~~~

example 1: 

~~~scala
postsDf.filter('postTypeId === 1).
  withColumn("activePeriod", datediff('lastActivityDate, 'creationDate)).
  orderBy('activePeriod desc).head.getString(3).
  replace("&lt;","<").replace("&gt;",">")
//		res0: String = <p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>
~~~

example 2: 

~~~shell
scala> postsDf.select(avg('score), max('score), count('score)).show
+-----------------+----------+------------+
|       avg(score)|max(score)|count(score)|
+-----------------+----------+------------+
|4.159397303727201|        24|        1261|
+-----------------+----------+------------+
~~~

#### Window functions

`window functions` let you define a “moving group” of rows, called frames, which are based on some calculations with current row

> they don’t group rows into a single output row per group, thus is different compared with ` aggregate functions` 

example 1: 

~~~scala
postsDf.
	filter('postTypeId === 1).
	select(
		'ownerUserId, 
		'acceptedAnswerId, 
		'score, 
		// use window function to aggregate
		max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser"
	).
  	withColumn("toMax", 'maxPerUser - 'score). //defin new column
  	show(3)
~~~

output

~~~shell
+-----------+----------------+-----+----------+-----+
|ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
+-----------+----------------+-----+----------+-----+
|        232|            2185|    6|         6|    0|
|        833|            2277|    4|         4|    0|
|        833|            null|    1|         4|    3|
|        235|            2004|   10|        10|    0|
|        835|            2280|    3|         3|    0|
|         37|            null|    4|        13|    9|
|         37|            null|   13|        13|    0|
|         37|            2313|    8|        13|    5|
|         37|              20|   13|        13|    0|
|         37|            null|    4|        13|    9|
+-----------+----------------+-----+----------+-----+
~~~

How to use `window function`: 

> in the example: `max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser"`

1. <b>Construct Agg-Column</b>: `max('score)`

	construct a Column with an aggregate function with agg-function (`min`,`max`,`sum`,`avg`,`count`,...) or function in [table 5.1](https://dpzbhybb2pdcj.cloudfront.net/bonaci/HighResolutionFigures/table_5-1.png), including: `first(column)`, `last(column)`, `lag(column, offset, [default])`, `lead (column, offset, [default])`, `ntile(n)`, `cumeDist`, `Rank`, `denseRank`

2. <b>build a `WindowSpec object`</b>: `Window.partitionBy('ownerUserId)`

	use static function `org.apache.spark.sql.expressions.Window` to build this object<br/>
	* need to specify partition columns using `partitionBy` function
	* optionally specify ordering in the partition using `orderBy` function
	* optionally further restrict which rows appear in frames by `rowsBetween` (according to index), `rangeBetween` (according to value) functions

3. <b>build the column by `over` function</b>: `over(Window.partitionBy('ownerUserId))`

	> pass the `WindowSpec object` as the parameter to the `over` function

#### User-defined functions (UDF)

<b>requirement</b>: question tags is stored in the format of '&lt;tag_name&gt;', we want to know how many tags each question has

`UDF` are created with `udf` function, take 0 to 10 (maximum) columns and return the final value

~~~scala
val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).length)
//		countTags: org.apache.spark.sql.UserDefinedFunction = ...
~~~

Alternatively using `SparkSession.udf.register` function, and UDF will get a name which can also used in SQL expressions (section $5.2)

~~~scala
val countTags = spark.udf.register("countTags",
    (tags: String) => "&lt;".r.findAllMatchIn(tags).length)
    
// use the registered name in SQL expressions
postsDf
	.filter('postTypeId === 1)
	.select('tags, countTags('tags) as "tagCnt")
	.show(10, false) //false means not to truncate the strings in columns
~~~

output

~~~shell
+------------------------------------------------------------------+------+
|tags                                                              |tagCnt|
+------------------------------------------------------------------+------+
|&lt;word-choice&gt;                                               |1     |
|&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt|3     |
|&lt;usage&gt;&lt;verbs&gt;                                        |2     |
|&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;             |3     |
|&lt;usage&gt;&lt;punctuation&gt;                                  |2     |
|&lt;usage&gt;&lt;tenses&gt;                                       |2     |
|&lt;history&gt;&lt;english-comparison&gt;                         |2     |
|&lt;idioms&gt;&lt;etymology&gt;                                   |2     |
|&lt;idioms&gt;&lt;regional&gt;                                    |2     |
|&lt;grammar&gt;                                                   |1     |
+------------------------------------------------------------------+------+
~~~

### 5.1.4. Working with missing values

removes rows that have `null` values in all of the columns

~~~scala
val cleanPosts = postsDf.na.drop()
cleanPosts.count()
//		res0: Long = 222
~~~

remove the rows that don’t have an `acceptedAnswerId`

~~~scala
postsDf.na.drop(Array("acceptedAnswerId"))
~~~

replace null and NaN values with a constant

~~~scala
postsDf.na.fill(Map("viewCount" -> 0))
~~~

replace certain values in specific columns with different ones

~~~scala
val postsDfCorrected = postsDf.na.
    replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))
~~~

### 5.1.5. Converting DataFrames to RDDs

~~~scala
val postsRdd = postsDf.rdd
//		postsRdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = ...
~~~

notice: 

* result RDD element is `org.apache.spark.sql.Row` (section $5.11), which has various `get*` (`getString(index)`, `getInt(index)`, ...) functions for accessing column values by column indexs, and `mkString(delimiter)` for converting to string

* `map`,`flatMap`,`mapPartitions` transformations when converting DataFrames to RDDs, and keep the `DataFrame` schema if need to convert the RDD back to `DataFrame`

~~~scala
val postsMapped = postsDf.rdd.map(row => Row.fromSeq(
  row.toSeq.
    updated(3, row.getString(3).replace("&lt;","<").replace("&gt;",">")).
    updated(8, row.getString(8).replace("&lt;","<").replace("&gt;",">"))))
val postsDfNew = spark.createDataFrame(postsMapped, postsDf.schema)
~~~

### 5.1.6. Grouping and joining data

<b>`GroupedData`</b>: 

* returned by `groupBy` function
* it can take one or more column expressions using aggregate funcstions such as `count`,`sum`,`max`,`min`,`avg` from `org.apache.spark.sql.functions`
* it also can take a map with column names to function name mappings

example 1

~~~scala
postsDfNew.groupBy('ownerUserId, 'tags, 'postTypeId).count.orderBy('ownerUserId desc).show(10)
~~~

~~~shell
+-----------+--------------------+----------+-----+
|ownerUserId|                tags|postTypeId|count|
+-----------+--------------------+----------+-----+
|        862|                    |         2|    1|
|        855|         <resources>|         1|    1|
|        846|<translation><eng...|         1|    1|
|        845|<word-meaning><tr...|         1|    1|
|        842|  <verbs><resources>|         1|    1|
|        835|    <grammar><verbs>|         1|    1|
|        833|                    |         2|    1|
|        833|           <meaning>|         1|    1|
|        833|<meaning><article...|         1|    1|
|        814|                    |         2|    1|
+-----------+--------------------+----------+-----+
~~~

example 2: 

~~~
postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)
postsDfNew.groupBy('ownerUserId).agg(Map("lastActivityDate" -> "max", "score" -> "max")).show(10)
~~~

~~~shell
+-----------+---------------------+----------+
|ownerUserId|max(lastActivityDate)|max(score)|
+-----------+---------------------+----------+
|        431| 2014-02-16 14:16:...|         1|
|        232| 2014-08-18 20:25:...|         6|
|        833| 2014-09-03 19:53:...|         4|
|        633| 2014-05-15 22:22:...|         1|
|        634| 2014-05-27 09:22:...|         6|
|        234| 2014-07-12 17:56:...|         5|
|        235| 2014-08-28 19:30:...|        10|
|        435| 2014-02-18 13:10:...|        -2|
|        835| 2014-08-26 15:35:...|         3|
|         37| 2014-09-13 13:29:...|        23|
+-----------+---------------------+----------+
~~~

example 3:

~~~scala
postsDfNew.groupBy('ownerUserId).
  agg(max('lastActivityDate), max('score).gt(5)).show(10)
~~~

~~~shell
+-----------+---------------------+----------------+
|ownerUserId|max(lastActivityDate)|(max(score) > 5)|
+-----------+---------------------+----------------+
|        431| 2014-02-16 14:16:...|           false|
|        232| 2014-08-18 20:25:...|            true|
|        833| 2014-09-03 19:53:...|           false|
|        633| 2014-05-15 22:22:...|           false|
|        634| 2014-05-27 09:22:...|            true|
|        234| 2014-07-12 17:56:...|           false|
|        235| 2014-08-28 19:30:...|            true|
|        435| 2014-02-18 13:10:...|           false|
|        835| 2014-08-26 15:35:...|           false|
|         37| 2014-09-13 13:29:...|            true|
+-----------+---------------------+----------------+
~~~

#### User-defined aggregate functions

general approach

1. create a class that extends `org.apache.spark.sql.expressions.UserDefinedAggregateFunction`
2. define input and buffer schemas
3. implement the initialize, update, merge, and evaluate functions

details: [http://mng.bz/Gbt3](http://mng.bz/Gbt3)

a Java example: [http://mng.bz/5bOb](http://mng.bz/5bOb)

#### Rollup and cube

compared with `groupBy`, `rollup` respects the hierarchy of the input columns and always groups by the first column

example 

~~~scala
val smplDf = postsDfNew.where('ownerUserId >= 13 and 'ownerUserId <= 15)
smplDf.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()
~~~

~~~shell
scala> smplDf.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()
+-----------+----+----------+-----+
|ownerUserId|tags|postTypeId|count|
+-----------+----+----------+-----+
|         15|    |         2|    2|
|         14|    |         2|    2|
|         13|    |         2|    1|
+-----------+----+----------+-----+
scala> smplDf.rollup('ownerUserId, 'tags, 'postTypeId).count.show()
+-----------+----+----------+-----+
|ownerUserId|tags|postTypeId|count|
+-----------+----+----------+-----+
|         15|    |         2|    2|
|         13|    |      null|    1|
|         13|null|      null|    1|
|         14|    |      null|    2|
|         13|    |         2|    1|
|         14|null|      null|    2|
|         15|    |      null|    2|
|         14|    |         2|    2|
|         15|null|      null|    2|
|       null|null|      null|    5|
+-----------+----+----------+-----+
~~~

` cube` function returns all of these results, but also adds other possible subtotals (per post type, per tags, per post type and tags, per post type and user)

#### some configuring Spark SQL

Detailed introduction is in chapter 10

~~~scala
spark.sql("SET spark.sql.caseSensitive=true")
spark.conf.set("spark.sql.caseSensitive", "true")
~~~

Another configurations:  `spark.sql.eagerAnalysis`

> if set it to true, spark throws an exception as soon as you mention a non-existent column in a DataFrame, instead of waiting for you to perform an action on the DataFrame that fetches the results.

### 5.1.7. Performing joins

example: 

~~~scala
// load the raw data
val itVotesRaw = 
	sc.textFile("first-edition/ch05/italianVotes.csv").
  	map(x => x.split("~"))

// convert to Rows
val itVotesRows = 
	itVotesRaw.map(row => Row(
		row(0).toLong, row(1).toLong, row(2).toInt, 
		Timestamp.valueOf(row(3)))
	)

// define Schema
val votesSchema = StructType(Seq(
	StructField("id", LongType, false),
	StructField("postId", LongType, false),
	StructField("voteTypeId", IntegerType, false),
	StructField("creationDate", TimestampType, false)) )

// create the DataFrame based on the Rows and Schema
val votesDf = spark.createDataFrame(itVotesRows, votesSchema)

// Inner Join by postId
val postsVotes 
	= postsDf.join(votesDf, postsDf("id") === 'postId)

// Outer Join by postId
val postsVotesOuter
	= postsDf.join(votesDf, postsDf("id") === 'postId, "outer")

// postId is unique across both DataFrames, which make it Ok to use implicit conversion from Scala's Symbol ('postaId)
~~~

> Notice: `spark.sql.shuffle.partitions` has influence on performance, currently it is a fixed parameter rather then depending on the data and runtime environment </br>
> Spark JIRA tickets to following this problem: [SPARK-9872](https://issues.apache.org/jira/browse/SPARK-9872), [SPARK-9850](https://issues.apache.org/jira/browse/SPARK-9850)

## 5.2. Beyond DataFrames: introducing DataSets

`DataSets` represent a competition to RDDs in a way because they have overlapping functions. `DataFrames` are now simply implemented as `DataSets` containing `Row` objects.

use `DataFrame.as(u:Encoder)` for converting a `DataFrame` to a `DataSet`, below is an example, also you can write your own encoders or use encoders from ordinary Java bean classes

example: 

~~~scala
val stringDataSet = spark.read.text("path/to/file").as[String]
~~~

DataSet documents: [http://mng.bz/3EQc](http://mng.bz/3EQc)

## 5.3. Using SQL commands (Spark SQL)

### 5.3.1. Table catalog and Hive metastore

#### Registering tables temporarily

#### Registering tables permanently

#### Working with the Spark table catalog

#### Configuring a remote Hive metastore

### 5.3.2. Executing SQL queries

#### Using the Spark SQL shell

### 5.3.3. Connecting to Spark SQL through the Thrift server

#### Starting a Thrift server

#### Connecting to the Thrift server with Beeline

#### Connecting from third-party JDBC clients


## 5.4. Saving and loading DataFrame data

### 5.4.1. Built-in data sources

#### JSON

#### ORC

#### Parquet

### 5.4.2. Saving data

#### Configuring the writer

#### Using the saveAsTable method

#### Using the insertInto method

#### Using the save method

#### Using the shortcut methods

#### Saving data to relational databases with JDBC

### 5.4.3. Loading data

#### Loading data from relational databases using JDBC

#### Loading data from data sources registered using SQL

## 5.5. Catalyst optimizer

#### Examining the execution plan

#### Taking advantage of partition statistics

## 5.6. Performance improvements with Tungsten

## 5.7. Summary
