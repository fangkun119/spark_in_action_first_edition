# Chapter 3. Writing Spark applications

## 3.1. Generating a new Spark project in Eclipse

> There are readily available resources online that describe how to use IntelliJ IDEA with Spark, whereas Eclipse resources are still hard to come by. That is why, in this chapter, you’ll learn how to use Eclipse for writing Spark programs

> We prepared a Maven Archetype (a template for quickly bootstrapping Maven projects) in the book’s GitHub repository at [https://github.com/spark-in-action](https://github.com/spark-in-action), which will help you bootstrap your new Spark application project in just a few clicks

[https://livebook.manning.com/book/spark-in-action/chapter-3/10](https://livebook.manning.com/book/spark-in-action/chapter-3/10)

## 3.2 Developing the application 

> create a daily report that lists all employees of your company and the number of pushes they’ve made to GitHub

### 3.2.1. Preparing the GitHub archive dataset

~~~shell
$ mkdir -p $HOME/sia/github-archive
$ cd $HOME/sia/github-archive
$ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
2015-03-01-0.json.gz
2015-03-01-1.json.gz
...
2015-03-01-23.json.gz
$ gunzip *
$ head -n 1 2015-03-01-0.json
{"id":"2614896652","type":"CreateEvent","actor":{"id":739622,"login":
 "treydock","gravatar_id":"","url":"https://api.github.com/users/
 treydock","avatar_url":"https://avatars.githubusercontent.com/u/
 739622?"},"repo":{"id":23934080,"name":"Early-Modern-OCR/emop-
 dashboard","url":"https://api.github.com/repos/Early-Modern-OCR/emop-
 dashboard"},"payload":{"ref":"development","ref_type":"branch","master_
 branch":"master","description":"","pusher_type":"user"},"public":true,
 "created_at":"2015-03-01T00:00:00Z","org":{"id":10965476,"login":
 "Early-Modern-OCR","gravatar_id":"","url":"https://api.github.com/
 orgs/Early-Modern-OCR","avatar_url":
 "https://avatars.githubusercontent.com/u/10965476?"}}
~~~

> rather large (around 1 GB in total, decompressed), use just the first hour of the day (44 MB) during development<br/>
> one Json object each line<br/>
> GitHub API: [https://developer.github.com/v3/activity/events/types/](https://developer.github.com/v3/activity/events/types/)

format the json with `jq` ([http://stedolan.github.io/jq/download](http://stedolan.github.io/jq/download))<br/> 

~~~shell
$ head -n 1 2015-03-01-0.json | jq '.'
{
  "id": "2614896652",
  "type": "CreateEvent",
  "actor": {
    "id": 739622,
    "login": "treydock",
    "gravatar_id": "",
    "url": "https://api.githb.com/users/treydock",
    "avatar_url": "https://avatars.githubusercontent.com/u/739622?"
  },
  "repo": {
    "id": 23934080,
    "name": "Early-Modern-OCR/emop-dashboard",
    "url": "https://api.github.com/repos/Early-Modern-OCR/emop-dashboard"
  },
  "payload": {
    "ref": "development",
    "ref_type": "branch",
    "master-branch": "master",
    "description": "",
    "pusher_type": "user",
  },
  "public": true,
  "created_at": "2015-03-01T00:00:00Z",
  "org": {
    "id": 10965476,
    "login": "Early-Modern-OCR",
    "gravatar_id": "",
    "url": "https://api.github.com/orgs/Early-Modern-OCR",
    "avatar_url": "https://avatars.githubusercontent.com/u/10965476?"
  }
}
~~~

`"type":"CreateEvent"` here is for `"ref_type":"branch"` with name `"payload.ref_type":"development"`, we need find json for `push`. After querying [GitHub API doc](https://developer.github.com/v3/activity/events/types/#pushevent), find out that `"type":"PushEvent"` is what we need

### 3.2.2 Loading JSON

Code: [App.scala](../ch03/scala/org/sia/chapter03App/App.scala) <br/>

<b>`SQLContext`,`SparkContext`,`SparkSession`</b>: 
	
* <b>SQLContext</b>: the main interface to Spark SQL, just like <b>SparkContext</b> to Spark Core. Since Spark 2.0, both of them two are merged into a single class <b>SparkSession</b>.  
* It's `read` method gives access to the `DataFrameReader` object. <br/>

[`DataFrameReader.json(paths: String*)`](http://mng.bz/amo7) is for reading json files (1 line a json object)<br/>

~~~scala
def json(paths: String*): DataFrame
Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
~~~

<b>`DataFrame`</b> object returned by `DataFrameReader.json(paths:String*)` 

* When created from a structured dataset (JSON, Table,..), Spark is able to infer a schema. <br/>
* Also has many RDD methods like `filter`,`map`,`flatMap`,`collect`,`count`,...
* `DataFrame` is a special case of `DataSet` (It's a `DataSet` containing `Row` objects` according to the scaladocs ([http://mng.bz/3EQc](http://mng.bz/3EQc)) with a overloade version of `filter` function) 


### 3.2.3 Running the application from Eclipse

Full Introduction and Related Eclipse IED operation on $3.2.2 - $3.2.7: [https://livebook.manning.com/book/spark-in-action/chapter-3/](https://livebook.manning.com/book/spark-in-action/chapter-3/) <br/>

some debugging-codes are added to demonstrate what is going on when running the application in [App.scala](../ch03/scala/org/sia/chapter03App/App.scala) 

~~~scala
pushes.printSchema  // display infered schema 
println("all events: " + ghLog.count) // total log count
println("only pushes: " + pushes.count) // log count after filtering
pushes.show(5) // first 5 lines of the filtered log
~~~

comments: 

* if there are too many `INFO` and `WARN` messages, change the logging-configuration in section 2.2.1
* `nullable = true` by default in infered schema, Spark leaves the choice to you 


### 3.2.4 Aggregating the data

Code: [App.scala](../ch03/scala/org/sia/chapter03App/App.scala)<br/>

<b>DataFrame.groupBy(column:String).count</b>

~~~scala
val grouped = pushes.groupBy("actor.login").count
grouped.show(5)
+----------+-----+
|     login|count|
+----------+-----+
|    gfgtdf|    1|
|   mdorman|    1|
|quinngrier|    1|
| aelveborn|    1|
|  jwallesh|    3|
+----------+-----+
~~~

other aggregating functions such as `min`,`max`,`avg`,`sum`<br/>
scaladoc of GroupedData: [http://mng.bz/X8lA](http://mng.bz/X8lA)<br/>

<b>GroupedData.orderBy(*cols, **kwargs)<b/>

~~~scala
val ordered = grouped.orderBy(grouped("count").desc)
ordered.show(5)
~~~

> `val grouped = pushes.groupBy("actor.login").count` is a DataFrame<br/>
> `grouped("count")` returns the `count` [`org.apache.spark.sql.Column`](https://kapeli.com/dash_share?docset_file=SparkProjectSQL&docset_name=org.apache.spark:spark-sql_2.12&path=dash_scaladoc/org/apache/spark/sql/Column.html&platform=scaladoc&repo=Scala%20Docsets&version=3.0.0-preview2) of the DataFrame (by implicitly calls `DataFrame.apply`)<br/>
> `Column.desc`: returns a sort expression based on the descending order of the column. (ascending order, it is `Column.asc`)<br/> 
> `DataFrame.apply`: [http://mng.bz/X8lA](http://mng.bz/X8lA) <br/>

### 3.2.5 Excluding non-employees 

<b>Code</b>: [App.scala](../ch03/scala/org/sia/chapter03App/App.scala)<br/>
<b>Employee Name List<b/>: [../ch03/ghEmployees.txt](../ch03/ghEmployees.txt)</br>

<b>Load the Name List into a set</b>

~~~scala
import scala.io.Source.fromFile
val empPath = homeDir + "/first-edition/ch03/ghEmployees.txt"
val employees = Set() ++ (
  for {
    line <- fromFile(empPath).getLines
  } yield line.trim
)
~~~

pseudocode of the scala code above

~~~txt
In each iteration of the for loop:
    Read the next line from the file
    Initialize a new line variable so that it contains the current line
        as its value
    Take the value from the line variable, trim it, and add it to the
        temporary collection
Once the last iteration finishes:
    Return the temporary, hidden collection as the result of for
    Add the result of for to an empty set
    Assign the set to the employees variable
~~~

> Loading an entire file isn’t a good practice, but in this case is fine (the file is small)<br/>
> `for` expression in scala: [http://mng.bz/k8q2](http://mng.bz/k8q2)<br/>

<b>Use the set as a filter</b>

`DataFrame.filter`: [http://mng.bz/3EQc](http://mng.bz/3EQc)

~~~scala
def filter(conditionExpr: String): DataSet
// example
// val oldPeopleDf = peopleDf.filter("age > 15")
~~~

`SparkSession.udf` for regist UDF(user defined function): [http://mng.bz/j9As](http://mng.bz/j9As) 

~~~scala
// define the check function explicity
val isEmp: (String => Boolean) = (arg: String) => employees.contains(arg)
// define the check function by type inference
val isEmp = user => employees.contains(user)
// regist isEmp as a UDF
val isEmployee = spark.udf.register("isEmpUdf", isEmp)
~~~

> when Spark goes to execute the UDF, it will take all of its dependencies (only the employees set, in this case) and send them along with each and every task, to be executed on a cluster

### 3.2.6 Broadcast variables 

`Broadcast Variables` allow you to send a vairable exactly once to each node in a cluster (rather than let the variable sending triggered by function invocation which might cause a huge frequency of data transfer) 

> the variable will be automatically cached after broadcast<br/>
> the broadcast is done by peer-to-peer protocal like bitTorrent thus the master doesn't get clogged<br/>

~~~scala
val bcEmployees = sc.broadcast(employees) #broadcast the variable
val isEmp = user => bcEmployees.value.contains(user) #define a function which uses the broadcasted variable
~~~

pass UDF to DataFrame.filter()

~~~scala
import sqlContext.implicits._
val filtered = ordered.filter(isEmployee($"login"))
filtered.show()
~~~

tell `filter` to apply the `isEmployee` UDF on the `login` column, if `isEmployee` returns `true` then the row gets included in the `filtered DataFrame`

Notice: we should paarameterize `appName`, `appMaster`, `inputPath` and `empPath` from outside rather than hard-coding them

Code: [App.scala](../ch03/scala/org/sia/chapter03App/App.scala) <br/>

~~~scala
package org.sia.chapter03App

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

object App {
  def main(args : Array[String]) {
    // TODO expose appName and master as app. params
    val spark = SparkSession.builder()
        .setAppNameappName("GitHub push counter")
        .setMastermaster("local[*]")
        .getOrCreate()

    val sc = spark.sparkContext

    // TODO expose inputPath as app. param
    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json" //single file
    // val inputPath = homeDir + "/sia/github-archive/*.json" //multiple files 
    val ghLog = spark.read.json(inputPath)

    val pushes = ghLog.filter("type = 'PushEvent'")
    // pushes.printSchema  						    // display infered schema 
	// println("all events: " + ghLog.count) 		// total log count
	// println("only pushes: " + pushes.count)		// log count after filtering
	// pushes.show(5)								// first 5 lines of the filtered log

    val grouped = pushes.groupBy("actor.login").count 
	// grouped.show(5) // group by user name

    val ordered = grouped.orderBy(grouped("count").desc)
    // ordered.show(5) // top 5 users with the push count

    // TODO expose empPath as app. param
    val empPath = homeDir + "/first-edition/ch03/ghEmployees.txt"
    val employees = Set() ++ (
      // for expression: http://mng.bz/k8q2
      for { 
        line <- fromFile(empPath).getLines  
      } yield line.trim
    )

    //Broadcast the employees set to all nodes
    val bcEmployees = sc.broadcast(employees)  

    import sqlContextspark.implicits._
    val isEmp = user => bcEmployees.value.contains(user)
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()
  }
}
~~~

### 3.2.7 Using the entire dataset

~~~scala
// val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json" //single file
val inputPath = homeDir + "/sia/github-archive/*.json" //multiple files 
val ghLog = spark.read.json(inputPath)
~~~

`spark.read.json(inputPath)` can also process multiple fiels

Code: [../ch03/scala/org/sia/chapter03App/GitHubDay.scala](../ch03/scala/org/sia/chapter03App/GitHubDay.scala) 

~~~scala
package org.sia.chapter03App

import scala.io.Source.fromFile
import org.apache.spark.sql.SparkSession

// object: http://mng.bz/y2ja
// is Spark Singleton
// must be consistent with the file name
object GitHubDay { 
  def main(args : Array[String]) {
    // SparkSession doc: http://mng.bz/j9As
    val spark = SparkSession.builder().getOrCreate()
    val sc    = spark.sparkContext

    val ghLog = spark.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    // Broadcast the employees set
    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
    )
    val bcEmployees = sc.broadcast(employees)

    import spark.implicits._
    val isEmp = user => bcEmployees.value.contains(user)
    val sqlFunc = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(sqlFunc($"login"))

    filtered.write.format(args(3)).save(args(2))
  }
}
~~~

## 3.3 Submitting the application

To make the spark application run on any Spark cluster, the problem is that you can not make sure all the dependency (project -> dependency hierarchy tab) exists on the cluster (such as spark-sql may not be installed)<br/>

2 approaches to solve the problems 

* use the `--jars` parameter of the `spark-submit` script, which will transfer all the listed JARs to the executors 
* build a so-called `uberjar`: a JAR that contains all needed dependencies 

### 3.3.1 Configuration for building the uberjar 

In the test environment, you can provide libraries locally and let the driver (a machine from which you connect to a cluster) expose the libraries over a provisional HTTP server to all other nodes<br/>

Reference - Advanced Dependency Manangement: [http://spark.apache.org/docs/latest/submitting-applications.html](http://spark.apache.org/docs/latest/submitting-applications.html)

Example: introduce a new dependency `commons-email` into uberjar

~~~maven
<dependency>
  <groupId>org.apache.commons</groupId>
  <artifactId>commons-email</artifactId>
  <version>1.3.1</version>
  <scope>compile</scope>
</dependency>
~~~

> add dependency, the scope must be `compile` to make sure it can be compiled into uberjar <br/>
> `Maven > Update Project` to update the depency <br/>

### 3.3.2 Adapting the application and build uberjar

need to make some modifications on the application so that it can be run by `spark-submit` script

<b>application after adapting</b>

~~~scala
package org.sia.chapter03App

import scala.io.Source.fromFile
import org.apache.spark.sql.SparkSession

object GitHubDay {
  def main(args : Array[String]) {
    ////// do not get appName and master from config  //////
    ////// spark-submit will provide them //////////////////
    // val spark = SparkSession.builder()
    //    .appName("GitHub push counter")
    //    .master("local[*]")
    //    .getOrCreate()
    val spark = SparkSession.builder().getOrCreate()

    val sc = spark.sparkContext
    val ghLog = spark.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
    )
    val bcEmployees = sc.broadcast(employees)

    import spark.implicits._
    val isEmp = user => bcEmployees.value.contains(user)
    val sqlFunc = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(sqlFunc($"login"))

	 ////// output destination and format is depend on `spark-submit` //////
	 // filtered.show()
    filtered.write.format(args(3)).save(args(2))
  }
}
~~~

> format: JSON, Parquet[a fast columnar file, http://parquet.apache.org/](http://parquet.apache.org/), and JDBC<br/>

the application will take four arguments: 

* Path to the input JSON files
* Path to the employees file
* Path to the output file
* Output format

<b>build uberjar</b>

Run > Run Configurations > Maven Build > New Launch Configuration > Build uberjar <br/>

* ${project_loc}: parameterized base directory
* Goals: maven goals that build the JAR
* Skip Tests

Build and get a chapter03App-0.0.1-SNAPSHOT.jar in target folder 


### 3.3.2 Using spark-submit 

Doc: [http://mng.bz/WY2Y](http://mng.bz/WY2Y)

Shell script to submit applications to be executed on a Spark cluster

~~~shell
spark-submit \
    --class <main-class> \
    --master <master-url> \
    --deploy-mode <deploy-mode> \
    --conf <key>=<value> \
    ... # other options
    <application-jar> \
    [application-arguments]
~~~

Get ready to check the logs after submit the application (the path is in log4j configuration) in another terminal

~~~shell
$ tail -f /usr/local/spark/logs/info.log
~~~

Submit the application

~~~shell
$ spark-submit --class org.sia.chapter03App.GitHubDay --master local[*]
 --name "Daily GitHub Push Counter" chapter03App-0.0.1-SNAPSHOT.jar
 "$HOME/sia/github-archive/*.json"
 "$HOME/first-edition/ch03/ghEmployees.txt"
 "$HOME/sia/emp-gh-push-output" "json"
~~~

This is for submitting a scala application, if submitting a python spplication, use below.

~~~shell
$ spark-submit --master local[*] --name "Daily GitHub Push Counter"
 GitHubDay.py "$HOME/sia/github-archive/*.json"
 "$HOME/sia/ghEmployees. txt" "$HOME/sia/emp-gh-push-output" "json"
~~~

Check output folder, there should be a file named as `_SUCCESS`

~~~shell
$ cd $HOME/sia/emp-gh-push-output
$ ls –la
~~~

# 3.4. Summary

[https://livebook.manning.com/book/spark-in-action/chapter-3/227](https://livebook.manning.com/book/spark-in-action/chapter-3/227)


















