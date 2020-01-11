package org.sia.chapter03App

import scala.io.Source.fromFile
import org.apache.spark.sql.SparkSession

// object: http://mng.bz/y2ja
// is Spark Singleton
// must be consistent with the file name
object GitHubDay { 
  def main(args : Array[String]) {
    // SparkSession doc: http://mng.bz/j9As
    ////// do not get appName and master from config  //////
    ////// spark-submit will provide them //////////////////
    // val spark = SparkSession.builder()
    //    .appName("GitHub push counter")
    //    .master("local[*]")
    //    .getOrCreate()
    val spark = SparkSession.builder().getOrCreate()
    val sc    = spark.sparkContext

    // 
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

    ////// output destination and format is depend on `spark-submit` //////
    // filtered.show()
    filtered.write.format(args(3)).save(args(2))
  }
}
