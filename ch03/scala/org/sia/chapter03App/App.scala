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
