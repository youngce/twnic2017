package sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object GettingStarted  extends App {
  case class Person(name: String, age: Option[Long])
  val ss=SparkSession.builder()
    .master("local[*]")
    .appName("Hello SparkSQL")
    .getOrCreate()
  val df: DataFrame =ss.read.json("src/main/resources/sql/people.json")

  df.printSchema()
//  // DSL
//  df.select("name").filter(df("age")>20).show()
//
//  // SQL statement
//  df.createGlobalTempView("people")
//  ss.sql("SELECT name FROM global_temp.people where age >20").show()

//  import   ss.implicits._
//  val primitiveDS = Seq(1, 2, 3).toDS()
//  primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
  df.printSchema()
//  df.as[Person].filter(_.age.isDefined).show()
//  df.printSchema()
  df.select(df("age") +1).show()
  df.show()
//  df.createOrReplaceGlobalTempView()





}
