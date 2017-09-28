package sql

import org.apache.spark.sql.SparkSession
import sql.SQLStmtPrac.ss

object DSLPrac extends App{
  val ss=SparkSession.builder()
//    .master("local[*]")
    .appName("DSL")
    .getOrCreate()
  val reader=ss.read.option("header", "true").option("inferSchema", "true")
  val ratingsDF=reader.csv("/ml-20m/ratings.csv")
  val moviesDF=reader.csv("/ml-20m/movies.csv")


  import ss.implicits._
  import org.apache.spark.sql.functions._

  
  ratingsDF.joinWith(moviesDF,ratingsDF("movieId")===moviesDF("movieId"))
    .groupBy("_1.movieId","_2.title")
    .agg(avg($"_1.rating") alias "average")
    .orderBy(desc("average")).limit(10)
    .show()
//
//  ratingsDF.joinWith(moviesDF,ratingsDF("movieId")===moviesDF("movieId"))
//    .groupBy($"_1.movieId",$"_2.title")
//    .agg(avg($"_1.rating") alias "average",variance($"_1.rating") alias "var")
//    .where(!isnan($"var")&&$"var">0)
//    .orderBy(desc("average")).limit(10)
//    .show()

}
