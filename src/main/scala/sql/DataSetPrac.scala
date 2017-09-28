package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}



object DataSetPrac extends App{

  //userId,movieId,rating,timestamp
  case class Rating(userId:Int,movieId:Int,rating:Double,timestamp:Long)
  //movieId,title,genres
  case class Movie(movieId:Int,title:String,genres:String)


  case class ResultRow(movieId:Int,title:String,avg:Double)
  val ss=SparkSession.builder()
//    .master("local[*]")
    .appName("DataSet")
    .getOrCreate()
  import ss.implicits._
  val reader=ss.read.option("header", "true")
    .option("inferSchema", "true")
  val ratingDS: Dataset[Rating] =reader.csv("src/main/resources/sql/ml-20m/ratings.csv").as[Rating]
  val movieDS: Dataset[Movie] =reader.csv("src/main/resources/sql/ml-20m/movies.csv").as[Movie]



//  val reader=ss.read.option("header", "true").option("inferSchema", "true")
//  val ratingsDF=reader.csv(".../ratings.csv")
//  val moviesDF=reader.csv(".../movies.csv")
  import ss.implicits._
  import org.apache.spark.sql.functions._
//  ratings.map(v=>v.copy(rating = v.rating+10)).show()
//  ratingDS.map(v=>v.movieId->(v.rating,1))
//  ratings.joinWith(movies,ratings("movieId")===movies("movieId"))
//    .groupBy("_1.movieId","_2.title")
//    .agg(avg($"_1.rating") alias "average")
//    .orderBy(desc("average")).limit(10)
//    .show()


  //  ratings.show()
//  movies.show()

  case class Person(name:String, age:Option[Long])
  val rdd: RDD[Rating] =ratingDS.rdd

  import org.json4s.native.Serialization.read
  implicit val formats=org.json4s.DefaultFormats
  val people: RDD[Person] =ss.sparkContext.textFile("src/main/resources/sql/people.json")
    .map(jsonStr=>read[Person](jsonStr))

  people.foreach(println)
  import ss.implicits._
  val peopleFromDS: RDD[Person] =ss.read.json("src/main/resources/sql/people.json").as[Person].rdd
  people.foreach(println)

}
